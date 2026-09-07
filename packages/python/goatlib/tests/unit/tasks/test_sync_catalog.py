"""Unit tests for the catalog file-sync task.

The S3 client is a small fake (head_object/download_file) rather than a
botocore Stubber — the sync algorithm is tested directly via
``_sync_with_client``, so no real network/boto3 setup is needed.

The bucket publishes ``items.parquet`` + ``collections.parquet`` (verified
against the real bucket: those two keys and ``catalog.json`` sit at the root,
and there is no published ``catalog.parquet``), so the fake serves both and the
sync builds the flat mirror locally with ``build_mirror``. The version marker is
therefore a composite of both ETags: a collection-only edit changes the licence
and publisher denormalised onto every item, so it has to trigger a rebuild.
"""

from pathlib import Path
from typing import Any

import duckdb
import pytest
from botocore.exceptions import ClientError
from goatlib.tasks.catalog_mirror import COLLECTIONS_FILENAME, ITEMS_FILENAME
from goatlib.tasks.sync_catalog import (
    MIRROR_COLLECTIONS_FILENAME,
    MIRROR_ITEMS_FILENAME,
    REQUIRED_COLLECTION_COLUMNS,
    REQUIRED_ITEM_COLUMNS,
    SyncCatalogParams,
    _build_client_and_bucket,
    _composite_version,
    _parse_s3_url,
    _sync_with_client,
)

# ────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────


class _FakeS3Client:
    """Minimal S3 client stand-in: only head_object/download_file are used."""

    def __init__(
        self,
        etag: str | None,
        sources: dict[str, Path] | None = None,
        head_error_code: str | None = None,
    ) -> None:
        self._etag = etag
        self._sources = sources or {}
        self._head_error_code = head_error_code
        self.head_calls: list[tuple[str, str]] = []
        self.download_calls: list[tuple[str, str, str]] = []

    def head_object(self, Bucket: str, Key: str) -> dict[str, Any]:  # noqa: N803
        self.head_calls.append((Bucket, Key))
        if self._head_error_code is not None:
            status = 403 if self._head_error_code == "AccessDenied" else 404
            raise ClientError(
                {
                    "Error": {"Code": self._head_error_code, "Message": "error"},
                    "ResponseMetadata": {"HTTPStatusCode": status},
                },
                "HeadObject",
            )
        # A distinct ETag per key, derived from the one the test supplied, so a
        # composite marker is exercised rather than two identical values.
        return {"ETag": f'"{self._etag}-{Key}"'}

    def download_file(self, Bucket: str, Key: str, Filename: str) -> None:  # noqa: N803
        self.download_calls.append((Bucket, Key, Filename))
        source = self._sources.get(Key.rsplit("/", 1)[-1])
        if source is None:
            raise AssertionError(f"download_file called for unconfigured key {Key}")
        Path(Filename).write_bytes(source.read_bytes())


def _expected_version(etag: str) -> str:
    """The composite marker the sync computes for a given per-key ETag base."""
    return _composite_version(
        {
            ITEMS_FILENAME: f"{etag}-{ITEMS_FILENAME}",
            COLLECTIONS_FILENAME: f"{etag}-{COLLECTIONS_FILENAME}",
        }
    )


def _write_published(tmp_path: Path, *, rows: int = 2) -> dict[str, Path]:
    """Tiny published-shaped items/collections parquet, as the bucket has them.

    The column shapes mirror the real published files (verified against the
    bucket): ``themes``/``language`` as structs, ``assets``/``links`` as
    structs, ``bbox`` as a struct of four doubles.
    """
    items_path = tmp_path / ITEMS_FILENAME
    colls_path = tmp_path / COLLECTIONS_FILENAME
    con = duckdb.connect()
    try:
        con.execute("INSTALL spatial; LOAD spatial;")
        con.execute(f"""
            COPY (
                SELECT
                    'item-' || i AS id,
                    'coll-1' AS collection,
                    '1.0.0' AS stac_version,
                    'Titel ' || i AS title,
                    'Beschreibung ' || i AS description,
                    ['schlagwort'] AS keywords,
                    {{'code': 'de'}} AS language,
                    'polygon' AS "goat:geometryType",
                    ST_Point(i, i) AS geometry,
                    {{'xmin': 0.0, 'ymin': 0.0, 'xmax': 1.0, 'ymax': 1.0}} AS bbox,
                    TIMESTAMPTZ '2026-01-01' AS datetime,
                    TIMESTAMPTZ '2026-01-01' AS created,
                    TIMESTAMPTZ '2026-01-02' AS updated,
                    {{'data': {{'href': 's3://b/x.parquet'}}}} AS assets
                FROM range({rows}) AS t(i)
            ) TO '{items_path.as_posix()}' (FORMAT PARQUET)
        """)
        con.execute(f"""
            COPY (
                SELECT 'coll-1' AS id, 'Collection' AS type, 'Sammlung' AS title,
                       'CC-BY-4.0' AS license,
                       [{{'name': 'Landesamt', 'roles': ['producer']}}] AS providers,
                       [{{'scheme': 's', 'concepts': [{{'id': 'transportation'}}]}}] AS themes,
                       'feature' AS "goat:layerType"
            ) TO '{colls_path.as_posix()}' (FORMAT PARQUET)
        """)
    finally:
        con.close()
    return {ITEMS_FILENAME: items_path, COLLECTIONS_FILENAME: colls_path}


def _write_parquet(
    path: Path,
    *,
    valid: bool = True,
    rows: int = 2,
    required: tuple[str, ...] = REQUIRED_ITEM_COLUMNS,
) -> None:
    """Build a tiny mirror parquet with DuckDB.

    `valid=True` includes every required column; `valid=False` drops one
    (``publisher``) to exercise the missing-column validation failure.
    """
    con = duckdb.connect()
    try:
        con.execute("INSTALL spatial; LOAD spatial;")
        columns = list(required)
        if not valid:
            columns.remove("publisher")

        select_parts = []
        for col in columns:
            if col == "geometry":
                select_parts.append("ST_Point(i, i) AS geometry")
            elif col in {"datetime", "created", "updated"}:
                select_parts.append(f"TIMESTAMPTZ '2026-01-01' AS {col}")
            elif col.startswith(("bbox_", "group_bbox_")):
                select_parts.append(f"1.0 AS {col}")
            elif col == "member_count":
                select_parts.append("1 AS member_count")
            else:
                quoted = f'"{col}"'
                select_parts.append(f"'{col}_' || i AS {quoted}")

        con.execute(
            f"""
            COPY (
                SELECT {", ".join(select_parts)}
                FROM range({rows}) AS t(i)
            ) TO '{path.as_posix()}' (FORMAT PARQUET)
            """
        )
    finally:
        con.close()


def _fake_build(
    out_items: Path, out_collections: Path, *, valid: bool, rows: int
) -> tuple[int, int]:
    """A stand-in converter, for testing what the sync does with a bad build."""
    _write_parquet(out_items, valid=valid, rows=rows)
    _write_parquet(
        out_collections, valid=True, rows=1, required=REQUIRED_COLLECTION_COLUMNS
    )
    return rows, 1


def _params(dest_dir: Path, *, dry_run: bool = False) -> SyncCatalogParams:
    return SyncCatalogParams(
        bucket="test-bucket",
        prefix="catalog",
        dest_dir=str(dest_dir),
        dry_run=dry_run,
    )


# ────────────────────────────────────────────────────────────────────────
# Tests
# ────────────────────────────────────────────────────────────────────────


def test_unchanged_etag_short_circuits_without_download(tmp_path: Path) -> None:
    dest_dir = tmp_path / "catalog"
    dest_dir.mkdir()
    final_path = dest_dir / MIRROR_ITEMS_FILENAME
    _write_parquet(final_path, valid=True, rows=3)
    version = _expected_version("abc123")
    (dest_dir / "VERSION").write_text(version)

    client = _FakeS3Client(etag="abc123")

    result = _sync_with_client(client, "test-bucket", "", dest_dir, dry_run=False)

    assert result == {"changed": False, "version": version, "items": 3}
    assert client.download_calls == []
    # Both published inputs are checked: a change to either must rebuild.
    assert client.head_calls == [
        ("test-bucket", ITEMS_FILENAME),
        ("test-bucket", COLLECTIONS_FILENAME),
    ]


def test_collection_only_change_triggers_a_rebuild(tmp_path: Path) -> None:
    """The marker covers both inputs, not just items.parquet.

    Licence and publisher are denormalised from the collection onto every item
    row, so a collections-only edit changes the served mirror even though
    items.parquet is untouched.
    """
    dest_dir = tmp_path / "catalog"
    dest_dir.mkdir()
    only_items_changed = _composite_version(
        {
            ITEMS_FILENAME: f"e1-{ITEMS_FILENAME}",
            COLLECTIONS_FILENAME: "stale",
        }
    )
    (dest_dir / "VERSION").write_text(only_items_changed)

    client = _FakeS3Client(etag="e1", sources=_write_published(tmp_path))
    result = _sync_with_client(client, "test-bucket", "", dest_dir, dry_run=False)

    assert result["changed"] is True


def test_unchanged_etag_reports_minus_one_when_local_file_missing(
    tmp_path: Path,
) -> None:
    dest_dir = tmp_path / "catalog"
    dest_dir.mkdir()
    version = _expected_version("abc123")
    (dest_dir / "VERSION").write_text(version)

    client = _FakeS3Client(etag="abc123")

    result = _sync_with_client(client, "test-bucket", "", dest_dir, dry_run=False)

    assert result == {"changed": False, "version": version, "items": -1}
    assert client.download_calls == []


def test_happy_path_builds_the_mirror_and_writes_version_last(tmp_path: Path) -> None:
    dest_dir = tmp_path / "catalog"
    dest_dir.mkdir()
    sources = _write_published(tmp_path, rows=5)

    client = _FakeS3Client(etag="newetag", sources=sources)

    result = _sync_with_client(client, "test-bucket", "", dest_dir, dry_run=False)

    # 5 items -- the collection lives in its own file and is not counted here.
    assert result == {
        "changed": True,
        "version": _expected_version("newetag"),
        "items": 5,
    }
    assert (dest_dir / MIRROR_ITEMS_FILENAME).exists()
    assert (dest_dir / MIRROR_COLLECTIONS_FILENAME).exists()
    assert (dest_dir / "VERSION").read_text() == _expected_version("newetag")
    assert not (dest_dir / f"{MIRROR_ITEMS_FILENAME}.tmp").exists()
    assert not (dest_dir / f"{MIRROR_COLLECTIONS_FILENAME}.tmp").exists()
    # Both inputs fetched, and neither left behind on the volume.
    assert [call[1] for call in client.download_calls] == [
        ITEMS_FILENAME,
        COLLECTIONS_FILENAME,
    ]
    assert not (dest_dir / f"{ITEMS_FILENAME}.tmp").exists()
    assert not (dest_dir / f"{COLLECTIONS_FILENAME}.tmp").exists()


def test_validation_failure_leaves_old_file_and_version_untouched(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    dest_dir = tmp_path / "catalog"
    dest_dir.mkdir()
    final_path = dest_dir / MIRROR_ITEMS_FILENAME
    _write_parquet(final_path, valid=True, rows=2)
    (dest_dir / "VERSION").write_text("oldetag")
    old_bytes = final_path.read_bytes()

    # A converter that drops a required column must not be swapped in. The
    # build is ours now, so this is a self-check rather than upstream distrust.
    monkeypatch.setattr(
        "goatlib.tasks.sync_catalog.build_mirror",
        lambda items, colls, out_items, out_colls: _fake_build(
            out_items, out_colls, valid=False, rows=2
        ),
    )
    client = _FakeS3Client(etag="newetag", sources=_write_published(tmp_path))

    with pytest.raises(ValueError, match="publisher"):
        _sync_with_client(client, "test-bucket", "", dest_dir, dry_run=False)

    # Old file + VERSION marker untouched.
    assert final_path.read_bytes() == old_bytes
    assert (dest_dir / "VERSION").read_text() == "oldetag"
    # tmp file cleaned up, never promoted.
    assert not (dest_dir / f"{MIRROR_ITEMS_FILENAME}.tmp").exists()
    assert not (dest_dir / f"{MIRROR_COLLECTIONS_FILENAME}.tmp").exists()


def test_validation_failure_zero_rows_also_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    dest_dir = tmp_path / "catalog"
    dest_dir.mkdir()

    monkeypatch.setattr(
        "goatlib.tasks.sync_catalog.build_mirror",
        lambda items, colls, out_items, out_colls: _fake_build(
            out_items, out_colls, valid=True, rows=0
        ),
    )
    client = _FakeS3Client(etag="newetag", sources=_write_published(tmp_path))

    with pytest.raises(ValueError, match="zero rows|no rows|empty"):
        _sync_with_client(client, "test-bucket", "", dest_dir, dry_run=False)

    assert not (dest_dir / MIRROR_ITEMS_FILENAME).exists()
    assert not (dest_dir / "VERSION").exists()


def test_dry_run_downloads_nothing(tmp_path: Path) -> None:
    dest_dir = tmp_path / "catalog"
    dest_dir.mkdir()

    client = _FakeS3Client(etag="newetag")

    result = _sync_with_client(client, "test-bucket", "", dest_dir, dry_run=True)

    assert result["changed"] is True
    assert result["version"] == _expected_version("newetag")
    assert client.download_calls == []
    assert not (dest_dir / MIRROR_ITEMS_FILENAME).exists()
    assert not (dest_dir / f"{MIRROR_ITEMS_FILENAME}.tmp").exists()
    assert not (dest_dir / f"{MIRROR_COLLECTIONS_FILENAME}.tmp").exists()
    assert not (dest_dir / "VERSION").exists()


def test_dry_run_short_circuits_when_unchanged(tmp_path: Path) -> None:
    dest_dir = tmp_path / "catalog"
    dest_dir.mkdir()
    final_path = dest_dir / MIRROR_ITEMS_FILENAME
    _write_parquet(final_path, valid=True, rows=4)
    version = _expected_version("same")
    (dest_dir / "VERSION").write_text(version)

    client = _FakeS3Client(etag="same")

    result = _sync_with_client(client, "test-bucket", "", dest_dir, dry_run=True)

    assert result == {"changed": False, "version": version, "items": 4}
    assert client.head_calls == [
        ("test-bucket", ITEMS_FILENAME),
        ("test-bucket", COLLECTIONS_FILENAME),
    ]
    assert client.download_calls == []


def test_missing_catalog_key_names_bucket_and_key(tmp_path: Path) -> None:
    dest_dir = tmp_path / "catalog"
    dest_dir.mkdir()

    client = _FakeS3Client(etag=None, head_error_code="NoSuchKey")

    with pytest.raises(FileNotFoundError) as excinfo:
        _sync_with_client(client, "test-bucket", "", dest_dir, dry_run=False)

    message = str(excinfo.value)
    assert "s3://test-bucket/items.parquet" in message
    assert "CATALOG_S3_BUCKET" in message
    assert "C1" not in message
    assert client.download_calls == []
    assert not (dest_dir / "VERSION").exists()


def test_missing_catalog_key_404_status_is_also_not_found(tmp_path: Path) -> None:
    dest_dir = tmp_path / "catalog"
    dest_dir.mkdir()

    client = _FakeS3Client(etag=None, head_error_code="404")

    with pytest.raises(FileNotFoundError, match="s3://test-bucket/items.parquet"):
        _sync_with_client(client, "test-bucket", "", dest_dir, dry_run=False)


def test_other_client_errors_propagate_unchanged(tmp_path: Path) -> None:
    dest_dir = tmp_path / "catalog"
    dest_dir.mkdir()

    client = _FakeS3Client(etag=None, head_error_code="AccessDenied")

    with pytest.raises(ClientError):
        _sync_with_client(client, "test-bucket", "", dest_dir, dry_run=False)


# ────────────────────────────────────────────────────────────────────────
# _parse_s3_url
# ────────────────────────────────────────────────────────────────────────


def test_parse_s3_url_none_returns_none() -> None:
    assert _parse_s3_url(None) is None
    assert _parse_s3_url("") is None


def test_parse_s3_url_extracts_all_parts() -> None:
    target = _parse_s3_url("https://AKID:SECRET@s3.fsn1.de/my-bucket?region=nbg1")

    assert target is not None
    assert target.endpoint_url == "https://s3.fsn1.de"
    assert target.access_key_id == "AKID"
    assert target.secret_access_key == "SECRET"
    assert target.region == "nbg1"
    assert target.bucket == "my-bucket"


def test_parse_s3_url_with_port_and_no_bucket_or_region() -> None:
    target = _parse_s3_url("http://key:secret@localhost:9000")

    assert target is not None
    assert target.endpoint_url == "http://localhost:9000"
    assert target.bucket is None
    assert target.region is None


def test_parse_s3_url_missing_host_raises() -> None:
    with pytest.raises(ValueError, match="host"):
        _parse_s3_url("not-a-url")


# ────────────────────────────────────────────────────────────────────────
# _build_client_and_bucket: which S3 the task talks to
# ────────────────────────────────────────────────────────────────────────

_SHARED_S3_ENV = {
    "S3_PROVIDER": "minio",
    "S3_ENDPOINT_URL": "http://minio:9000",
    "S3_ACCESS_KEY_ID": "minioadmin",
    "S3_SECRET_ACCESS_KEY": "minioadmin",
    "S3_REGION": "us-east-1",
    "S3_BUCKET_NAME": "goat",
}

_CATALOG_S3_ENV = {
    "CATALOG_S3_BUCKET": "goat-catalog",
    "CATALOG_S3_ENDPOINT_URL": "https://acct.r2.cloudflarestorage.com",
    "CATALOG_S3_ACCESS_KEY_ID": "catalog-key",
    "CATALOG_S3_SECRET_ACCESS_KEY": "catalog-secret",
    "CATALOG_S3_REGION": "auto",
}


def _set_env(monkeypatch: pytest.MonkeyPatch, values: dict[str, str]) -> None:
    for name in {*_SHARED_S3_ENV, *_CATALOG_S3_ENV}:
        monkeypatch.delenv(name, raising=False)
    for name, value in values.items():
        monkeypatch.setenv(name, value)


def test_catalog_s3_env_wins_over_shared_s3_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_env(monkeypatch, {**_SHARED_S3_ENV, **_CATALOG_S3_ENV})

    client, bucket = _build_client_and_bucket(SyncCatalogParams())

    assert bucket == "goat-catalog"
    assert client.meta.endpoint_url == "https://acct.r2.cloudflarestorage.com"
    assert client.meta.region_name == "auto"


def test_without_catalog_s3_env_the_shared_bucket_is_used(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_env(monkeypatch, _SHARED_S3_ENV)

    client, bucket = _build_client_and_bucket(SyncCatalogParams())

    assert bucket == "goat"
    assert client.meta.endpoint_url == "http://minio:9000"


def test_s3_url_param_overrides_catalog_s3_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_env(monkeypatch, {**_SHARED_S3_ENV, **_CATALOG_S3_ENV})
    params = SyncCatalogParams(
        s3_url="https://k:s@nbg1.your-objectstorage.com/p4b-catalog-test?region=nbg1"
    )

    client, bucket = _build_client_and_bucket(params)

    assert bucket == "p4b-catalog-test"
    assert client.meta.endpoint_url == "https://nbg1.your-objectstorage.com"
    assert client.meta.region_name == "nbg1"


def test_bucket_param_overrides_catalog_s3_bucket_but_keeps_its_endpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_env(monkeypatch, {**_SHARED_S3_ENV, **_CATALOG_S3_ENV})

    client, bucket = _build_client_and_bucket(SyncCatalogParams(bucket="other"))

    assert bucket == "other"
    assert client.meta.endpoint_url == "https://acct.r2.cloudflarestorage.com"
