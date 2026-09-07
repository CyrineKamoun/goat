"""Build the local catalog mirror from the published stac-geoparquet.

This is the consumer side of the mirror described in
`docs/goat-catalog-design.md` §5: `apps/catalog` serves STAC search off
DuckDB-readable parquet on the shared data volume, and this task is what keeps
those files current. It does *not* touch Postgres and does not upsert rows
anywhere — the catalog is two files, swapped atomically.

(NUTS, the other file the service reads, has its own producer:
`goatlib.tasks.sync_nuts` builds it from Eurostat, not from this bucket.)

The bucket publishes **native stac-geoparquet**: ``items.parquet`` (one row per
STAC Item) and ``collections.parquet`` (one row per Collection). It does *not*
publish the query-shaped mirror `apps/catalog` serves; that shape is built
here, by `goatlib.tasks.catalog_mirror`, so every derived column
(``search_text``, the bundle precomputation, the envelope doubles) stays ours
to change without a harvester contract negotiation.

Algorithm:
    1. ``head_object`` both published keys to read their ETags.
    2. Compare a composite of the two against the local ``VERSION`` marker
       (written by the previous successful sync). Equal ⇒ nothing to do, no
       download: a change to *either* published file must trigger a rebuild,
       so the marker has to cover both.
    3. Otherwise download both, run ``build_mirror`` into two ``.tmp`` files,
       then check each has every column ``apps/catalog`` requires (see
       ``REQUIRED_ITEM_COLUMNS`` / ``REQUIRED_COLLECTION_COLUMNS``) and at
       least one row. A failure here raises, leaving the previous mirror +
       ``VERSION`` untouched — a bad upstream file, or a converter bug, must
       never take the service down.
    4. ``os.replace()`` both validated tmp files over their final paths
       (collections first — see the comment at the call site for the
       microsecond window this leaves and why that ordering is the safer
       one), then write ``VERSION`` (the composite marker) *last*:
       `apps/catalog`'s `CatalogStore.ensure_current()` reloads when the
       marker changes, so writing it last is what bounds a mixed read to the
       gap between the two replaces rather than to the whole build.

Which bucket: the ``s3_url`` param, else the ``bucket`` param, else the
dedicated ``CATALOG_S3_*`` variables (shared with `apps/catalog`), else the
generic ``S3_*`` settings. If neither published key exists in the resolved
bucket this raises ``FileNotFoundError`` naming bucket, key and endpoint —
in practice that means the task resolved to the uploads bucket.

Windmill deployment: registered in `goatlib.tasks.registry` as
`f/goat/tasks/sync_catalog` and synced via `scripts/windmill/sync-tools.sh`
(the *tasks* sync path, not the analytics-tools one — same split as every
other module in `goatlib/tasks/`). Worker tag ``"tools"``. No schedule is
hardcoded as a hard requirement of this module; the registry entry sets a
periodic cron (see `goatlib.tasks.registry`), matching the "Windmill cron"
cadence noted in the design doc.
"""

from __future__ import annotations

import hashlib
import logging
import os
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

import duckdb
from botocore.exceptions import ClientError
from pydantic import BaseModel, Field

from goatlib.tasks.catalog_mirror import (
    COLLECTIONS_FILENAME,
    GUARANTEED_COLLECTION_COLUMNS,
    GUARANTEED_ITEM_COLUMNS,
    ITEMS_FILENAME,
    MIRROR_FORMAT_VERSION,
    build_mirror,
)
from goatlib.tools.base import ToolSettings

logger = logging.getLogger(__name__)

__all__ = [
    "REQUIRED_COLLECTION_COLUMNS",
    "REQUIRED_ITEM_COLUMNS",
    "SyncCatalogParams",
    "main",
]

# What the mirror guarantees, taken from the converter that produces it rather
# than restated here -- two copies of a schema contract is how the last one
# drifted. `apps/catalog` names these columns in its own SQL, and validation
# below is the runtime check that a build really emitted them.
REQUIRED_ITEM_COLUMNS: tuple[str, ...] = tuple(
    name for name, _type in GUARANTEED_ITEM_COLUMNS
) + ("id", "geometry", "member_count")
REQUIRED_COLLECTION_COLUMNS: tuple[str, ...] = tuple(
    name for name, _type in GUARANTEED_COLLECTION_COLUMNS
) + ("id", "geometry", "member_count", "goat:geometryType", "thumbnail_item")

#: The mirror is two files, mirroring how the catalog is published and how the
#: service relates them: item queries never touch collection rows and vice
#: versa. Named distinctly from the published inputs so both can sit in the
#: same directory during a sync.
MIRROR_ITEMS_FILENAME = "mirror_items.parquet"
MIRROR_COLLECTIONS_FILENAME = "mirror_collections.parquet"
_VERSION_FILENAME = "VERSION"
_MISSING_KEY_ERROR_CODES = {"404", "NoSuchKey", "NotFound"}


class SyncCatalogParams(BaseModel):
    """Inputs for the catalog file sync."""

    bucket: str | None = Field(
        default=None, description="S3 bucket holding the catalog mirror. Empty = env."
    )
    prefix: str | None = Field(
        default="",
        description="Prefix under which items.parquet/collections.parquet are "
        "published. Empty = bucket root, which is where they actually live.",
    )
    s3_url: str | None = Field(
        default=None,
        description="Everything for one bucket in a single URL: "
        "https://KEY:SECRET@host/bucket?region=nbg1 . Overrides `bucket`. "
        "Pass a Windmill variable ($var:f/goat/...) to keep the secret out "
        "of the job arguments. Empty = env.",
    )
    dest_dir: str | None = Field(
        default=None,
        description="Local directory to sync into. Empty = ${DATA_DIR}/catalog.",
    )
    dry_run: bool = Field(
        default=False, description="Report what would change, download/write nothing."
    )


@dataclass(frozen=True)
class _S3Target:
    endpoint_url: str | None
    access_key_id: str | None
    secret_access_key: str | None
    region: str | None
    bucket: str | None


def _parse_s3_url(url: str | None) -> _S3Target | None:
    """Parse `https://KEY:SECRET@host[:port]/bucket?region=xyz` into parts.

    Returns None for an empty/missing URL — callers fall back to env-based
    settings entirely in that case.
    """
    if not url:
        return None

    parsed = urlparse(url)
    if not parsed.hostname:
        raise ValueError("invalid s3_url: missing host")

    endpoint = f"{parsed.scheme}://{parsed.hostname}"
    if parsed.port:
        endpoint += f":{parsed.port}"

    bucket = parsed.path.strip("/") or None
    region = parse_qs(parsed.query).get("region", [None])[0]

    return _S3Target(
        endpoint_url=endpoint,
        access_key_id=unquote(parsed.username) if parsed.username else None,
        secret_access_key=unquote(parsed.password) if parsed.password else None,
        region=region,
        bucket=bucket,
    )


def _default_dest_dir() -> Path:
    return Path(os.environ.get("DATA_DIR", "/app/data")) / "catalog"


def _catalog_s3_target() -> _S3Target | None:
    """The dedicated catalog bucket, from ``CATALOG_S3_*``.

    These are the same variables ``apps/catalog`` reads to serve previews, so
    one set of env keys points both consumers at the same bucket. The shared
    ``S3_*`` settings describe the *uploads* bucket, which never holds the
    published catalog; falling back to them is only right when no dedicated
    bucket is configured at all.
    """
    bucket = ToolSettings._get_secret("CATALOG_S3_BUCKET", "") or None
    if bucket is None:
        return None
    return _S3Target(
        endpoint_url=ToolSettings._get_secret("CATALOG_S3_ENDPOINT_URL", "") or None,
        access_key_id=ToolSettings._get_secret("CATALOG_S3_ACCESS_KEY_ID", "") or None,
        secret_access_key=ToolSettings._get_secret("CATALOG_S3_SECRET_ACCESS_KEY", "")
        or None,
        region=ToolSettings._get_secret("CATALOG_S3_REGION", "") or None,
        bucket=bucket,
    )


def _build_client_and_bucket(params: SyncCatalogParams) -> tuple[Any, str]:
    """Resolve an S3 client + bucket from params/env.

    Precedence, highest first: the ``s3_url`` param (everything for one
    bucket), then the ``bucket`` param, then the dedicated ``CATALOG_S3_*``
    variables, then the shared ``S3_*`` settings. Connection details come
    from the most specific source that has them, so ``bucket`` alone still
    talks to the catalog endpoint when one is configured.
    """
    settings = ToolSettings.from_env()
    url_target = _parse_s3_url(params.s3_url)
    target = url_target or _catalog_s3_target()

    if target is not None:
        settings = replace(
            settings,
            s3_endpoint_url=target.endpoint_url or settings.s3_endpoint_url,
            s3_access_key_id=target.access_key_id or settings.s3_access_key_id,
            s3_secret_access_key=target.secret_access_key
            or settings.s3_secret_access_key,
            s3_region_name=target.region or settings.s3_region_name,
        )

    bucket = (
        (url_target.bucket if url_target else None)
        or params.bucket
        or (target.bucket if target else None)
        or settings.s3_bucket_name
        or "goat"
    )
    return settings.get_s3_client(), bucket


def _row_count(path: Path) -> int:
    con = duckdb.connect()
    try:
        row = con.execute(
            "SELECT count(*) FROM read_parquet(?)", [str(path)]
        ).fetchone()
        return int(row[0]) if row is not None else 0
    finally:
        con.close()


def _validate_and_count(path: Path, required: tuple[str, ...]) -> int:
    """Open `path` with DuckDB, check required columns + row count.

    Raises ValueError on any validation failure (unreadable file, missing
    column, zero rows) — never partially trusts a bad file.

    Since the file is now built here rather than downloaded, this doubles as a
    self-check on the converter: `build_mirror` emitting a column that
    ``apps/catalog`` does not expect (or dropping one it does) fails the sync
    and leaves the previous mirror serving, instead of swapping in a file the
    service cannot query.
    """
    con = duckdb.connect()
    try:
        try:
            described = con.execute(
                "DESCRIBE SELECT * FROM read_parquet(?)", [str(path)]
            ).fetchall()
        except duckdb.Error as exc:
            raise ValueError(f"built catalog.parquet is not readable: {exc}") from exc

        columns = {row[0] for row in described}
        missing = [c for c in required if c not in columns]
        if missing:
            raise ValueError(
                "built catalog.parquet is missing required column(s): "
                f"{', '.join(missing)}"
            )

        count = _row_count(path)
        if count == 0:
            raise ValueError("built catalog.parquet has zero rows")
        return count
    finally:
        con.close()


def _raise_missing_key(
    exc: ClientError, client: Any, bucket: str, prefix: str, key: str
) -> None:
    """Turn a 404 on a published key into a message that says where we looked.

    A missing ``items.parquet`` almost always means the task resolved to the
    wrong bucket (typically the shared uploads bucket because no
    ``CATALOG_S3_*`` is set), so the error names bucket, key and endpoint and
    the variables that steer them.
    """
    error = exc.response.get("Error", {})
    code = str(error.get("Code", ""))
    status = str(exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode", ""))
    if code in _MISSING_KEY_ERROR_CODES or status == "404":
        endpoint = getattr(getattr(client, "meta", None), "endpoint_url", None)
        where = f"s3://{bucket}/{key}"
        if endpoint:
            where += f" (endpoint {endpoint})"
        raise FileNotFoundError(
            f"{where} does not exist. The published catalog is items.parquet + "
            "collections.parquet at the root of the catalog bucket; this bucket "
            "has neither. Check the `s3_url`/`bucket` params or the "
            "CATALOG_S3_BUCKET / CATALOG_S3_ENDPOINT_URL / CATALOG_S3_ACCESS_KEY_ID / "
            "CATALOG_S3_SECRET_ACCESS_KEY / CATALOG_S3_REGION variables of the "
            f"worker (prefix={prefix!r})."
        ) from exc
    raise exc


def _composite_version(etags: dict[str, str]) -> str:
    """One marker covering every published input.

    A change to either ``items.parquet`` or ``collections.parquet`` has to
    rebuild the mirror -- a collection-only edit changes the licence and
    publisher denormalised onto every item -- so the marker cannot be one
    file's ETag. Hashed rather than concatenated to keep it the same shape as
    the single ETag it replaces, which is what `apps/catalog` stamps as its
    ETag seed.

    ``MIRROR_FORMAT_VERSION`` is in the hash because the inputs are not the only
    thing that decides the output: a change to the converter produces a different
    mirror from identical files, and without it every deployment would keep
    serving the old one -- the ETags it compares would not have moved. Bumping
    that constant forces exactly one rebuild.
    """
    joined = "\n".join(f"{name}:{etags[name]}" for name in sorted(etags))
    joined = f"v{MIRROR_FORMAT_VERSION}\n{joined}"
    return hashlib.sha256(joined.encode()).hexdigest()[:32]


def _sync_with_client(
    client: Any, bucket: str, prefix: str, dest_dir: Path, *, dry_run: bool
) -> dict[str, Any]:
    """Core sync algorithm, driven by an already-built S3 client.

    Kept separate from client construction so it is unit-testable with a
    small fake client (head_object/download_file) instead of a real
    boto3/botocore stack.
    """

    # The published files sit at the bucket root today; a prefix is still
    # supported, but must not introduce a leading slash when it is empty.
    def _key(name: str) -> str:
        return f"{prefix}/{name}" if prefix else name

    sources = {
        ITEMS_FILENAME: _key(ITEMS_FILENAME),
        COLLECTIONS_FILENAME: _key(COLLECTIONS_FILENAME),
    }

    etags: dict[str, str] = {}
    for name, key in sources.items():
        try:
            head = client.head_object(Bucket=bucket, Key=key)
        except ClientError as exc:
            _raise_missing_key(exc, client, bucket, prefix, key)
            raise  # pragma: no cover - _raise_missing_key always raises
        etags[name] = str(head.get("ETag", "")).strip('"')

    version = _composite_version(etags)

    version_path = dest_dir / _VERSION_FILENAME
    final_path = dest_dir / MIRROR_ITEMS_FILENAME
    try:
        local_version = version_path.read_text().strip()
    except FileNotFoundError:
        local_version = None

    if local_version == version:
        items = _row_count(final_path) if final_path.exists() else -1
        return {"changed": False, "version": version, "items": items}

    if dry_run:
        return {"changed": True, "version": version, "items": -1}

    dest_dir.mkdir(parents=True, exist_ok=True)
    downloads = {name: dest_dir / f"{name}.tmp" for name in sources}
    tmp_items = dest_dir / f"{MIRROR_ITEMS_FILENAME}.tmp"
    tmp_collections = dest_dir / f"{MIRROR_COLLECTIONS_FILENAME}.tmp"
    scratch = [*downloads.values(), tmp_items, tmp_collections]
    try:
        for name, key in sources.items():
            client.download_file(bucket, key, str(downloads[name]))

        build_mirror(
            downloads[ITEMS_FILENAME],
            downloads[COLLECTIONS_FILENAME],
            tmp_items,
            tmp_collections,
        )
        items = _validate_and_count(tmp_items, REQUIRED_ITEM_COLUMNS)
        _validate_and_count(tmp_collections, REQUIRED_COLLECTION_COLUMNS)
    except Exception:
        for path in scratch:
            path.unlink(missing_ok=True)
        raise

    # The published inputs are scratch: only the built mirror is served, and
    # keeping them would double the volume's footprint at the 1M-item target.
    for path in downloads.values():
        path.unlink(missing_ok=True)

    # Each `os.replace` is atomic on its own -- a reader mid-query holds the
    # old inode until it closes -- but the *pair* is not: two calls cannot be
    # one transaction, and `apps/catalog` reads the files through views that
    # re-open them per query. So a query issued in the microseconds between
    # these two lines can see new items beside old collections.
    #
    # What that costs, bounded honestly: item rows already carry their
    # denormalised licence/publisher/category, so a mixed read affects
    # collection-side fields only (a collection's `member_count`, title, or a
    # collection that does not exist yet). It self-heals on the VERSION write
    # below, which moves the marker and makes the next request rebuild.
    #
    # Collections go first deliberately: of the two mixed states, "a
    # collection whose items have not appeared yet" degrades to an empty item
    # list, while "an item whose collection is missing" 404s a link the item
    # itself advertises.
    #
    # Closing the window entirely would mean version-stamped filenames named
    # by VERSION (one atomic write, plus GC of the old pair). Not built: the
    # exposure is microseconds on a monthly harvest, and the cost is a
    # lifecycle to get wrong.
    os.replace(tmp_collections, dest_dir / MIRROR_COLLECTIONS_FILENAME)
    os.replace(tmp_items, dest_dir / MIRROR_ITEMS_FILENAME)
    # VERSION written LAST: apps/catalog's CatalogStore only reloads when
    # this marker's content changes, so this is what makes the swap atomic
    # from a reader's point of view.
    version_path.write_text(version)

    return {"changed": True, "version": version, "items": items}


def _sync(params: SyncCatalogParams) -> dict[str, Any]:
    client, bucket = _build_client_and_bucket(params)
    prefix = (params.prefix or "").strip().strip("/")
    dest_dir = Path(params.dest_dir) if params.dest_dir else _default_dest_dir()
    return _sync_with_client(client, bucket, prefix, dest_dir, dry_run=params.dry_run)


def main(params: SyncCatalogParams = SyncCatalogParams()) -> dict[str, Any]:
    """Entry point for the Windmill task."""
    result = _sync(params)
    logger.info("catalog sync: %s", result)
    return result
