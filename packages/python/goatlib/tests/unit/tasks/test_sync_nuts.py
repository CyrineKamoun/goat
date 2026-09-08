"""Unit tests for the NUTS reference-data sync.

No network: the GISCO download is monkeypatched to write a small GeoJSON with
the real column names, so the conversion, the validation gate and the
release-marker logic are all exercised against the shape the service reads.
"""

import json
from pathlib import Path
from typing import Any

import duckdb
import pytest
from goatlib.tasks.sync_nuts import (
    NUTS_FILENAME,
    REQUIRED_NUTS_COLUMNS,
    SyncNutsParams,
    _sync,
    build_nuts,
)


def _feature(nuts_id: str, name: str, level: int, x: float) -> dict[str, Any]:
    return {
        "type": "Feature",
        "properties": {
            "NUTS_ID": nuts_id,
            "NAME_LATN": name,
            "LEVL_CODE": level,
            "CNTR_CODE": nuts_id[:2],
        },
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[x, 0.0], [x + 1, 0.0], [x + 1, 1.0], [x, 1.0], [x, 0.0]]],
        },
    }


def _write_geojson(path: Path, count: int) -> None:
    features = [
        _feature(f"DE{i:03d}", f"Region {i}", i % 4, float(i)) for i in range(count)
    ]
    path.write_text(
        json.dumps({"type": "FeatureCollection", "features": features}),
    )


@pytest.fixture()
def fake_download(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Replace the GISCO fetch; records the URLs it was asked for."""
    calls: list[str] = []

    def _fake(url: str, target: Path) -> None:
        calls.append(url)
        _write_geojson(target, 1200)

    monkeypatch.setattr("goatlib.tasks.sync_nuts._download", _fake)
    return calls


def test_gisco_columns_are_mapped_to_the_service_schema(tmp_path: Path) -> None:
    """The service names the columns; GISCO's spellings stop here.

    `apps/catalog` queries `nuts_id`/`nuts_name`/`level`/`country`, not
    `NUTS_ID`/`NAME_LATN`/`LEVL_CODE`/`CNTR_CODE`.
    """
    source = tmp_path / "nuts.geojson"
    _write_geojson(source, 5)
    out = tmp_path / NUTS_FILENAME

    assert build_nuts(source, out) == 5

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    described = con.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{out.as_posix()}')"
    ).fetchall()
    columns = {row[0] for row in described}
    row = con.execute(
        f"SELECT nuts_id, nuts_name, level, country FROM "
        f"read_parquet('{out.as_posix()}') ORDER BY nuts_id LIMIT 1"
    ).fetchone()
    con.close()

    assert set(REQUIRED_NUTS_COLUMNS) <= columns
    assert row == ("DE000", "Region 0", 0, "DE")


def test_happy_path_writes_the_file_and_the_marker(
    tmp_path: Path, fake_download: list[str]
) -> None:
    result = _sync(SyncNutsParams(dest_dir=str(tmp_path)))

    assert result == {"changed": True, "version": "2024-01M", "regions": 1200}
    assert (tmp_path / NUTS_FILENAME).exists()
    assert (tmp_path / "NUTS_VERSION").read_text() == "2024-01M"
    assert fake_download == [
        "https://gisco-services.ec.europa.eu/distribution/v2/nuts/geojson/"
        "NUTS_RG_01M_2024_4326.geojson"
    ]
    assert not (tmp_path / f"{NUTS_FILENAME}.tmp").exists()


def test_same_release_short_circuits_without_downloading(
    tmp_path: Path, fake_download: list[str]
) -> None:
    """A GISCO release is immutable, so the (year, resolution) pair is the ETag."""
    _sync(SyncNutsParams(dest_dir=str(tmp_path)))
    fake_download.clear()

    result = _sync(SyncNutsParams(dest_dir=str(tmp_path)))

    assert result == {"changed": False, "version": "2024-01M", "regions": -1}
    assert fake_download == []


def test_force_redownloads_the_same_release(
    tmp_path: Path, fake_download: list[str]
) -> None:
    _sync(SyncNutsParams(dest_dir=str(tmp_path)))
    fake_download.clear()

    result = _sync(SyncNutsParams(dest_dir=str(tmp_path), force=True))

    assert result["changed"] is True
    assert len(fake_download) == 1


def test_a_new_release_year_triggers_a_rebuild(
    tmp_path: Path, fake_download: list[str]
) -> None:
    _sync(SyncNutsParams(dest_dir=str(tmp_path)))
    fake_download.clear()

    result = _sync(SyncNutsParams(dest_dir=str(tmp_path), year=2021))

    assert result["version"] == "2021-01M"
    assert "NUTS_RG_01M_2021_4326.geojson" in fake_download[0]


def test_dry_run_downloads_and_writes_nothing(
    tmp_path: Path, fake_download: list[str]
) -> None:
    result = _sync(SyncNutsParams(dest_dir=str(tmp_path), dry_run=True))

    assert result == {"changed": True, "version": "2024-01M", "regions": -1}
    assert fake_download == []
    assert not (tmp_path / NUTS_FILENAME).exists()
    assert not (tmp_path / "NUTS_VERSION").exists()


def test_a_truncated_download_never_replaces_a_good_file(
    tmp_path: Path, fake_download: list[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """A short file is a broken fetch, not a smaller Europe.

    Without the row-count gate, a half-written GeoJSON would silently replace
    a complete one and the spatial filter would lose most of its regions.
    """
    _sync(SyncNutsParams(dest_dir=str(tmp_path)))
    good = (tmp_path / NUTS_FILENAME).read_bytes()

    def _truncated(url: str, target: Path) -> None:
        _write_geojson(target, 3)

    monkeypatch.setattr("goatlib.tasks.sync_nuts._download", _truncated)

    with pytest.raises(ValueError, match="refusing to replace"):
        _sync(SyncNutsParams(dest_dir=str(tmp_path), year=2021))

    assert (tmp_path / NUTS_FILENAME).read_bytes() == good
    assert (tmp_path / "NUTS_VERSION").read_text() == "2024-01M"
    assert not (tmp_path / f"{NUTS_FILENAME}.tmp").exists()


def test_unknown_resolution_is_rejected_before_any_download(
    tmp_path: Path, fake_download: list[str]
) -> None:
    with pytest.raises(ValueError, match="unknown resolution"):
        _sync(SyncNutsParams(dest_dir=str(tmp_path), resolution="99M"))
    assert fake_download == []
