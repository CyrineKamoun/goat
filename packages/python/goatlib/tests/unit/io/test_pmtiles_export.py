"""The GeoJSON export that feeds tippecanoe.

GDAL's GeoJSON writer converts a date through a unix-time path that rejects
anything before 1970 — `Invalid unixTime` — and the error fails the export, so
ONE historical date costs the whole layer its tiles. A public catalog is full
of them (protection dates, construction years, survey dates), and the layer
still renders from dynamic tiles, so the loss is silent apart from the
`tiles: failed` marker on the layer.
"""

import duckdb
import pytest
from goatlib.io.pmtiles import PMTilesConfig, PMTilesGenerator


@pytest.fixture()
def con() -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect()
    connection.execute("INSTALL spatial; LOAD spatial;")
    return connection


def _export(generator, con, sql, tmp_path, name="out.geojson"):
    con.execute(f"CREATE OR REPLACE VIEW v AS {sql}")
    out = tmp_path / name
    generator._export_to_geojson(
        duckdb_con=con, table_name="v", output_path=out, geometry_column="geometry"
    )
    return out


@pytest.fixture()
def generator(tmp_path) -> PMTilesGenerator:
    return PMTilesGenerator(tiles_data_dir=tmp_path, config=PMTilesConfig())


def test_a_date_before_1970_still_exports(generator, con, tmp_path) -> None:
    out = _export(
        generator,
        con,
        "SELECT 0 AS rowid, DATE '1919-01-03' AS surveyed, ST_Point(7.6, 47.5) AS geometry",
        tmp_path,
    )

    assert out.exists()
    assert "1919-01-03" in out.read_text()


def test_a_timestamp_before_1970_still_exports(generator, con, tmp_path) -> None:
    out = _export(
        generator,
        con,
        "SELECT 0 AS rowid, TIMESTAMP '1943-06-01 08:30:00' AS recorded, "
        "ST_Point(7.6, 47.5) AS geometry",
        tmp_path,
    )

    assert out.exists()
    assert "1943-06-01T08:30:00" in out.read_text()


def test_modern_dates_are_written_exactly_as_before(generator, con, tmp_path) -> None:
    """Only the columns that would fail are rewritten, so nothing else moves."""
    out = _export(
        generator,
        con,
        "SELECT 0 AS rowid, DATE '2021-10-01' AS d, "
        "TIMESTAMP '2021-10-01 14:30:00' AS ts, ST_Point(7.6, 47.5) AS geometry",
        tmp_path,
    )

    text = out.read_text()
    assert '"d": "2021-10-01"' in text
    assert '"ts": "2021-10-01T14:30:00"' in text


def test_a_column_mixing_old_and_new_dates_exports(generator, con, tmp_path) -> None:
    """The real shape: one historical row among current ones."""
    out = _export(
        generator,
        con,
        "SELECT * FROM (VALUES "
        "(0, DATE '1919-01-03', ST_Point(7.6, 47.5)), "
        "(1, DATE '2021-10-01', ST_Point(7.7, 47.6))"
        ") AS t(rowid, surveyed, geometry)",
        tmp_path,
    )

    text = out.read_text()
    assert "1919-01-03" in text and "2021-10-01" in text
