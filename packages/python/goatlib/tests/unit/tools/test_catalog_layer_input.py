"""A promoted catalog layer as a tool's INPUT.

It is one immutable parquet file read through a view, not a DuckLake table, and
every path a tool takes to reach its data has to cope with that.
"""

from pathlib import Path

import duckdb
import pytest
from goatlib.tools.base import BaseToolRunner
from goatlib.tools.if_node import _resolve_layer_sql_ref
from goatlib.utils.layer import catalog_layer_relation

LAYER = "3fa85f64-5717-4562-b3fc-2c963f66afa6"
TABLE = "t_3fa85f6457174562b3fc2c963f66afa6"


def _write_layer(dir_: Path) -> Path:
    """Three points with a `category` column, as materialize would write them."""
    dir_.mkdir(parents=True, exist_ok=True)
    path = dir_ / f"{TABLE}.parquet"
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    con.execute(
        f"""
        COPY (
            SELECT * FROM (VALUES
                (1, 'school',   ST_Point(11.5, 48.1)),
                (2, 'hospital', ST_Point(11.6, 48.2)),
                (3, 'school',   ST_Point(11.7, 48.3))
            ) AS t(id, category, geometry)
        ) TO '{path}' (FORMAT PARQUET)
        """
    )
    con.close()
    return path


class _Runner(BaseToolRunner):
    """The base class with a plain in-memory DuckDB in place of the DuckLake one."""

    def __init__(self) -> None:
        super().__init__()
        self._fresh_connections = 0

    def _get_duckdb_connection(self) -> duckdb.DuckDBPyConnection:
        self._fresh_connections += 1
        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        return con

    def process(self, params, temp_dir):  # noqa: ANN001, ANN201 - not exercised
        raise NotImplementedError


@pytest.fixture()
def catalog_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv("CATALOG_LAYERS_DIR", str(tmp_path / "layers"))
    _write_layer(tmp_path / "layers")
    return tmp_path / "layers"


class TestFilteredExport:
    def test_a_cql_filter_is_applied_to_a_catalog_layer(
        self, catalog_dir: Path, tmp_path: Path
    ) -> None:
        """The filter must reach the COPY. It used to crash on a misnamed
        attribute, and behind that it passed the wrong argument shape, so the
        filter was silently dropped and the whole layer exported."""
        runner = _Runner()
        out = runner._export_catalog_filtered(
            catalog_dir / f"{TABLE}.parquet",
            {"op": "=", "args": [{"property": "category"}, "school"]},
        )
        rows = (
            duckdb.connect().execute(f"SELECT id FROM '{out}' ORDER BY id").fetchall()
        )
        assert rows == [(1,), (3,)], "only the two schools should survive the filter"


class TestViewSurvivesReconnect:
    def test_view_is_replayed_on_a_new_connection(self, catalog_dir: Path) -> None:
        """`_execute_with_retry` drops the connection on a transient error and
        retries on a fresh one; the catalog view must still be there."""
        runner = _Runner()
        rel = runner._ensure_catalog_view(LAYER, catalog_dir / f"{TABLE}.parquet")
        assert rel == catalog_layer_relation(LAYER)
        assert runner.duckdb_con.execute(f"SELECT count(*) FROM {rel}").fetchone() == (
            3,
        )

        # what the retry path does
        runner._duckdb_con.close()
        runner._duckdb_con = None

        assert runner._fresh_connections == 1
        assert runner.duckdb_con.execute(f"SELECT count(*) FROM {rel}").fetchone() == (
            3,
        )
        assert runner._fresh_connections == 2


class TestIfNode:
    def test_resolves_a_catalog_layer_to_its_file(self, catalog_dir: Path) -> None:
        """The if-node has its own resolver; without the catalog branch it names
        a DuckLake table that does not exist and the broad excepts turn that into
        a silently wrong branch."""
        ref = _resolve_layer_sql_ref(LAYER, "00000000-0000-0000-0000-000000000000")
        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        assert con.execute(f"SELECT count(*) FROM {ref}").fetchone() == (3,)
