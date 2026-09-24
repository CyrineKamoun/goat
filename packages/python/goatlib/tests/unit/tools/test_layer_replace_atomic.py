"""Replacing a layer's table in place is all or nothing.

``_replace_ducklake_table`` drops the table and recreates it from a parquet
file. Run as separate statements, a failure after the drop — an unreadable
file, a killed worker — left the layer with no table at all until the next
successful run. Workflow re-runs and ``layer_update`` both go through it.
"""

from pathlib import Path
from typing import Any

import duckdb
import pytest
from goatlib.tools.layer_replace import LayerReplaceMixin


class _Host(LayerReplaceMixin):
    settings: Any = None

    def __init__(self, con: duckdb.DuckDBPyConnection) -> None:
        self._con = con

    @property
    def duckdb_con(self) -> duckdb.DuckDBPyConnection:
        return self._con

    def resolve_layer_table_path(self, layer_id: str) -> str:
        return f"lake.main.t_{layer_id}"


@pytest.fixture()
def lake(tmp_path: Path) -> duckdb.DuckDBPyConnection:
    data = tmp_path / "data"
    data.mkdir()
    con = duckdb.connect()
    con.execute("INSTALL ducklake; LOAD ducklake")
    con.execute(
        f"ATTACH 'ducklake:{tmp_path / 'meta.ducklake'}' AS lake (DATA_PATH '{data}')"
    )
    con.execute("CREATE TABLE lake.main.t_abc AS SELECT * FROM (VALUES (1), (2)) v(x)")
    return con


def test_failed_replace_keeps_the_old_data(
    lake: duckdb.DuckDBPyConnection, tmp_path: Path
) -> None:
    with pytest.raises(duckdb.Error):
        _Host(lake)._replace_ducklake_table(
            layer_id="abc",
            owner_id="someone",
            parquet_path=tmp_path / "missing.parquet",
        )

    assert lake.execute("SELECT x FROM lake.main.t_abc ORDER BY x").fetchall() == [
        (1,),
        (2,),
    ]


def test_replace_swaps_in_the_new_data(
    lake: duckdb.DuckDBPyConnection, tmp_path: Path
) -> None:
    parquet = tmp_path / "new.parquet"
    lake.execute(f"COPY (SELECT 10 AS x) TO '{parquet}' (FORMAT parquet)")

    info = _Host(lake)._replace_ducklake_table(
        layer_id="abc", owner_id="someone", parquet_path=parquet
    )

    assert lake.execute("SELECT x FROM lake.main.t_abc").fetchall() == [(10,)]
    assert info["feature_count"] == 1


def test_a_failed_commit_reports_its_own_error(
    lake: duckdb.DuckDBPyConnection, tmp_path: Path
) -> None:
    """DuckDB ends the transaction when COMMIT fails (e.g. a write conflict);
    a ROLLBACK after that raises "no transaction is active" and would hide
    the conflict that actually happened."""
    parquet = tmp_path / "new.parquet"
    lake.execute(f"COPY (SELECT 10 AS x) TO '{parquet}' (FORMAT parquet)")

    class _ConflictOnCommit:
        def __init__(self, con: duckdb.DuckDBPyConnection) -> None:
            self._con = con

        def execute(self, sql: str, *args: Any) -> Any:
            if sql.strip().upper() == "COMMIT":
                self._con.execute("ROLLBACK")
                raise duckdb.TransactionException("write-write conflict on t_abc")
            return self._con.execute(sql, *args)

    with pytest.raises(duckdb.TransactionException, match="write-write conflict"):
        _Host(_ConflictOnCommit(lake))._replace_ducklake_table(  # type: ignore[arg-type]
            layer_id="abc", owner_id="someone", parquet_path=parquet
        )
