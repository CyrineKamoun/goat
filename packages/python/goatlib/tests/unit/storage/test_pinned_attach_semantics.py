"""Pin-semantics tests against a file-backed DuckLake (no PG required).

These tests prove the DuckDB behaviors the pinned-read design relies on:
- SNAPSHOT_VERSION attach does not see later snapshots (churn immunity)
- a fresh attach at the newer snapshot does see them (refresh works)
- the exact error shapes for missing tables and expired snapshots.

ADAPTATION FROM THE ORIGINAL BRIEF: the installed ducklake extension (bundled
with duckdb 1.4.4) does not allow two simultaneous ATTACHes of the same
physical metadata file within a single OS process -- not even under a
different alias, and not even with READ_ONLY. Attempting it raises a Binder
Error ("Unique file handle conflict: Cannot attach ... - the database file
... is already attached by database ..."). A DETACH (no connection close
required) on the first attacher releases the file so another connection can
attach it.

This means a long-lived writer connection and a long-lived pinned-reader
connection cannot coexist attached to the same catalog file in one process
-- but this constraint is specific to FILE-BACKED catalogs, which is what
this test's fixture uses. It does not generalize to the production
PG-backed catalog: DuckLake's postgres catalog backend supports multiple
concurrent attaches to the same catalog (production holds several -- the
manager's single connection plus the pool's several -- attached at once,
each independently pinned). The tests below insert explicit `DETACH lake`
calls between attach/write steps to route around the file-backed
single-attach constraint while still proving the pin invariant: a pin
recorded at snapshot N never sees objects created in snapshot N+1+, even
when the pinned connection is re-attached later at the same recorded
snapshot version.
"""

from collections.abc import Callable
from pathlib import Path

import duckdb
import pytest
from goatlib.storage.pin_errors import is_pin_miss_error

LakeAttach = Callable[..., duckdb.DuckDBPyConnection]


@pytest.fixture()
def lake(tmp_path: Path) -> LakeAttach:
    meta = tmp_path / "meta.ducklake"
    data = tmp_path / "data"
    data.mkdir()

    def attach(snapshot_version: int | None = None) -> duckdb.DuckDBPyConnection:
        con = duckdb.connect()
        con.execute("INSTALL ducklake; LOAD ducklake")
        opts = f"DATA_PATH '{data}'"
        if snapshot_version is not None:
            opts += f", SNAPSHOT_VERSION {snapshot_version}"
        con.execute(f"ATTACH 'ducklake:{meta}' AS lake ({opts})")
        return con

    return attach


def latest_snapshot(con: duckdb.DuckDBPyConnection) -> int:
    row = con.execute(
        "SELECT max(snapshot_id) FROM ducklake_snapshots('lake')"
    ).fetchone()
    assert row is not None
    return int(row[0])


def test_pinned_reader_does_not_see_later_tables(lake: LakeAttach) -> None:
    w = lake()
    w.execute("CREATE TABLE lake.t1 AS SELECT 1 AS x")
    pin = latest_snapshot(w)
    w.execute("DETACH lake")  # release the file so the reader can attach it

    r = lake(snapshot_version=pin)
    assert r.execute("SELECT count(*) FROM lake.t1").fetchone() == (1,)
    r.execute("DETACH lake")  # release the file so the writer can attach it

    w2 = lake()
    w2.execute("CREATE TABLE lake.t2 AS SELECT 2 AS x")
    new_pin = latest_snapshot(w2)
    w2.execute("DETACH lake")

    # a reader re-attached at the SAME old pin still serves t1 and does NOT
    # see t2 -- the pin is durable, not just a point-in-time query cursor
    r2 = lake(snapshot_version=pin)
    assert r2.execute("SELECT count(*) FROM lake.t1").fetchone() == (1,)
    with pytest.raises(duckdb.Error) as exc_info:
        r2.execute("SELECT count(*) FROM lake.t2").fetchone()
    assert is_pin_miss_error(exc_info.value)
    r2.execute("DETACH lake")

    # a fresh attach at the new snapshot sees t2 (this is what refresh does)
    r3 = lake(snapshot_version=new_pin)
    assert r3.execute("SELECT count(*) FROM lake.t2").fetchone() == (1,)


def test_column_error_is_not_a_pin_miss(lake: LakeAttach) -> None:
    w = lake()
    w.execute("CREATE TABLE lake.t3 AS SELECT 1 AS x")
    pin = latest_snapshot(w)
    w.execute("DETACH lake")

    r = lake(snapshot_version=pin)
    with pytest.raises(duckdb.Error) as exc_info:
        r.execute("SELECT nope FROM lake.t3").fetchone()
    assert not is_pin_miss_error(exc_info.value)


def test_table_already_exists_error_is_not_a_pin_miss(lake: LakeAttach) -> None:
    w = lake()
    w.execute("CREATE TABLE lake.t6 AS SELECT 1 AS x")
    with pytest.raises(duckdb.Error) as exc_info:
        w.execute("CREATE TABLE lake.t6 AS SELECT 1 AS x")
    assert not is_pin_miss_error(exc_info.value)


def test_expired_snapshot_error_is_a_pin_miss(lake: LakeAttach) -> None:
    w = lake()
    w.execute("CREATE TABLE lake.t4 AS SELECT 1 AS x")
    old = latest_snapshot(w)
    w.execute("CREATE TABLE lake.t5 AS SELECT 1 AS x")
    w.execute("CALL ducklake_expire_snapshots('lake', older_than => now()::TIMESTAMP)")
    w.execute("DETACH lake")

    with pytest.raises(duckdb.Error) as exc_info:
        lake(snapshot_version=old)
    assert is_pin_miss_error(exc_info.value)
