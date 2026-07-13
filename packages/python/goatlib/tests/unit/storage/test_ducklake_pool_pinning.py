"""Pool swap/generation logic tests with stubbed connections (no DuckDB)."""

import queue
import time
from typing import Any, Generator

import pytest
from goatlib.storage.ducklake import DuckLakePool


class FakeCon:
    def __init__(self, snapshot: int | None) -> None:
        self.snapshot = snapshot
        self.closed = False

    def close(self) -> None:
        self.closed = True

    def execute(self, *_: Any) -> "FakeCon":
        if self.closed:
            raise RuntimeError("closed")
        return self

    def fetchall(self) -> list[tuple[int]]:
        return [(1,)]

    def fetchone(self) -> tuple[int]:
        return (1,)


@pytest.fixture()
def pool(monkeypatch: pytest.MonkeyPatch) -> DuckLakePool:
    p = DuckLakePool(pool_size=2, pin_snapshot=True)
    created: list[FakeCon] = []

    def fake_create(snapshot_version: int | None = None) -> FakeCon:
        con = FakeCon(snapshot_version)
        created.append(con)
        return con

    monkeypatch.setattr(p, "_create_connection", fake_create)
    monkeypatch.setattr(
        p,
        "_create_connection_with_retry",
        lambda max_retries=3, retry_delay=1.0, snapshot_version=None: fake_create(
            snapshot_version
        ),
    )
    monkeypatch.setattr(p, "_warm_connection", lambda con: None)
    monkeypatch.setattr(p, "_fetch_latest_snapshot_id", lambda: 10)
    p._test_created = created  # type: ignore[attr-defined]
    # Simulate init(): fill the pool at snapshot 10, generation 0
    for _ in range(2):
        p._pool.put((fake_create(10), time.time(), p._generation))  # type: ignore[arg-type]
    p._initialized = True
    return p


def pool_entries(p: DuckLakePool) -> list[tuple[Any, float, int]]:
    items = []
    while True:
        try:
            items.append(p._pool.get_nowait())
        except queue.Empty:
            break
    for it in items:
        p._pool.put(it)
    return items


def test_apply_snapshot_swaps_all_connections(pool: DuckLakePool) -> None:
    old = [e[0] for e in pool_entries(pool)]
    pool._apply_snapshot(11)
    entries = pool_entries(pool)
    assert len(entries) == 2
    assert all(e[0].snapshot == 11 for e in entries)
    assert all(e[2] == pool._generation for e in entries)
    assert all(c.closed for c in old)


def test_checked_out_stale_connection_closed_on_return(pool: DuckLakePool) -> None:
    with pool.connection() as con:
        pool._apply_snapshot(11)  # rebuild while one conn is checked out
        held = con
    assert held.closed  # type: ignore[attr-defined]  # stale gen: closed, not re-pooled
    entries = pool_entries(pool)
    assert len(entries) == 2
    assert all(e[0].snapshot == 11 for e in entries)


def test_miss_triggers_force_refresh_and_retry(
    pool: DuckLakePool, monkeypatch: pytest.MonkeyPatch
) -> None:
    from goatlib.storage.snapshot_pin import SnapshotPin

    pool._pin = SnapshotPin(
        pool._fetch_latest_snapshot_id, pool._apply_snapshot, min_refresh_gap=0.0
    )
    pool._pin._current = 9  # behind: latest is 10

    calls = {"n": 0}

    class MissThenOk:
        def execute(self, *_: Any) -> "MissThenOk":
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("Catalog Error: Table with name t_x does not exist!")
            return self

        def fetchall(self) -> list[tuple[int]]:
            return [(42,)]

        def fetchone(self) -> tuple[int]:
            return (42,)

        def close(self) -> None:
            pass

    import contextlib

    @contextlib.contextmanager
    def fake_conn() -> Generator[Any, None, None]:
        yield MissThenOk()

    monkeypatch.setattr(pool, "connection", fake_conn)
    result = pool.execute_with_retry("SELECT 1", fetch_all=True)
    assert result == [(42,)]
    assert calls["n"] == 2
    assert pool._pin.current == 10  # force_refresh advanced the pin


def test_genuine_missing_table_still_raises(
    pool: DuckLakePool, monkeypatch: pytest.MonkeyPatch
) -> None:
    from goatlib.storage.snapshot_pin import SnapshotPin

    pool._pin = SnapshotPin(
        pool._fetch_latest_snapshot_id, pool._apply_snapshot, min_refresh_gap=0.0
    )
    pool._pin._current = (
        10  # already at latest: refresh returns True, retry fails again
    )

    import contextlib

    class AlwaysMiss:
        def execute(self, *_: Any) -> "AlwaysMiss":
            raise RuntimeError("Catalog Error: Table with name t_gone does not exist!")

        def close(self) -> None:
            pass

    @contextlib.contextmanager
    def fake_conn() -> Generator[Any, None, None]:
        yield AlwaysMiss()

    monkeypatch.setattr(pool, "connection", fake_conn)
    with pytest.raises(RuntimeError, match="does not exist"):
        pool.execute_with_retry("SELECT 1")


def test_unpinned_pool_behaves_as_before(monkeypatch: pytest.MonkeyPatch) -> None:
    p = DuckLakePool(pool_size=1)  # pin_snapshot defaults to False
    assert p.force_pin_refresh() is False
    con = FakeCon(None)
    p._pool.put((con, time.time(), 0))  # type: ignore[arg-type]
    p._initialized = True
    with p.connection() as c:
        assert c is con  # type: ignore[comparison-overlap]
    entries = pool_entries(p)
    assert len(entries) == 1 and entries[0][0] is con


def test_recycle_aged_rebuilds_old_connections(pool: DuckLakePool) -> None:
    pool.MAX_CONNECTION_AGE_SECONDS = 0  # everything is "old"
    pool._pin = __import__(
        "goatlib.storage.snapshot_pin", fromlist=["SnapshotPin"]
    ).SnapshotPin(pool._fetch_latest_snapshot_id, pool._apply_snapshot)
    pool._pin._current = 10  # type: ignore[union-attr]
    old = [e[0] for e in pool_entries(pool)]
    pool._recycle_aged()
    entries = pool_entries(pool)
    assert len(entries) == 2
    assert all(not e[0].closed for e in entries)
    assert all(c.closed for c in old)
    assert all(e[0].snapshot == 10 for e in entries)  # same pin, fresh conns


def test_apply_snapshot_warm_failure_closes_new_and_keeps_pool(
    pool: DuckLakePool, monkeypatch: pytest.MonkeyPatch
) -> None:
    def bad_warm(con: Any) -> None:
        raise RuntimeError("warm failed")

    monkeypatch.setattr(pool, "_warm_connection", bad_warm)
    before = [e[0] for e in pool_entries(pool)]
    created: list[FakeCon] = pool._test_created  # type: ignore[attr-defined]
    n_before = len(created)

    with pytest.raises(RuntimeError, match="warm failed"):
        pool._apply_snapshot(11)

    new_cons = created[n_before:]
    assert len(new_cons) == 1
    assert new_cons[0].closed  # freshly created conn was not leaked
    entries = pool_entries(pool)
    assert len(entries) == 2
    assert [e[0] for e in entries] == before  # previous healthy conns intact
    assert all(not e[0].closed for e in entries)


def test_recycle_aged_warm_failure_keeps_old_connection(
    pool: DuckLakePool, monkeypatch: pytest.MonkeyPatch
) -> None:
    from goatlib.storage.snapshot_pin import SnapshotPin

    pool.MAX_CONNECTION_AGE_SECONDS = 0  # everything is "old"
    pool._pin = SnapshotPin(pool._fetch_latest_snapshot_id, pool._apply_snapshot)
    pool._pin._current = 10

    def bad_warm(con: Any) -> None:
        raise RuntimeError("warm failed")

    monkeypatch.setattr(pool, "_warm_connection", bad_warm)
    before = [e[0] for e in pool_entries(pool)]
    created: list[FakeCon] = pool._test_created  # type: ignore[attr-defined]
    n_before = len(created)

    with pytest.raises(RuntimeError, match="warm failed"):
        pool._recycle_aged()

    new_cons = created[n_before:]
    assert len(new_cons) == 1
    assert new_cons[0].closed  # freshly created conn was not leaked
    entries = pool_entries(pool)
    assert len(entries) == 2  # pool did not shrink
    assert set(e[0] for e in entries) == set(before)  # old conns kept serving
    assert all(not e[0].closed for e in entries)


def test_unpinned_init_does_not_warm(monkeypatch: pytest.MonkeyPatch) -> None:
    p = DuckLakePool(pool_size=2)  # pin_snapshot defaults to False
    warm_calls = {"n": 0}

    def fake_create(
        max_retries: int = 3,
        retry_delay: float = 1.0,
        snapshot_version: int | None = None,
    ) -> FakeCon:
        return FakeCon(snapshot_version)

    def spy_warm(con: Any) -> None:
        warm_calls["n"] += 1

    monkeypatch.setattr(p, "_create_connection_with_retry", fake_create)
    monkeypatch.setattr(p, "_warm_connection", spy_warm)
    monkeypatch.setattr(
        p, "_fetch_latest_snapshot_id", lambda: pytest.fail("unpinned must not poll")
    )

    class Settings:
        POSTGRES_DATABASE_URI = "postgresql://u:p@localhost/db"
        DUCKLAKE_CATALOG_SCHEMA = "ducklake"

    p.init(Settings())  # type: ignore[arg-type]
    assert warm_calls["n"] == 0  # unpinned pools never pay the warm-up query
    entries = pool_entries(p)
    assert len(entries) == 2
    assert all(e[0].snapshot is None for e in entries)  # no SNAPSHOT_VERSION


def test_apply_snapshot_build_failure_leaves_pool_untouched(
    pool: DuckLakePool, monkeypatch: pytest.MonkeyPatch
) -> None:
    created: list[FakeCon] = pool._test_created  # type: ignore[attr-defined]
    calls = {"n": 0}

    def flaky_create(
        max_retries: int = 3,
        retry_delay: float = 1.0,
        snapshot_version: int | None = None,
    ) -> FakeCon:
        calls["n"] += 1
        if calls["n"] == 2:  # 2nd of 2 builds fails
            raise RuntimeError("create failed")
        con = FakeCon(snapshot_version)
        created.append(con)
        return con

    monkeypatch.setattr(pool, "_create_connection_with_retry", flaky_create)
    before = [e[0] for e in pool_entries(pool)]
    gen_before = pool._generation
    n_before = len(created)

    with pytest.raises(RuntimeError, match="create failed"):
        pool._apply_snapshot(11)

    assert pool._generation == gen_before  # generation NOT bumped
    entries = pool_entries(pool)
    assert [e[0] for e in entries] == before  # same entries, untouched
    assert all(not e[0].closed for e in entries)  # old conns NOT closed
    new_cons = created[n_before:]
    assert len(new_cons) == 1  # only the 1st build succeeded
    assert new_cons[0].closed  # and it was cleaned up, not leaked


def test_error_recreate_racing_rebuild_discards_replacement(
    pool: DuckLakePool, monkeypatch: pytest.MonkeyPatch
) -> None:
    created: list[FakeCon] = pool._test_created  # type: ignore[attr-defined]
    raced = {"done": False}

    def racing_create(
        max_retries: int = 3,
        retry_delay: float = 1.0,
        snapshot_version: int | None = None,
    ) -> FakeCon:
        if not raced["done"]:
            # Simulate a rebuild landing while this replacement is created:
            # generation bumps and the queue is swapped to pool_size new-gen
            # entries (including a replacement for the checked-out slot).
            raced["done"] = True
            pool._generation += 1
            while True:
                try:
                    old, _, _ = pool._pool.get_nowait()
                except queue.Empty:
                    break
                old.close()
            for _ in range(2):
                nc = FakeCon(11)
                created.append(nc)
                pool._pool.put((nc, time.time(), pool._generation))  # type: ignore[arg-type]
        con = FakeCon(snapshot_version)
        created.append(con)
        return con

    monkeypatch.setattr(pool, "_create_connection_with_retry", racing_create)

    with pytest.raises(RuntimeError, match="connection reset"):
        with pool.connection():
            raise RuntimeError("connection reset")  # triggers recreate path

    replacement = created[-1]
    assert replacement.closed  # discarded: pooling it would exceed pool_size
    entries = pool_entries(pool)
    assert len(entries) == 2  # exactly pool_size entries
    assert all(e[2] == pool._generation for e in entries)
    assert all(not e[0].closed for e in entries)


def test_fetch_latest_snapshot_id_installs_postgres_before_load(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A fresh poll connection must INSTALL postgres before LOAD postgres —
    on a clean container this is the first DuckDB call of the process and
    must not rely on autoinstall-on-LOAD."""
    p = DuckLakePool(pool_size=1, pin_snapshot=True)
    p._catalog_schema = "ducklake"
    p._postgres_uri = "postgresql://u:p@localhost/db"

    executed: list[str] = []

    class FakePollConn:
        def execute(self, sql: str, *_: Any) -> "FakePollConn":
            executed.append(sql)
            return self

        def fetchone(self) -> tuple[int]:
            return (5,)

        def close(self) -> None:
            pass

    import goatlib.storage.ducklake as ducklake_module

    monkeypatch.setattr(ducklake_module.duckdb, "connect", lambda: FakePollConn())

    assert p._fetch_latest_snapshot_id() == 5
    assert executed[0] == "INSTALL postgres"
    assert executed[1] == "LOAD postgres"


def test_fetch_latest_snapshot_id_holds_poll_lock() -> None:
    p = DuckLakePool(pool_size=1, pin_snapshot=True)
    p._catalog_schema = "ducklake"
    observed: dict[str, bool] = {}

    class FakePollCon:
        def execute(self, *_: Any) -> "FakePollCon":
            observed["locked"] = p._poll_lock.locked()
            return self

        def fetchone(self) -> tuple[int]:
            return (7,)

        def close(self) -> None:
            pass

    p._poll_con = FakePollCon()  # type: ignore[assignment]
    assert p._fetch_latest_snapshot_id() == 7
    assert observed["locked"] is True  # lock held for the whole fetch body
    assert not p._poll_lock.locked()  # and released afterwards
