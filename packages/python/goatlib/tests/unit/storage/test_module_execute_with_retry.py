"""Module-level `execute_with_retry(manager, query, ...)` pin-miss handling.

This standalone function (distinct from the `BaseDuckLakeManager` and
`DuckLakePool` methods of the same name, which already force-refresh on a
pin miss) is what geoapi's feature_service.py uses. Before this fix it only
retried connection errors, so a brand-new layer's items/count requests
would 500 until the next background poll tick instead of miss-refreshing
immediately.
"""

import contextlib
from typing import Any, Generator

import pytest
from goatlib.storage.ducklake import execute_with_retry


class MissThenOkConnection:
    """First execute() raises a pin-miss error, second succeeds."""

    def __init__(self, calls: dict[str, int]) -> None:
        self._calls = calls

    def execute(self, query: str, params: Any = None) -> "MissThenOkConnection":
        self._calls["n"] += 1
        if self._calls["n"] == 1:
            raise RuntimeError("Catalog Error: Table with name t_x does not exist!")
        return self

    def fetchall(self) -> list[tuple[int]]:
        return [(42,)]

    def fetchone(self) -> tuple[int]:
        return (42,)

    @property
    def description(self) -> list[tuple[str]]:
        return [("col",)]


class AlwaysMissConnection:
    """Every execute() raises a pin-miss error (genuine 404, not stale pin)."""

    def execute(self, query: str, params: Any = None) -> "AlwaysMissConnection":
        raise RuntimeError("Catalog Error: Table with name t_gone does not exist!")


class FakeManager:
    def __init__(self, con: Any, force_refresh_result: bool = True) -> None:
        self._con = con
        self.force_refresh_calls = 0
        self.reconnect_calls = 0
        self._force_refresh_result = force_refresh_result

    @contextlib.contextmanager
    def connection(self) -> Generator[Any, None, None]:
        yield self._con

    def force_pin_refresh(self) -> bool:
        self.force_refresh_calls += 1
        return self._force_refresh_result

    def reconnect(self) -> None:
        self.reconnect_calls += 1


def test_miss_then_ok_refreshes_pin_and_retries_once() -> None:
    calls = {"n": 0}
    manager = FakeManager(MissThenOkConnection(calls))

    result, description = execute_with_retry(manager, "SELECT 1")

    assert result == [(42,)]
    assert description == [("col",)]
    assert calls["n"] == 2
    assert manager.force_refresh_calls == 1
    assert manager.reconnect_calls == 0  # pin-miss path, not connection-error path


def test_genuine_missing_table_still_propagates() -> None:
    """force_pin_refresh() reports the pin is already at latest (True), but
    the query fails again: it's a real 404, not a stale pin, so the error
    must propagate rather than retry forever."""
    manager = FakeManager(AlwaysMissConnection(), force_refresh_result=True)

    with pytest.raises(RuntimeError, match="does not exist"):
        execute_with_retry(manager, "SELECT 1")

    assert manager.force_refresh_calls == 1  # only tried once (max_retries=1)


def test_unpinned_manager_behavior_unchanged() -> None:
    """force_pin_refresh() returning False (unpinned manager) means the
    pin-miss branch never fires, so unpinned callers see the same
    behavior as before this fix: the error just propagates."""
    manager = FakeManager(AlwaysMissConnection(), force_refresh_result=False)

    with pytest.raises(RuntimeError, match="does not exist"):
        execute_with_retry(manager, "SELECT 1")

    assert manager.force_refresh_calls == 1
    assert manager.reconnect_calls == 0
