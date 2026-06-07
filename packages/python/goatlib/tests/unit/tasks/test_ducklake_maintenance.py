"""Unit tests for the DuckLake maintenance task.

The DuckLake connection is mocked — these tests verify only that the right
sequence of SQL commands is issued with the right parameters, and that the
return shape is populated correctly from before/after measurements.

Integration coverage (real catalog mutations) lives separately; this file
intentionally stays fast and offline.
"""

from contextlib import contextmanager
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from goatlib.tasks.ducklake_maintenance import (
    DuckLakeMaintenanceParams,
    DuckLakeMaintenanceTask,
    main,
)

# ────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────


class _FakeConn:
    """Mocks a DuckDB connection.

    Records all SQL executed, and returns canned results for the
    measurement / dry-run queries the task issues.
    """

    def __init__(self, before_snaps: int, after_snaps: int,
                 before_bytes: int, after_bytes: int,
                 would_expire: int, orphan_paths: list[str],
                 fs_orphan_paths: list[str] | None = None,
                 tracked_file_count: int = 100) -> None:
        self.calls: list[str] = []
        self._before_snaps = before_snaps
        self._after_snaps = after_snaps
        self._before_bytes = before_bytes
        self._after_bytes = after_bytes
        self._would_expire = would_expire
        self._orphan_paths = orphan_paths
        self._fs_orphan_paths = fs_orphan_paths or []
        self._tracked_file_count = tracked_file_count
        self._snap_calls = 0
        self._bytes_calls = 0
        self._cleanup_calls = 0

    def execute(self, sql: str) -> "_FakeConn":
        self.calls.append(sql.strip())
        return self

    def fetchone(self) -> tuple[Any, ...]:
        last = self.calls[-1]
        if "count(*) FROM ducklake_snapshots('lake')" in last and "INTERVAL" in last:
            return (self._would_expire,)
        if "count(*) FROM ducklake_snapshots('lake')" in last:
            # First measure → before; second measure → after.
            self._snap_calls += 1
            return (self._before_snaps if self._snap_calls == 1 else self._after_snaps,)
        if (
            "count(*) FROM ducklake_table_info('lake')" in last
            and "sum(" not in last
        ):
            # Sanity-guard query: total tracked files.
            return (self._tracked_file_count,)
        if "ducklake_table_info('lake')" in last:
            self._bytes_calls += 1
            return (self._before_bytes if self._bytes_calls == 1 else self._after_bytes,)
        return (0,)

    def fetchall(self) -> list[tuple[str]]:
        last = self.calls[-1]
        if "ducklake_cleanup_old_files" in last:
            self._cleanup_calls += 1
            return [(p,) for p in self._orphan_paths]
        if "ducklake_delete_orphaned_files" in last:
            return [(p,) for p in self._fs_orphan_paths]
        return []


@contextmanager
def _fake_connection_cm(con: _FakeConn) -> Any:
    yield con


def _make_task(con: _FakeConn) -> DuckLakeMaintenanceTask:
    """Build a task with mocked manager + settings (no env / network)."""
    task = DuckLakeMaintenanceTask()
    task.settings = MagicMock()
    fake_manager = MagicMock()
    fake_manager.connection.return_value = _fake_connection_cm(con)
    task._manager = fake_manager
    return task


def _has(calls: list[str], needle: str) -> bool:
    return any(needle in c for c in calls)


# ────────────────────────────────────────────────────────────────────────
# Params validation
# ────────────────────────────────────────────────────────────────────────


class TestParams:
    def test_defaults(self) -> None:
        p = DuckLakeMaintenanceParams()
        assert p.retention_days == 1
        assert p.cleanup_files is True
        assert p.dry_run is False

    def test_retention_days_must_be_non_negative(self) -> None:
        with pytest.raises(ValueError):
            DuckLakeMaintenanceParams(retention_days=-1)

    def test_retention_days_zero_allowed(self) -> None:
        # 0 means "expire everything but the current snapshot"; valid use case
        # for aggressive reclamation on dev / sandbox.
        p = DuckLakeMaintenanceParams(retention_days=0)
        assert p.retention_days == 0


# ────────────────────────────────────────────────────────────────────────
# Dry-run path
# ────────────────────────────────────────────────────────────────────────


class TestDryRun:
    def test_does_not_mutate_catalog(self) -> None:
        con = _FakeConn(before_snaps=100, after_snaps=100,
                        before_bytes=1_000_000_000, after_bytes=1_000_000_000,
                        would_expire=42, orphan_paths=["/x/a.parquet", "/x/b.parquet"])
        task = _make_task(con)
        out = task.run(DuckLakeMaintenanceParams(retention_days=7, dry_run=True))

        # No destructive call ran
        assert not _has(con.calls, "CALL ducklake_expire_snapshots")
        # The dry_run variant of cleanup_old_files IS allowed (it's read-only)
        assert _has(con.calls, "dry_run => true")

        assert out["dry_run"] is True
        assert out["would_expire_snapshots"] == 42
        assert out["would_delete_files"] == 2

    def test_retention_days_threaded_into_preview_sql(self) -> None:
        con = _FakeConn(before_snaps=10, after_snaps=10,
                        before_bytes=0, after_bytes=0,
                        would_expire=3, orphan_paths=[])
        task = _make_task(con)
        task.run(DuckLakeMaintenanceParams(retention_days=14, dry_run=True))
        assert _has(con.calls, "INTERVAL '14 days'")

    def test_cleanup_files_false_skips_orphan_preview(self) -> None:
        con = _FakeConn(before_snaps=10, after_snaps=10,
                        before_bytes=0, after_bytes=0,
                        would_expire=3, orphan_paths=["/x/a.parquet"])
        task = _make_task(con)
        out = task.run(DuckLakeMaintenanceParams(
            retention_days=7, dry_run=True, cleanup_files=False
        ))
        assert not _has(con.calls, "ducklake_cleanup_old_files")
        assert out["would_delete_files"] == 0


# ────────────────────────────────────────────────────────────────────────
# Real-run path
# ────────────────────────────────────────────────────────────────────────


class TestRealRun:
    def test_issues_expire_then_cleanup(self) -> None:
        con = _FakeConn(before_snaps=200, after_snaps=50,
                        before_bytes=10_000_000_000, after_bytes=4_000_000_000,
                        would_expire=0, orphan_paths=["/p1", "/p2", "/p3"])
        task = _make_task(con)
        out = task.run(DuckLakeMaintenanceParams(retention_days=7))

        # Order matters: expire must happen before cleanup.
        expire_idx = next(i for i, c in enumerate(con.calls)
                          if "CALL ducklake_expire_snapshots" in c)
        cleanup_idx = next(i for i, c in enumerate(con.calls)
                           if "CALL ducklake_cleanup_old_files" in c
                           and "dry_run" not in c)
        assert expire_idx < cleanup_idx

        # Output reflects measurements
        assert out["dry_run"] is False
        assert out["retention_days"] == 7
        assert out["snapshots_expired"] == 150
        assert out["files_deleted"] == 3
        assert out["bytes_freed"] == 6_000_000_000

    def test_retention_days_threaded_into_expire_sql(self) -> None:
        con = _FakeConn(before_snaps=10, after_snaps=5,
                        before_bytes=0, after_bytes=0,
                        would_expire=0, orphan_paths=[])
        task = _make_task(con)
        task.run(DuckLakeMaintenanceParams(retention_days=30))
        assert _has(con.calls,
                    "ducklake_expire_snapshots('lake', older_than => NOW() "
                    "- INTERVAL '30 days')")

    def test_cleanup_files_false_only_expires(self) -> None:
        con = _FakeConn(before_snaps=20, after_snaps=10,
                        before_bytes=5_000_000, after_bytes=5_000_000,
                        would_expire=0, orphan_paths=["/x"])
        task = _make_task(con)
        out = task.run(DuckLakeMaintenanceParams(
            retention_days=7, cleanup_files=False
        ))

        assert _has(con.calls, "CALL ducklake_expire_snapshots")
        # No cleanup call (real or dry-run) executed
        assert not _has(con.calls, "ducklake_cleanup_old_files")
        assert out["files_deleted"] == 0
        assert out["snapshots_expired"] == 10

    def test_zero_retention_means_now(self) -> None:
        con = _FakeConn(before_snaps=10, after_snaps=1,
                        before_bytes=0, after_bytes=0,
                        would_expire=0, orphan_paths=[])
        task = _make_task(con)
        task.run(DuckLakeMaintenanceParams(retention_days=0))
        assert _has(con.calls, "INTERVAL '0 days'")


# ────────────────────────────────────────────────────────────────────────
# delete_orphans path (filesystem-orphan reclamation)
# ────────────────────────────────────────────────────────────────────────


class TestDeleteOrphans:
    def test_real_run_issues_delete_orphaned_files(self) -> None:
        con = _FakeConn(before_snaps=10, after_snaps=10,
                        before_bytes=0, after_bytes=0,
                        would_expire=0, orphan_paths=[],
                        fs_orphan_paths=["/a/leaked.parquet", "/b/leaked.parquet"])
        task = _make_task(con)
        out = task.run(DuckLakeMaintenanceParams(
            retention_days=7, orphan_age_days=1
        ))
        assert _has(con.calls,
                    "ducklake_delete_orphaned_files('lake', "
                    "older_than => NOW() - INTERVAL '1 days')")
        # Must NOT use cleanup_all=true on delete_orphaned_files — older_than
        # is the safety guard against deleting in-progress writes.
        assert not _has(
            con.calls,
            "ducklake_delete_orphaned_files('lake', cleanup_all => true",
        )
        assert out["orphans_deleted"] == 2

    def test_delete_orphans_false_skips_filesystem_scan(self) -> None:
        con = _FakeConn(before_snaps=10, after_snaps=10,
                        before_bytes=0, after_bytes=0,
                        would_expire=0, orphan_paths=[],
                        fs_orphan_paths=["/a/leaked.parquet"])
        task = _make_task(con)
        out = task.run(DuckLakeMaintenanceParams(
            retention_days=7, delete_orphans=False
        ))
        assert not _has(con.calls, "ducklake_delete_orphaned_files")
        assert out["orphans_deleted"] == 0

    def test_orphan_age_days_threaded_into_sql(self) -> None:
        con = _FakeConn(before_snaps=10, after_snaps=10,
                        before_bytes=0, after_bytes=0,
                        would_expire=0, orphan_paths=[],
                        fs_orphan_paths=[])
        task = _make_task(con)
        task.run(DuckLakeMaintenanceParams(
            retention_days=7, orphan_age_days=14
        ))
        assert _has(con.calls, "INTERVAL '14 days'")

    def test_dry_run_previews_filesystem_orphans(self) -> None:
        con = _FakeConn(before_snaps=10, after_snaps=10,
                        before_bytes=0, after_bytes=0,
                        would_expire=0, orphan_paths=[],
                        fs_orphan_paths=["/a", "/b", "/c"])
        task = _make_task(con)
        out = task.run(DuckLakeMaintenanceParams(retention_days=7, dry_run=True))

        # No destructive call (with no dry_run flag) issued
        non_dry_calls = [c for c in con.calls
                         if "ducklake_delete_orphaned_files" in c
                         and "dry_run => true" not in c]
        assert non_dry_calls == []

        assert _has(con.calls,
                    "ducklake_delete_orphaned_files('lake', "
                    "older_than => NOW() - INTERVAL '1 days', "
                    "dry_run => true)")
        assert out["would_delete_orphans"] == 3

    def test_dry_run_orphans_false_skips_orphan_preview(self) -> None:
        con = _FakeConn(before_snaps=10, after_snaps=10,
                        before_bytes=0, after_bytes=0,
                        would_expire=0, orphan_paths=[],
                        fs_orphan_paths=["/a"])
        task = _make_task(con)
        out = task.run(DuckLakeMaintenanceParams(
            retention_days=7, dry_run=True, delete_orphans=False
        ))
        assert not _has(con.calls, "ducklake_delete_orphaned_files")
        assert out["would_delete_orphans"] == 0


# ────────────────────────────────────────────────────────────────────────
# Sanity guard against catastrophic delete_orphaned_files
# ────────────────────────────────────────────────────────────────────────


class TestOrphanSanityGuard:
    def test_below_threshold_proceeds(self) -> None:
        # 5 orphans out of 100 tracked = 5% → below default 10% threshold
        con = _FakeConn(before_snaps=10, after_snaps=10,
                        before_bytes=0, after_bytes=0,
                        would_expire=0, orphan_paths=[],
                        fs_orphan_paths=["/a", "/b", "/c", "/d", "/e"],
                        tracked_file_count=100)
        task = _make_task(con)
        out = task.run(DuckLakeMaintenanceParams(retention_days=7))
        # Both the dry-run preview AND the real delete should have run
        orphan_calls = [c for c in con.calls
                        if "ducklake_delete_orphaned_files" in c]
        assert len(orphan_calls) == 2
        assert orphan_calls[0].endswith("dry_run => true)")
        assert "dry_run" not in orphan_calls[1]
        assert out["orphans_deleted"] == 5

    def test_above_threshold_aborts(self) -> None:
        # 60 orphans out of 100 tracked = 60% → way above default 10%
        con = _FakeConn(before_snaps=10, after_snaps=10,
                        before_bytes=0, after_bytes=0,
                        would_expire=0, orphan_paths=[],
                        fs_orphan_paths=["/p" + str(i) for i in range(60)],
                        tracked_file_count=100)
        task = _make_task(con)
        with pytest.raises(RuntimeError, match="ABORT delete_orphaned_files"):
            task.run(DuckLakeMaintenanceParams(retention_days=7))
        # Preview ran; the destructive call did NOT.
        orphan_calls = [c for c in con.calls
                        if "ducklake_delete_orphaned_files" in c]
        assert len(orphan_calls) == 1
        assert "dry_run => true" in orphan_calls[0]

    def test_threshold_can_be_relaxed_to_100(self) -> None:
        # Disabling the guard: orphan_abort_pct=100 accepts any ratio.
        con = _FakeConn(before_snaps=10, after_snaps=10,
                        before_bytes=0, after_bytes=0,
                        would_expire=0, orphan_paths=[],
                        fs_orphan_paths=["/p" + str(i) for i in range(99)],
                        tracked_file_count=100)
        task = _make_task(con)
        out = task.run(DuckLakeMaintenanceParams(
            retention_days=7, orphan_abort_pct=100.0
        ))
        assert out["orphans_deleted"] == 99

    def test_empty_catalog_doesnt_divide_by_zero(self) -> None:
        # tracked_file_count=0 → percentage calc must not crash.
        con = _FakeConn(before_snaps=10, after_snaps=10,
                        before_bytes=0, after_bytes=0,
                        would_expire=0, orphan_paths=[],
                        fs_orphan_paths=[],
                        tracked_file_count=0)
        task = _make_task(con)
        out = task.run(DuckLakeMaintenanceParams(retention_days=7))
        assert out["orphans_deleted"] == 0


# ────────────────────────────────────────────────────────────────────────
# main() wrapper
# ────────────────────────────────────────────────────────────────────────


class TestMain:
    def test_main_inits_and_closes_task(self) -> None:
        """main() must call init_from_env, run, and close (even on success)."""
        with patch(
            "goatlib.tasks.ducklake_maintenance.DuckLakeMaintenanceTask"
        ) as mock_cls:
            instance = mock_cls.return_value
            instance.run.return_value = {"dry_run": True}

            result = main(DuckLakeMaintenanceParams(dry_run=True))

            instance.init_from_env.assert_called_once()
            instance.run.assert_called_once()
            instance.close.assert_called_once()
            assert result == {"dry_run": True}

    def test_main_closes_task_even_if_run_raises(self) -> None:
        with patch(
            "goatlib.tasks.ducklake_maintenance.DuckLakeMaintenanceTask"
        ) as mock_cls:
            instance = mock_cls.return_value
            instance.run.side_effect = RuntimeError("boom")

            with pytest.raises(RuntimeError):
                main(DuckLakeMaintenanceParams())

            instance.close.assert_called_once()
