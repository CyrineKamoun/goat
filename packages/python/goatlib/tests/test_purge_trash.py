"""Unit tests for the trash-purge task.

The core Postgres connection is mocked (`asyncpg.connect`), and per-layer
artifact deletion (DuckLake table + PMTiles) is mocked at
`LayerDeleteMultiRunner._delete_layer_artifacts` — the same runner method
`LayerDeleteMultiRunner.run` uses for the interactive multi-delete flow.
These tests verify only the SQL issued and the resulting counts; no live
Postgres or DuckLake is needed.
"""

from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest
from goatlib.tasks.purge_trash import PurgeTrashParams, PurgeTrashTask
from goatlib.tools.base import ToolSettings
from goatlib.tools.layer_delete_multi import RECYCLE_DUCKDB_EVERY

OLD = datetime.now(timezone.utc) - timedelta(days=31)


def _settings() -> ToolSettings:
    return ToolSettings(
        postgres_server="db",
        postgres_port=5432,
        postgres_user="postgres",
        postgres_password="postgres",
        postgres_db="goat",
        ducklake_postgres_uri="postgresql://postgres:postgres@db:5432/goat",
        ducklake_catalog_schema="ducklake",
        ducklake_data_dir="/app/data/ducklake",
    )


def _conn(rows_by_query: dict[str, list[dict[str, Any]]]) -> MagicMock:
    """A fake asyncpg connection: fetch() dispatches on a substring of the SQL."""
    conn = MagicMock()

    async def fetch(sql: str, *args: Any) -> list[dict[str, Any]]:
        for key, rows in rows_by_query.items():
            if key in sql:
                return rows
        return []

    conn.fetch = AsyncMock(side_effect=fetch)
    conn.execute = AsyncMock(return_value="DELETE 1")
    conn.close = AsyncMock()
    return conn


def _executed(conn: MagicMock) -> str:
    return " ".join(str(c.args[0]) for c in conn.execute.await_args_list)


def _make_task() -> PurgeTrashTask:
    task = PurgeTrashTask()
    task.settings = _settings()
    return task


# ---------------------------------------------------------------------------
# Params validation
# ---------------------------------------------------------------------------


class TestParams:
    def test_defaults(self) -> None:
        p = PurgeTrashParams()
        assert p.retention_days == 30
        assert p.batch_size == 500
        assert p.dry_run is False

    def test_retention_days_must_be_positive(self) -> None:
        with pytest.raises(ValueError):
            PurgeTrashParams(retention_days=0)

    def test_batch_size_bounds(self) -> None:
        with pytest.raises(ValueError):
            PurgeTrashParams(batch_size=0)
        with pytest.raises(ValueError):
            PurgeTrashParams(batch_size=5001)


# ---------------------------------------------------------------------------
# Layers: DuckLake first, then row + grant
# ---------------------------------------------------------------------------


_ARTIFACTS_TARGET = (
    "goatlib.tools.layer_delete_multi.LayerDeleteMultiRunner._delete_layer_artifacts"
)
_DUCKLAKE_TARGET = (
    "goatlib.tools.layer_delete_multi.LayerDeleteMultiRunner._delete_ducklake_table"
)
_PMTILES_TARGET = (
    "goatlib.tools.layer_delete_multi.LayerDeleteMultiRunner._delete_pmtiles"
)


class TestLayerPurge:
    async def test_purges_owned_layer_and_drops_artifacts_first(self) -> None:
        owner = uuid4()
        old_id = uuid4()
        conn = _conn(
            {
                "FROM customer.layer": [
                    {"id": old_id, "type": "feature", "user_id": owner}
                ]
            }
        )
        task = _make_task()
        with (
            patch(
                "goatlib.tasks.purge_trash.asyncpg.connect",
                AsyncMock(return_value=conn),
            ),
            patch(_ARTIFACTS_TARGET, return_value=(True, True)) as dl,
        ):
            out = await task._run(PurgeTrashParams(retention_days=30))

        dl.assert_called_once_with(str(old_id), str(owner))
        assert out.purged["layer"] == 1
        assert out.ducklake_deleted == 1
        assert out.skipped == 0
        executed = _executed(conn)
        assert "DELETE FROM customer.resource_grant" in executed
        assert "resource_type = 'layer'" in executed
        assert "DELETE FROM customer.layer" in executed
        assert "AND deleted_at IS NOT NULL" in executed

    async def test_artifact_delete_raising_keeps_the_row_for_the_next_run(
        self,
    ) -> None:
        owner = uuid4()
        lid = uuid4()
        conn = _conn(
            {"FROM customer.layer": [{"id": lid, "type": "feature", "user_id": owner}]}
        )
        task = _make_task()
        with (
            patch(
                "goatlib.tasks.purge_trash.asyncpg.connect",
                AsyncMock(return_value=conn),
            ),
            patch(_ARTIFACTS_TARGET, side_effect=RuntimeError("boom")),
        ):
            out = await task._run(PurgeTrashParams(retention_days=30))

        assert out.skipped == 1
        assert out.purged["layer"] == 0
        assert "DELETE FROM customer.layer" not in _executed(conn)

    async def test_pmtiles_failure_keeps_the_row_for_the_next_run(self) -> None:
        # DuckLake table dropped fine (True) but PMTiles delete reported
        # failure (False) — the runner can't tell "no PMTiles" from "PMTiles
        # delete errored", so this must still be treated as a failure: the
        # row stays for a retry rather than risk leaving tiles on disk.
        owner = uuid4()
        lid = uuid4()
        conn = _conn(
            {"FROM customer.layer": [{"id": lid, "type": "feature", "user_id": owner}]}
        )
        task = _make_task()
        with (
            patch(
                "goatlib.tasks.purge_trash.asyncpg.connect",
                AsyncMock(return_value=conn),
            ),
            patch(_ARTIFACTS_TARGET, return_value=(True, False)),
        ):
            out = await task._run(PurgeTrashParams(retention_days=30))

        assert out.skipped == 1
        assert out.purged["layer"] == 0
        assert "DELETE FROM customer.layer" not in _executed(conn)
        # The DuckLake table drop itself did succeed, so it's still counted.
        assert out.ducklake_deleted == 1

    async def test_layers_without_owner_still_get_artifact_cleanup(self) -> None:
        # Flat storage resolves a layer's DuckLake table by layer id alone
        # (resolve_layer_table_path) — a missing user_id (orphan, or
        # promote-on-use catalog placeholder metadata) is no reason to skip
        # per-layer artifact deletion; the owner_id param only feeds a
        # legacy owner-scoped fallback the runner may still use internally.
        lid = uuid4()
        conn = _conn(
            {"FROM customer.layer": [{"id": lid, "type": "table", "user_id": None}]}
        )
        task = _make_task()
        with (
            patch(
                "goatlib.tasks.purge_trash.asyncpg.connect",
                AsyncMock(return_value=conn),
            ),
            patch(_ARTIFACTS_TARGET, return_value=(True, True)) as dl,
        ):
            out = await task._run(PurgeTrashParams(retention_days=30))

        dl.assert_called_once_with(str(lid), "")
        assert out.purged["layer"] == 1
        assert "DELETE FROM customer.layer" in _executed(conn)

    async def test_non_data_layer_type_skips_artifact_call(self) -> None:
        owner = uuid4()
        lid = uuid4()
        conn = _conn(
            {"FROM customer.layer": [{"id": lid, "type": "raster", "user_id": owner}]}
        )
        task = _make_task()
        with (
            patch(
                "goatlib.tasks.purge_trash.asyncpg.connect",
                AsyncMock(return_value=conn),
            ),
            patch(_ARTIFACTS_TARGET) as dl,
        ):
            out = await task._run(PurgeTrashParams(retention_days=30))

        dl.assert_not_called()
        assert out.purged["layer"] == 1


class TestDuckDBRecycling:
    async def test_recycles_duckdb_connection_at_the_runners_cadence(self) -> None:
        # Mirrors LayerDeleteMultiRunner.run's own recycling: each DROP TABLE
        # is a DuckLake commit whose memory cost accumulates on the
        # connection, so a purge batch must recycle at the same cadence —
        # reusing RECYCLE_DUCKDB_EVERY rather than a second hardcoded number.
        owner = uuid4()
        layer_ids = [uuid4() for _ in range(12)]
        conn = _conn(
            {
                "FROM customer.layer": [
                    {"id": lid, "type": "feature", "user_id": owner}
                    for lid in layer_ids
                ]
            }
        )
        task = _make_task()
        with (
            patch(
                "goatlib.tasks.purge_trash.asyncpg.connect",
                AsyncMock(return_value=conn),
            ),
            patch(_ARTIFACTS_TARGET, return_value=(True, True)),
            patch(
                "goatlib.tools.layer_delete_multi.LayerDeleteMultiRunner"
                ".recycle_duckdb_connection"
            ) as recycle,
        ):
            out = await task._run(PurgeTrashParams())

        assert out.purged["layer"] == 12
        assert recycle.call_count == len(layer_ids) // RECYCLE_DUCKDB_EVERY


class TestAbsenceIsSuccess:
    """Absence-is-success: nothing to delete purges; only a real error skips.

    These patch the two leaf methods (`_delete_ducklake_table`,
    `_delete_pmtiles`) rather than the `_delete_layer_artifacts` combinator,
    so the real combinator code (added for the PMTiles fix) is exercised
    end to end through `PurgeTrashTask`.
    """

    async def test_table_layer_with_no_ducklake_table_purges_fully_in_one_run(
        self,
    ) -> None:
        # A `table`-type layer whose DuckLake table was never created (or
        # already dropped by an earlier attempt) has nothing to drop — that
        # must not block the row from being purged.
        owner = uuid4()
        lid = uuid4()
        conn = _conn(
            {"FROM customer.layer": [{"id": lid, "type": "table", "user_id": owner}]}
        )
        task = _make_task()
        with (
            patch(
                "goatlib.tasks.purge_trash.asyncpg.connect",
                AsyncMock(return_value=conn),
            ),
            patch(_DUCKLAKE_TARGET, return_value=True),  # "absent" -> True
            patch(_PMTILES_TARGET, return_value=True),  # never had tiles -> True
        ):
            out = await task._run(PurgeTrashParams())

        assert out.skipped == 0
        assert out.purged["layer"] == 1
        assert out.ducklake_deleted == 1
        assert "DELETE FROM customer.layer" in _executed(conn)

    async def test_feature_layer_with_no_tiles_on_disk_purges(self) -> None:
        # A `feature`-type layer whose PMTiles were never built (or already
        # removed) — its DuckLake table is genuinely dropped (True), and the
        # PMTiles step reports "already gone" (also True) rather than False.
        owner = uuid4()
        lid = uuid4()
        conn = _conn(
            {"FROM customer.layer": [{"id": lid, "type": "feature", "user_id": owner}]}
        )
        task = _make_task()
        with (
            patch(
                "goatlib.tasks.purge_trash.asyncpg.connect",
                AsyncMock(return_value=conn),
            ),
            patch(_DUCKLAKE_TARGET, return_value=True),
            patch(_PMTILES_TARGET, return_value=True),  # no tiles on disk -> True
        ):
            out = await task._run(PurgeTrashParams())

        assert out.skipped == 0
        assert out.purged["layer"] == 1
        assert "DELETE FROM customer.layer" in _executed(conn)

    async def test_pmtiles_remove_raising_is_skipped(self) -> None:
        # A real error removing an existing PMTiles file (permissions, disk,
        # etc.) is the one case that must still block the row: the runner
        # catches it internally and reports False, which purge treats as a
        # failure to retry next run.
        owner = uuid4()
        lid = uuid4()
        conn = _conn(
            {"FROM customer.layer": [{"id": lid, "type": "feature", "user_id": owner}]}
        )
        task = _make_task()
        with (
            patch(
                "goatlib.tasks.purge_trash.asyncpg.connect",
                AsyncMock(return_value=conn),
            ),
            patch(_DUCKLAKE_TARGET, return_value=True),
            patch(_PMTILES_TARGET, return_value=False),  # real remove error
        ):
            out = await task._run(PurgeTrashParams())

        assert out.skipped == 1
        assert out.purged["layer"] == 0
        assert "DELETE FROM customer.layer" not in _executed(conn)
        # The DuckLake side still succeeded independently.
        assert out.ducklake_deleted == 1


# ---------------------------------------------------------------------------
# Dry run
# ---------------------------------------------------------------------------


class TestDryRun:
    async def test_dry_run_deletes_nothing(self) -> None:
        conn = _conn(
            {
                "FROM customer.layer": [
                    {"id": uuid4(), "type": "table", "user_id": uuid4()}
                ]
            }
        )
        task = _make_task()
        with (
            patch(
                "goatlib.tasks.purge_trash.asyncpg.connect",
                AsyncMock(return_value=conn),
            ),
            patch(_ARTIFACTS_TARGET) as dl,
        ):
            out = await task._run(PurgeTrashParams(dry_run=True))

        dl.assert_not_called()
        conn.execute.assert_not_awaited()
        assert out.dry_run is True
        # Preview count still reported so operators can see what would go.
        assert out.purged["layer"] == 1


# ---------------------------------------------------------------------------
# Projects / bundles
# ---------------------------------------------------------------------------


class TestSimpleResourcePurge:
    async def test_purges_projects_and_bundles_with_their_grants(self) -> None:
        pid, bid = uuid4(), uuid4()
        conn = _conn(
            {
                "FROM customer.project": [{"id": pid}],
                "FROM customer.bundle": [{"id": bid}],
            }
        )
        task = _make_task()
        with patch(
            "goatlib.tasks.purge_trash.asyncpg.connect",
            AsyncMock(return_value=conn),
        ):
            out = await task._run(PurgeTrashParams())

        assert out.purged["project"] == 1
        assert out.purged["bundle"] == 1
        executed = _executed(conn)
        assert "resource_type = 'project'" in executed
        assert "DELETE FROM customer.project" in executed
        assert "resource_type = 'bundle'" in executed
        assert "DELETE FROM customer.bundle" in executed
        project_delete = next(
            str(c.args[0])
            for c in conn.execute.await_args_list
            if "DELETE FROM customer.project " in str(c.args[0])
        )
        bundle_delete = next(
            str(c.args[0])
            for c in conn.execute.await_args_list
            if "DELETE FROM customer.bundle " in str(c.args[0])
        )
        assert "AND deleted_at IS NOT NULL" in project_delete
        assert "AND deleted_at IS NOT NULL" in bundle_delete


class TestTemplatePurge:
    async def test_trashed_template_purges_with_its_grants(self) -> None:
        # `template` is one of SIMPLE_RESOURCE_TYPES: a template past
        # retention loses its resource_grant rows and its own row, and is
        # counted under its own key.
        tid = uuid4()
        conn = _conn({"FROM customer.template r": [{"id": tid}]})
        task = _make_task()
        with patch(
            "goatlib.tasks.purge_trash.asyncpg.connect",
            AsyncMock(return_value=conn),
        ):
            out = await task._run(PurgeTrashParams())

        assert out.purged["template"] == 1
        executed = _executed(conn)
        assert "resource_type = 'template'" in executed
        assert "DELETE FROM customer.template " in executed
        template_delete = next(
            str(c.args[0])
            for c in conn.execute.await_args_list
            if "DELETE FROM customer.template " in str(c.args[0])
        )
        assert "AND deleted_at IS NOT NULL" in template_delete

    async def test_project_a_live_template_points_at_is_never_purged(self) -> None:
        # A project template's frozen copy is a hidden `project` row the
        # template still points at (`source_project_id`, ON DELETE SET
        # NULL). Purging it would leave the live template unusable, so the
        # project select carries a NOT EXISTS guard against live templates.
        # This fake connection answers like Postgres would: the frozen copy
        # is returned only when the guard is missing from the SQL.
        frozen_id = uuid4()
        conn = MagicMock()

        async def fetch(sql: str, *args: Any) -> list[dict[str, Any]]:
            if "FROM customer.project r" in sql:
                if (
                    "t.source_project_id = r.id" in sql
                    and "t.deleted_at IS NULL" in sql
                ):
                    return []
                return [{"id": frozen_id}]
            return []

        conn.fetch = AsyncMock(side_effect=fetch)
        conn.execute = AsyncMock(return_value="DELETE 1")
        conn.close = AsyncMock()

        task = _make_task()
        with patch(
            "goatlib.tasks.purge_trash.asyncpg.connect",
            AsyncMock(return_value=conn),
        ):
            out = await task._run(PurgeTrashParams())

        assert out.purged["project"] == 0
        assert "DELETE FROM customer.project" not in _executed(conn)
        project_sql = next(
            str(c.args[0])
            for c in conn.fetch.await_args_list
            if "FROM customer.project r" in str(c.args[0])
        )
        assert "customer.template t" in project_sql

    async def test_a_trashed_template_does_not_hold_its_project_back(self) -> None:
        # Only a LIVE template blocks the project: `crud_template.delete`
        # soft-deletes the frozen copy along with the template, so both age
        # out together and the guard must not turn that into a row that can
        # never be purged.
        pid = uuid4()
        conn = _conn({"FROM customer.project r": [{"id": pid}]})
        task = _make_task()
        with patch(
            "goatlib.tasks.purge_trash.asyncpg.connect",
            AsyncMock(return_value=conn),
        ):
            out = await task._run(PurgeTrashParams())

        assert out.purged["project"] == 1
        assert "DELETE FROM customer.project " in _executed(conn)


# ---------------------------------------------------------------------------
# Folders: leaves only
# ---------------------------------------------------------------------------


class TestFolderPurge:
    async def test_deletes_empty_trashed_folder(self) -> None:
        # A real Postgres would only return this folder once — its own row
        # deletion drops it out of the next SELECT. Play that back with a
        # one-shot queue: [row], then empty (loop converges after 2 passes).
        fid = uuid4()
        folder_passes = [[{"id": fid}]]
        conn = MagicMock()

        async def fetch(sql: str, *args: Any) -> list[dict[str, Any]]:
            if "FROM customer.folder" in sql:
                return folder_passes.pop(0) if folder_passes else []
            return []

        conn.fetch = AsyncMock(side_effect=fetch)
        conn.execute = AsyncMock(return_value="DELETE 1")
        conn.close = AsyncMock()

        task = _make_task()
        with patch(
            "goatlib.tasks.purge_trash.asyncpg.connect",
            AsyncMock(return_value=conn),
        ):
            out = await task._run(PurgeTrashParams())

        assert out.purged["folder"] == 1
        executed = _executed(conn)
        assert "resource_type = 'folder'" in executed
        assert "DELETE FROM customer.folder" in executed
        assert "AND deleted_at IS NOT NULL" in executed

    async def test_folder_query_guards_against_any_remaining_child(self) -> None:
        conn = _conn({})
        task = _make_task()
        with patch(
            "goatlib.tasks.purge_trash.asyncpg.connect",
            AsyncMock(return_value=conn),
        ):
            await task._run(PurgeTrashParams())

        folder_sql = next(
            str(c.args[0])
            for c in conn.fetch.await_args_list
            if "FROM customer.folder" in str(c.args[0])
        )
        assert "customer.layer l WHERE l.folder_id = f.id" in folder_sql
        assert "customer.project p WHERE p.folder_id = f.id" in folder_sql
        assert "customer.bundle b WHERE b.folder_id = f.id" in folder_sql
        assert "customer.template t WHERE t.folder_id = f.id" in folder_sql
        assert "customer.folder c WHERE c.parent_id = f.id" in folder_sql

    async def test_folder_holding_a_trashed_template_is_not_purged(self) -> None:
        # A folder cascade-deletes its templates (`template.folder_id` is ON
        # DELETE CASCADE), which would take a still-restorable template with
        # it and leave its grants behind — so a template child keeps the
        # folder out of the select, exactly like a layer or project child.
        # This fake connection answers like Postgres: the folder comes back
        # only when the template guard is missing from the SQL.
        fid = uuid4()
        conn = MagicMock()

        async def fetch(sql: str, *args: Any) -> list[dict[str, Any]]:
            if "FROM customer.folder f" in sql:
                if "customer.template t WHERE t.folder_id = f.id" in sql:
                    return []
                return [{"id": fid}]
            return []

        conn.fetch = AsyncMock(side_effect=fetch)
        conn.execute = AsyncMock(return_value="DELETE 1")
        conn.close = AsyncMock()

        task = _make_task()
        with patch(
            "goatlib.tasks.purge_trash.asyncpg.connect",
            AsyncMock(return_value=conn),
        ):
            out = await task._run(PurgeTrashParams())

        assert out.purged["folder"] == 0
        assert "DELETE FROM customer.folder" not in _executed(conn)

    async def test_depth_3_folder_chain_fully_purged_in_one_run(self) -> None:
        # child -> parent -> root, all trashed past retention and each empty
        # of everything but the next one down. A real Postgres would only
        # ever return the child on the first SELECT (the NOT EXISTS guards
        # keep parent/root out while their child row still exists); this
        # fake conn plays that same sequence back pass-by-pass so the test
        # proves the *task* loops rather than stopping after one pass.
        child, parent, root = uuid4(), uuid4(), uuid4()
        folder_passes = [[{"id": child}], [{"id": parent}], [{"id": root}], []]
        conn = MagicMock()

        async def fetch(sql: str, *args: Any) -> list[dict[str, Any]]:
            if "FROM customer.folder" in sql:
                return folder_passes.pop(0) if folder_passes else []
            return []

        conn.fetch = AsyncMock(side_effect=fetch)
        conn.execute = AsyncMock(return_value="DELETE 1")
        conn.close = AsyncMock()

        task = _make_task()
        with patch(
            "goatlib.tasks.purge_trash.asyncpg.connect",
            AsyncMock(return_value=conn),
        ):
            out = await task._run(PurgeTrashParams())

        assert out.purged["folder"] == 3
        deleted_ids = [
            call.args[1]
            for call in conn.execute.await_args_list
            if "DELETE FROM customer.folder " in str(call.args[0])
        ]
        assert deleted_ids == [[child], [parent], [root]]
        assert folder_passes == []  # every queued pass was consumed

    async def test_folder_passes_are_bounded(self) -> None:
        # An unbroken chain longer than the pass cap must not loop forever —
        # it stops after _MAX_FOLDER_PASSES and leaves the rest for next run.
        conn = MagicMock()
        call_count = 0

        async def fetch(sql: str, *args: Any) -> list[dict[str, Any]]:
            nonlocal call_count
            if "FROM customer.folder" in sql:
                call_count += 1
                return [{"id": uuid4()}]  # always "finds" one more
            return []

        conn.fetch = AsyncMock(side_effect=fetch)
        conn.execute = AsyncMock(return_value="DELETE 1")
        conn.close = AsyncMock()

        task = _make_task()
        with patch(
            "goatlib.tasks.purge_trash.asyncpg.connect",
            AsyncMock(return_value=conn),
        ):
            out = await task._run(PurgeTrashParams())

        assert call_count == task._MAX_FOLDER_PASSES
        assert out.purged["folder"] == task._MAX_FOLDER_PASSES


# ---------------------------------------------------------------------------
# retention_days / batch_size threaded into SQL
# ---------------------------------------------------------------------------


class TestRetentionAndBatching:
    async def test_cutoff_and_batch_size_passed_to_every_query(self) -> None:
        conn = _conn({})
        task = _make_task()
        with patch(
            "goatlib.tasks.purge_trash.asyncpg.connect",
            AsyncMock(return_value=conn),
        ):
            await task._run(PurgeTrashParams(retention_days=45, batch_size=10))

        for call in conn.fetch.await_args_list:
            cutoff_arg, limit_arg = call.args[1], call.args[2]
            assert isinstance(cutoff_arg, datetime)
            assert limit_arg == 10
            # cutoff ~45 days back (allow slack for test execution time)
            expected = datetime.now(timezone.utc) - timedelta(days=45)
            assert abs((cutoff_arg - expected).total_seconds()) < 5


# ---------------------------------------------------------------------------
# run() / main()
# ---------------------------------------------------------------------------


class TestRunAndMain:
    def test_run_requires_init(self) -> None:
        task = PurgeTrashTask()
        with pytest.raises(RuntimeError):
            task.run(PurgeTrashParams())

    def test_main_inits_from_env_and_returns_dict(self) -> None:
        from goatlib.tasks.purge_trash import main

        with patch("goatlib.tasks.purge_trash.PurgeTrashTask") as mock_cls:
            instance = mock_cls.return_value
            instance.run.return_value.model_dump.return_value = {"purged": {}}
            result = main(PurgeTrashParams())

        instance.init_from_env.assert_called_once()
        instance.run.assert_called_once()
        assert result == {"purged": {}}
