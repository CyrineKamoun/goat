"""Authorization on the bundle tools that are handed resource ids.

The processes service dispatches a job without authorizing the ids inside it,
so each of these tools has to ask for itself. They are one family with four
different right answers, which is why the verbs are asserted and not just the
refusals: a copy reads its source, an ingest and a rebuild write the bundle,
and a cleanup runs after the row is gone and needs its own rule.
"""

from types import SimpleNamespace

import pytest
from goatlib.tools.authz import (
    authorize_bundle_ingest,
    authorize_bundle_rebuild,
)

USER = "11111111-1111-1111-1111-111111111111"
BUNDLE = "22222222-2222-2222-2222-222222222222"
FOLDER = "33333333-3333-3333-3333-333333333333"


class _Authz:
    """Answers `user_can` from a table, and records what was asked."""

    def __init__(self, allowed: dict) -> None:
        self.allowed = allowed
        self.asked: list[tuple] = []

    async def user_can(self, resource_type, resource_id, user_id, action):
        self.asked.append((resource_type, resource_id, user_id, action))
        return self.allowed.get((resource_type, action), False)


# --- rebuild ----------------------------------------------------------------


async def test_a_rebuild_needs_write_on_the_bundle_not_read() -> None:
    """The sharpest of the four. A rebuild replaces the artifact everything
    routing through the bundle reads, and it builds as the bundle's *owner* —
    so read access is not enough, and an unchecked id would let any
    authenticated caller cause work under someone else's identity."""
    db = _Authz({("bundle", "read"): True})
    with pytest.raises(ValueError, match="not yours to change"):
        await authorize_bundle_rebuild(db, user_id=USER, bundle_id=BUNDLE)
    assert db.asked == [("bundle", BUNDLE, USER, "write")]


async def test_a_rebuild_the_caller_may_write_is_allowed() -> None:
    db = _Authz({("bundle", "write"): True})
    await authorize_bundle_rebuild(db, user_id=USER, bundle_id=BUNDLE)


async def test_the_rebuild_tool_asks_before_it_reads_the_bundle(monkeypatch) -> None:
    """Ordering matters here for a specific reason: the identity the build runs
    under comes out of the row this must not read yet."""
    from goatlib.tools import bundle_artifact_rebuild as module

    class _Db:
        def __init__(self, pool, schema=None) -> None:
            pass

        async def user_can(self, *args, **kwargs):
            return False

        async def get_bundle(self, *args, **kwargs):
            raise AssertionError("the bundle was read before authorization")

    monkeypatch.setattr(module, "ToolDatabaseService", _Db)
    runner = module.BundleArtifactRebuildRunner.__new__(
        module.BundleArtifactRebuildRunner
    )
    runner.settings = SimpleNamespace(customer_schema="customer")
    runner._duckdb_con = None

    async def _pool():
        return object()

    runner.get_postgres_pool = _pool
    runner.cleanup = lambda: None

    with pytest.raises(ValueError, match="not yours to change"):
        await runner._run(
            module.BundleArtifactRebuildParams(user_id=USER, bundle_id=BUNDLE)
        )


# --- ingest -----------------------------------------------------------------


async def test_an_ingest_writes_the_bundle_and_the_folder() -> None:
    """Two resources: the bundle it fills and flips to ready, and the folder
    its member layers are created in."""
    db = _Authz({("bundle", "write"): True, ("folder", "write"): True})
    await authorize_bundle_ingest(db, user_id=USER, bundle_id=BUNDLE, folder_id=FOLDER)
    assert db.asked == [
        ("bundle", BUNDLE, USER, "write"),
        ("folder", FOLDER, USER, "write"),
    ]


async def test_ingesting_into_a_bundle_that_is_not_yours_is_refused() -> None:
    db = _Authz({("folder", "write"): True})
    with pytest.raises(ValueError, match="nothing can be imported into it"):
        await authorize_bundle_ingest(
            db, user_id=USER, bundle_id=BUNDLE, folder_id=FOLDER
        )


async def test_ingesting_into_a_folder_that_is_not_yours_is_refused() -> None:
    db = _Authz({("bundle", "write"): True})
    with pytest.raises(ValueError, match="have nowhere to go"):
        await authorize_bundle_ingest(
            db, user_id=USER, bundle_id=BUNDLE, folder_id=FOLDER
        )


async def test_a_refused_ingest_does_not_roll_the_bundle_away(monkeypatch) -> None:
    """The refusal sits outside the rollback on purpose. Deleting a bundle the
    caller was just told they may not touch would be worse than the unchecked
    ingest was."""
    from goatlib.bundles import runner as module

    rolled: list = []

    class _Db:
        def __init__(self, pool, schema=None) -> None:
            pass

        async def user_can(self, *args, **kwargs):
            return False

    class _Pool:
        async def close(self) -> None:
            pass

    monkeypatch.setattr(module, "ToolDatabaseService", _Db)
    runner = module.BundleImportRunner.__new__(module.BundleImportRunner)
    runner.settings = SimpleNamespace(customer_schema="customer")
    runner._duckdb_con = None
    runner.cleanup = lambda: None

    async def _pool():
        return _Pool()

    async def _rollback(*args, **kwargs):
        rolled.append(True)

    runner.get_postgres_pool = _pool
    runner._rollback_bundle = _rollback

    with pytest.raises(ValueError, match="nothing can be imported into it"):
        await runner.ingest_into_bundle(
            bundle_id=BUNDLE,
            source_path="/tmp/x.zip",
            bundle_type="street_network",
            user_id=USER,
            folder_id=FOLDER,
        )
    assert rolled == []
