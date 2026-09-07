"""I7: the grant copy in `0003_sharing_grants` must UPDATE a changed role_id,
not silently keep the stale one — the DO NOTHING form dropped any role change
made since a previous run, and the copy has to be safe to re-run because it is
what the verification before the DROP checks against."""

import importlib.util
from pathlib import Path
from types import ModuleType

MIGRATION_PATH = (
    Path(__file__).parents[2] / "alembic" / "versions" / "0003_sharing_grants.py"
)


def _load_migration() -> ModuleType:
    spec = importlib.util.spec_from_file_location("grants_migration", MIGRATION_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_conflict_clause_updates_role_id_by_column_list_not_do_nothing() -> None:
    module = _load_migration()
    assert "DO NOTHING" not in module.CONFLICT
    assert "ON CONFLICT (resource_type, resource_id, grantee_type, grantee_id)" in (
        module.CONFLICT
    )
    assert "DO UPDATE SET role_id = EXCLUDED.role_id" in module.CONFLICT


def test_every_copy_statement_embeds_the_conflict_clause() -> None:
    module = _load_migration()
    for stmt in module.COPY_SQL:
        assert module.CONFLICT in stmt
