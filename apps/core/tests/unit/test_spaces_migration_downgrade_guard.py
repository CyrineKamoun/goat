"""A downgrade must never delete content.

The consolidated migrations (`0001_bundles_catalog` … `0005_home_templates`)
carry data steps that cannot be undone without losing rows — the legacy
share-link tables are dropped, the catalog identity is deleted, the legacy
layer columns and the scenario tables go — so none of them offers a
downgrade: each one refuses. This guards that directly in source (rather
than by running alembic against a database) the same way
test_drop_migration_installs_functions.py guards `0003`'s ordering: import
each migration module by path and assert on its `downgrade()`.
"""

import importlib.util
import inspect
from pathlib import Path
from types import ModuleType

import pytest

pytestmark = pytest.mark.unit

VERSIONS_DIR = Path(__file__).parents[2] / "alembic" / "versions"
REVISIONS = [
    "0001_bundles_catalog",
    "0002_favorites",
    "0003_sharing_grants",
    "0004_spaces",
    "0005_home_templates",
]


def _load_migration(name: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        f"{name}_migration", VERSIONS_DIR / f"{name}.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("name", REVISIONS)
def test_downgrade_refuses_instead_of_touching_rows(name: str) -> None:
    module = _load_migration(name)
    source = inspect.getsource(module.downgrade)
    assert "DELETE FROM" not in source
    assert "DROP TABLE" not in source
    assert "raise NotImplementedError" in source
    with pytest.raises(NotImplementedError):
        module.downgrade()


def test_the_chain_runs_from_init_in_order() -> None:
    expected_parent = "init"
    for name in REVISIONS:
        module = _load_migration(name)
        assert module.revision == name
        assert module.down_revision == expected_parent
        expected_parent = name
