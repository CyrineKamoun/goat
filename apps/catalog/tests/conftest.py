import os
from collections.abc import Iterator
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from catalog.app import create_app
from catalog.config import CatalogSettings
from catalog.services.registry import QueryableRegistry
from catalog.store import CatalogStore

from .fixtures.gen_catalog import write_catalog, write_nuts


@pytest.fixture(autouse=True, scope="session")
def _settings_from_kwargs_only() -> Iterator[None]:
    """Let each test construct settings explicitly, whatever the dev's env says.

    Many `CatalogSettings` fields read a bare env var through a
    `validation_alias` (`AUTH`, `S3_ENDPOINT_URL`, ...). When such a var is
    set, pydantic-settings supplies the field under its alias, and a
    `CatalogSettings(field=...)` kwarg arrives under the field name as well —
    a second key for an already-populated field, which the model rejects as an
    extra input. Tests construct settings that way throughout, so a developer
    whose environment defines those vars would see the suite fail on
    configuration rather than on behaviour.

    The names come from the model, so a field added later is covered without
    touching this.
    """
    aliased: set[str] = set()
    for field in CatalogSettings.model_fields.values():
        alias = field.validation_alias
        if alias is None:
            continue
        choices = getattr(alias, "choices", [alias])
        aliased.update(c for c in choices if isinstance(c, str))

    saved = {k: os.environ.pop(k) for k in aliased if k in os.environ}
    yield
    os.environ.update(saved)


@pytest.fixture()
def catalog_dir(tmp_path: Path) -> Path:
    write_catalog(tmp_path)
    write_nuts(tmp_path)
    return tmp_path


@pytest.fixture()
def client(catalog_dir: Path) -> Iterator[TestClient]:
    # AUTH defaults to True (see catalog.config.CatalogSettings); tests use
    # the open-access path by default and opt into AUTH=True explicitly
    # (see tests/test_endpoints.py's auth tests) rather than the reverse.
    app = create_app(CatalogSettings(data_dir=catalog_dir, auth=False))
    with TestClient(app) as c:
        yield c


@pytest.fixture()
def store(catalog_dir: Path) -> CatalogStore:
    s = CatalogStore(CatalogSettings(data_dir=catalog_dir))
    s.ensure_current()
    return s


@pytest.fixture()
def registry(store: CatalogStore) -> QueryableRegistry:
    """The queryable registry of the fixture catalog.

    Taken off the store rather than constructed, so these tests exercise the
    registry that was actually derived from the loaded table (a hand-built one
    could advertise columns the fixture doesn't have).
    """
    return store.registry
