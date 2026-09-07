"""The SQL behind ``LayerService.user_can_edit_layer``, against a real database.

The rule, enforced by ``customer.layer_write_allowed``, is: the layer's
owner; an editor-or-owner via a direct, folder, or bundle grant
(``effective_role('layer', ...)``); or the shared-workspace rule — a
project containing the layer where the owner and the requester both hold an
edit-or-owner role on that project. A catalog layer (owner NULL, or flagged
``in_catalog``/``catalog_external_uid``) is never writable by anyone but its
owner column — no grant path, including the shared-workspace one, may raise
it above viewer.

The owner-side half of the shared-workspace condition is a security
boundary, not a nicety — adding a layer to a project only requires
``read-layer``, so without it any user could add a catalog layer (or one
shared with them as viewer) to a project they own and inherit write access to
somebody else's dataset. Mocked tests cannot show that; only the SQL can.

Every test runs inside a transaction that is rolled back, so the database is
left untouched.

Requires Postgres with the ``customer`` schema (local compose stack). Run
with: ``uv run pytest apps/geoapi/tests/integration -m integration``
"""

from __future__ import annotations

import uuid
from typing import AsyncGenerator, cast

import asyncpg
import pytest
import pytest_asyncio
from fastapi import HTTPException

from geoapi.config import settings
from geoapi.services.layer_service import LayerService

pytestmark = pytest.mark.integration


class _SingleConnectionPool:
    """Adapts one transaction-bound connection to the pool API LayerService uses.

    LayerService only ever does ``async with pool.acquire() as conn``; handing
    it the open transaction's connection keeps every read inside the rollback.
    """

    def __init__(self, conn: asyncpg.Connection) -> None:
        self._conn = conn

    def acquire(self) -> "_SingleConnectionPool":
        return self

    async def __aenter__(self) -> asyncpg.Connection:
        return self._conn

    async def __aexit__(self, *exc_info: object) -> None:
        return None


@pytest_asyncio.fixture()
async def conn() -> AsyncGenerator[asyncpg.Connection, None]:
    try:
        connection = await asyncpg.connect(
            host=settings.POSTGRES_SERVER,
            port=settings.POSTGRES_PORT,
            user=settings.POSTGRES_USER,
            password=settings.POSTGRES_PASSWORD,
            database=settings.POSTGRES_DB,
            timeout=5,
        )
    except (OSError, asyncpg.PostgresError) as exc:
        pytest.skip(f"Postgres not reachable: {exc}")

    transaction = connection.transaction()
    await transaction.start()
    try:
        yield connection
    finally:
        await transaction.rollback()
        await connection.close()


@pytest_asyncio.fixture()
async def service(conn: asyncpg.Connection) -> LayerService:
    svc = LayerService()
    svc._pool = _SingleConnectionPool(conn)
    return svc


async def _role_id(conn: asyncpg.Connection, name: str) -> uuid.UUID:
    role_id = await conn.fetchval("SELECT id FROM customer.role WHERE name = $1", name)
    assert role_id is not None, f"role {name} missing — is the DB seeded?"
    return role_id


async def _make_user(conn: asyncpg.Connection) -> uuid.UUID:
    return await conn.fetchval(
        "INSERT INTO customer.user (id, email) VALUES ($1, $2) RETURNING id",
        uuid.uuid4(),
        f"{uuid.uuid4()}@example.test",
    )


async def _ensure_personal_space(
    conn: asyncpg.Connection, user_id: uuid.UUID
) -> uuid.UUID:
    """The owner of `effective_role` is a space, not a user_id column — every
    user this suite creates needs its own personal space before it can own
    anything."""
    space_id = await conn.fetchval(
        "SELECT id FROM customer.space WHERE kind = 'personal' AND user_id = $1",
        user_id,
    )
    if space_id is not None:
        return cast(uuid.UUID, space_id)
    return cast(
        uuid.UUID,
        await conn.fetchval(
            """
            INSERT INTO customer.space (id, kind, user_id, default_role)
            VALUES ($1, 'personal', $2, 'editor') RETURNING id
            """,
            uuid.uuid4(),
            user_id,
        ),
    )


async def _make_folder(conn: asyncpg.Connection, user_id: uuid.UUID) -> uuid.UUID:
    """A fresh root folder in the owner's personal space. Root folder names
    are unique per space, so each call needs its own name — one user's
    tests can create several folders in the same (now shared) space."""
    space_id = await _ensure_personal_space(conn, user_id)
    folder_id = uuid.uuid4()
    return await conn.fetchval(
        """
        INSERT INTO customer.folder (id, user_id, space_id, name, updated_at)
        VALUES ($1, $2, $3, $4, NOW()) RETURNING id
        """,
        folder_id,
        user_id,
        space_id,
        f"test-folder-{folder_id}",
    )


async def _make_layer(
    conn: asyncpg.Connection, owner_id: uuid.UUID, *, in_catalog: bool = False
) -> uuid.UUID:
    folder_id = await _make_folder(conn, owner_id)
    space_id = await _ensure_personal_space(conn, owner_id)
    return await conn.fetchval(
        """
        INSERT INTO customer.layer
            (id, user_id, folder_id, space_id, name, type, in_catalog, updated_at)
        VALUES ($1, $2, $3, $4, 'test-layer', 'feature', $5, NOW())
        RETURNING id
        """,
        uuid.uuid4(),
        owner_id,
        folder_id,
        space_id,
        in_catalog,
    )


async def _make_project(conn: asyncpg.Connection, owner_id: uuid.UUID) -> uuid.UUID:
    """Create a project in the owner's personal space — the space is what
    makes the owner its owner under `effective_role`."""
    folder_id = await _make_folder(conn, owner_id)
    space_id = await _ensure_personal_space(conn, owner_id)
    return await conn.fetchval(
        """
        INSERT INTO customer.project (id, user_id, folder_id, space_id, name, updated_at)
        VALUES ($1, $2, $3, $4, 'test-project', NOW())
        RETURNING id
        """,
        uuid.uuid4(),
        owner_id,
        folder_id,
        space_id,
    )


async def _add_layer_to_project(
    conn: asyncpg.Connection, layer_id: uuid.UUID, project_id: uuid.UUID
) -> None:
    await conn.execute(
        """
        INSERT INTO customer.layer_project
            (layer_id, project_id, name, "order", updated_at)
        VALUES ($1, $2, 'test-layer', 0, NOW())
        """,
        layer_id,
        project_id,
    )


async def _share_project_with_user(
    conn: asyncpg.Connection,
    project_id: uuid.UUID,
    user_id: uuid.UUID,
    role: str,
) -> None:
    granter = await _make_user(conn)
    await conn.execute(
        """
        INSERT INTO customer.resource_grant
            (resource_type, resource_id, grantee_type, grantee_id, role_id, granted_by)
        VALUES ('project', $1, 'user', $2, $3, $4)
        """,
        project_id,
        user_id,
        await _role_id(conn, role),
        granter,
    )


async def _share_project_with_team(
    conn: asyncpg.Connection,
    project_id: uuid.UUID,
    user_id: uuid.UUID,
    role: str,
) -> None:
    team_id = await conn.fetchval(
        "INSERT INTO customer.team (id, name) VALUES ($1, 'test-team') RETURNING id",
        uuid.uuid4(),
    )
    await conn.execute(
        "INSERT INTO customer.user_team (team_id, user_id, role_id) VALUES ($1, $2, $3)",
        team_id,
        user_id,
        await _role_id(conn, "team-member"),
    )
    granter = await _make_user(conn)
    await conn.execute(
        """
        INSERT INTO customer.resource_grant
            (resource_type, resource_id, grantee_type, grantee_id, role_id, granted_by)
        VALUES ('project', $1, 'team', $2, $3, $4)
        """,
        project_id,
        team_id,
        await _role_id(conn, role),
        granter,
    )


# --------------------------------------------------------------------------
# The rule admits collaborators
# --------------------------------------------------------------------------


async def test_editor_on_owners_shared_project_may_edit_the_owners_layer(
    conn: asyncpg.Connection, service: LayerService
) -> None:
    owner = await _make_user(conn)
    collaborator = await _make_user(conn)
    layer = await _make_layer(conn, owner)
    project = await _make_project(conn, owner)
    await _add_layer_to_project(conn, layer, project)
    await _share_project_with_user(conn, project, collaborator, "project-editor")

    assert await service.user_can_edit_layer(layer, collaborator) is True


async def test_team_editor_on_owners_shared_project_may_edit_the_owners_layer(
    conn: asyncpg.Connection, service: LayerService
) -> None:
    owner = await _make_user(conn)
    collaborator = await _make_user(conn)
    layer = await _make_layer(conn, owner)
    project = await _make_project(conn, owner)
    await _add_layer_to_project(conn, layer, project)
    await _share_project_with_team(conn, project, collaborator, "project-editor")

    assert await service.user_can_edit_layer(layer, collaborator) is True


async def test_project_owner_may_edit_a_collaborators_contributed_layer(
    conn: asyncpg.Connection, service: LayerService
) -> None:
    """Symmetry: a layer a collaborator adds to the shared project is editable
    by the project owner, because the contributor is an editor there too."""
    project_owner = await _make_user(conn)
    collaborator = await _make_user(conn)
    project = await _make_project(conn, project_owner)
    await _share_project_with_user(conn, project, collaborator, "project-editor")
    layer = await _make_layer(conn, collaborator)
    await _add_layer_to_project(conn, layer, project)

    assert await service.user_can_edit_layer(layer, project_owner) is True


# --------------------------------------------------------------------------
# The rule refuses everyone else — the escalation boundary
# --------------------------------------------------------------------------


async def test_adding_a_catalog_layer_to_your_own_project_grants_no_write(
    conn: asyncpg.Connection, service: LayerService
) -> None:
    """Catalog layers are readable by everyone. Linking one into a project you
    own must not turn that read access into write access on someone else's
    dataset."""
    owner = await _make_user(conn)
    outsider = await _make_user(conn)
    layer = await _make_layer(conn, owner, in_catalog=True)
    outsiders_project = await _make_project(conn, outsider)
    await _add_layer_to_project(conn, layer, outsiders_project)

    assert await service.user_can_edit_layer(layer, outsider) is False


async def test_adding_someone_elses_layer_to_your_own_project_grants_no_write(
    conn: asyncpg.Connection, service: LayerService
) -> None:
    """Same escalation via a layer shared read-only rather than via catalog."""
    owner = await _make_user(conn)
    outsider = await _make_user(conn)
    layer = await _make_layer(conn, owner)
    outsiders_project = await _make_project(conn, outsider)
    await _add_layer_to_project(conn, layer, outsiders_project)

    assert await service.user_can_edit_layer(layer, outsider) is False


async def test_viewer_on_the_owners_project_may_not_edit(
    conn: asyncpg.Connection, service: LayerService
) -> None:
    owner = await _make_user(conn)
    viewer = await _make_user(conn)
    layer = await _make_layer(conn, owner)
    project = await _make_project(conn, owner)
    await _add_layer_to_project(conn, layer, project)
    await _share_project_with_user(conn, project, viewer, "project-viewer")

    assert await service.user_can_edit_layer(layer, viewer) is False


async def test_unrelated_user_may_not_edit(
    conn: asyncpg.Connection, service: LayerService
) -> None:
    owner = await _make_user(conn)
    stranger = await _make_user(conn)
    layer = await _make_layer(conn, owner)
    project = await _make_project(conn, owner)
    await _add_layer_to_project(conn, layer, project)

    assert await service.user_can_edit_layer(layer, stranger) is False


async def test_layer_in_no_project_grants_no_write(
    conn: asyncpg.Connection, service: LayerService
) -> None:
    owner = await _make_user(conn)
    stranger = await _make_user(conn)
    layer = await _make_layer(conn, owner)

    assert await service.user_can_edit_layer(layer, stranger) is False


# --------------------------------------------------------------------------
# Injection
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "malicious_layer_id",
    [
        "' OR '1'='1",
        "00000000-0000-0000-0000-000000000000'; DROP TABLE customer.layer; --",
        "$2::uuid OR true",
    ],
)
async def test_layer_id_is_never_interpolated_into_the_query(
    conn: asyncpg.Connection, service: LayerService, malicious_layer_id: str
) -> None:
    """The layer id arrives from the URL path.

    It is rejected as malformed before reaching the database; were it ever
    interpolated rather than bound, these payloads would parse as SQL.
    """
    stranger = await _make_user(conn)

    with pytest.raises(HTTPException) as exc_info:
        await service.user_can_edit_layer(malicious_layer_id, stranger)

    assert exc_info.value.status_code == 400
    # The table the payload tries to drop is still there.
    assert await conn.fetchval("SELECT to_regclass('customer.layer')") is not None


async def test_shared_project_does_not_grant_write_on_unrelated_layers(
    conn: asyncpg.Connection, service: LayerService
) -> None:
    """The grant is scoped to layers actually in the shared project."""
    owner = await _make_user(conn)
    collaborator = await _make_user(conn)
    shared_layer = await _make_layer(conn, owner)
    private_layer = await _make_layer(conn, owner)
    project = await _make_project(conn, owner)
    await _add_layer_to_project(conn, shared_layer, project)
    await _share_project_with_user(conn, project, collaborator, "project-editor")

    assert await service.user_can_edit_layer(private_layer, collaborator) is False
