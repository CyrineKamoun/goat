from collections.abc import Awaitable, Callable
from uuid import UUID

import pytest
from core.core.config import settings
from core.db.models.folder import Folder
from core.db.models.layer import Layer
from core.db.models.user import User
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from tests.authz.conftest import call_check_layer, find_resource

S = settings.SCHEMA


@pytest.mark.asyncio
async def test_functions_installed_and_roles_seeded(
    db_session: AsyncSession, roles: dict[str, UUID]
) -> None:
    names = {
        r[0]
        for r in (
            await db_session.execute(
                text(
                    "SELECT p.proname FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace "
                    "WHERE n.nspname = :s"
                ),
                {"s": S},
            )
        ).all()
    }
    assert {
        "authorization",
        "check_layer",
        "check_project",
        "check_team",
        "get_needed_roles",
    } <= names
    assert {"layer-viewer", "layer-editor", "project-viewer", "team-member"} <= set(
        roles
    )


@pytest.mark.asyncio
async def test_owner_can_read_own_layer_through_check_layer(
    db_session: AsyncSession,
    roles: dict[str, UUID],
    make_user: Callable[..., Awaitable[User]],
    make_folder: Callable[..., Awaitable[Folder]],
    make_layer: Callable[..., Awaitable[Layer]],
) -> None:
    owner = await make_user()
    folder = await make_folder(owner)
    layer = await make_layer(owner, folder)
    rid = await find_resource(db_session, "layer/{layer_id}", "GET")
    assert await call_check_layer(
        db_session,
        resource_id=rid,
        user_id=owner.id,
        organization_id=None,
        layer_ids=[layer.id],
    )


@pytest.mark.asyncio
async def test_stranger_cannot_read_layer(
    db_session: AsyncSession,
    roles: dict[str, UUID],
    make_user: Callable[..., Awaitable[User]],
    make_folder: Callable[..., Awaitable[Folder]],
    make_layer: Callable[..., Awaitable[Layer]],
) -> None:
    owner = await make_user()
    stranger = await make_user()
    layer = await make_layer(owner, await make_folder(owner))
    rid = await find_resource(db_session, "layer/{layer_id}", "GET")
    assert not await call_check_layer(
        db_session,
        resource_id=rid,
        user_id=stranger.id,
        organization_id=None,
        layer_ids=[layer.id],
    )


@pytest.mark.asyncio
async def test_session_recovers_after_a_denied_check(
    db_session: AsyncSession,
    roles: dict[str, UUID],
    make_user: Callable[..., Awaitable[User]],
    make_folder: Callable[..., Awaitable[Folder]],
    make_layer: Callable[..., Awaitable[Layer]],
) -> None:
    """A denied check_layer call raises inside Postgres; the same db_session
    must still be usable afterward. Eight later tasks write multi-assertion
    tests that mix a denied call with a subsequent allowed one in one
    session — if _call's recovery (plain rollback(), see conftest.py) is
    ever subtly wrong, every one of those tests would fail confusingly with
    nothing here to blame."""
    owner = await make_user()
    stranger = await make_user()
    folder = await make_folder(owner)
    layer = await make_layer(owner, folder)
    rid = await find_resource(db_session, "layer/{layer_id}", "GET")

    # denied: raises inside the DB, caught and turned into False
    assert not await call_check_layer(
        db_session,
        resource_id=rid,
        user_id=stranger.id,
        organization_id=None,
        layer_ids=[layer.id],
    )

    # the same session must still execute further statements correctly
    assert await call_check_layer(
        db_session,
        resource_id=rid,
        user_id=owner.id,
        organization_id=None,
        layer_ids=[layer.id],
    )
    assert await find_resource(db_session, "layer/{layer_id}", "GET") == rid
