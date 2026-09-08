"""check_layer / check_project must give the same answers as effective_role."""

from collections.abc import Awaitable, Callable
from uuid import UUID, uuid4

import pytest
from core.core.config import settings
from core.db.models.folder import Folder
from core.db.models.layer import Layer
from core.db.models.organization import Organization
from core.db.models.project import Project
from core.db.models.user import User
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from tests.authz.conftest import call_check_layer, call_check_project, find_resource

S = settings.SCHEMA


async def grant(db: AsyncSession, roles: dict[str, UUID], **k: object) -> None:
    await db.execute(
        text(
            f"INSERT INTO {S}.resource_grant "
            "(resource_type, resource_id, grantee_type, grantee_id, role_id, granted_by) "
            "VALUES (:t,:r,:gt,:g,:role,:by)"
        ),
        {
            "t": k["rtype"],
            "r": k["rid"],
            "gt": k["gtype"],
            "g": k["gid"],
            "role": roles[str(k["role_name"])],
            "by": k["by"],
        },
    )


@pytest.mark.asyncio
async def test_layer_viewer_grant_reads_but_cannot_update(
    db_session: AsyncSession,
    roles: dict[str, UUID],
    make_user: Callable[..., Awaitable[User]],
    make_org: Callable[[], Awaitable[Organization]],
    make_folder: Callable[..., Awaitable[Folder]],
    make_layer: Callable[..., Awaitable[Layer]],
) -> None:
    org = await make_org()
    owner, viewer = await make_user(org.id), await make_user(org.id)
    layer = await make_layer(owner, await make_folder(owner))
    await grant(
        db_session,
        roles,
        rtype="layer",
        rid=layer.id,
        gtype="user",
        gid=viewer.id,
        role_name="layer-viewer",
        by=owner.id,
    )
    get_r = await find_resource(db_session, "layer/{layer_id}", "GET")
    put_r = await find_resource(db_session, "layer/{layer_id}", "PUT")
    assert await call_check_layer(
        db_session,
        resource_id=get_r,
        user_id=viewer.id,
        organization_id=org.id,
        layer_ids=[layer.id],
    )
    assert not await call_check_layer(
        db_session,
        resource_id=put_r,
        user_id=viewer.id,
        organization_id=org.id,
        layer_ids=[layer.id],
    )


@pytest.mark.asyncio
async def test_owner_passes_without_any_grant_row(
    db_session: AsyncSession,
    roles: dict[str, UUID],
    make_user: Callable[..., Awaitable[User]],
    make_folder: Callable[..., Awaitable[Folder]],
    make_layer: Callable[..., Awaitable[Layer]],
    make_project: Callable[..., Awaitable[Project]],
) -> None:
    owner = await make_user()
    folder = await make_folder(owner)
    layer = await make_layer(owner, folder)
    project = await make_project(owner, folder)
    put_r = await find_resource(db_session, "layer/{layer_id}", "PUT")
    add_r = await find_resource(db_session, "project/{project_id}/layer", "POST")
    assert await call_check_layer(
        db_session,
        resource_id=put_r,
        user_id=owner.id,
        organization_id=None,
        layer_ids=[layer.id],
    )
    assert await call_check_project(
        db_session,
        resource_id=add_r,
        user_id=owner.id,
        organization_id=None,
        project_ids=[project.id],
    )


@pytest.mark.asyncio
async def test_batch_fails_if_any_layer_is_denied(
    db_session: AsyncSession,
    roles: dict[str, UUID],
    make_user: Callable[..., Awaitable[User]],
    make_folder: Callable[..., Awaitable[Folder]],
    make_layer: Callable[..., Awaitable[Layer]],
) -> None:
    owner, other = await make_user(), await make_user()
    mine = await make_layer(owner, await make_folder(owner))
    theirs = await make_layer(other, await make_folder(other))
    get_r = await find_resource(db_session, "layer/{layer_id}", "GET")
    assert not await call_check_layer(
        db_session,
        resource_id=get_r,
        user_id=owner.id,
        organization_id=None,
        layer_ids=[mine.id, theirs.id],
    )


@pytest.mark.asyncio
async def test_project_editor_via_org_grant_can_add_layers(
    db_session: AsyncSession,
    roles: dict[str, UUID],
    make_user: Callable[..., Awaitable[User]],
    make_org: Callable[[], Awaitable[Organization]],
    make_folder: Callable[..., Awaitable[Folder]],
    make_project: Callable[..., Awaitable[Project]],
) -> None:
    org = await make_org()
    owner, member = await make_user(org.id), await make_user(org.id)
    project = await make_project(owner, await make_folder(owner))
    await grant(
        db_session,
        roles,
        rtype="project",
        rid=project.id,
        gtype="organization",
        gid=org.id,
        role_name="project-editor",
        by=owner.id,
    )
    add_r = await find_resource(db_session, "project/{project_id}/layer", "POST")
    assert await call_check_project(
        db_session,
        resource_id=add_r,
        user_id=member.id,
        organization_id=org.id,
        project_ids=[project.id],
    )


@pytest.mark.asyncio
async def test_layer_viewer_can_add_the_layer_to_a_project_they_own(
    db_session: AsyncSession,
    roles: dict[str, UUID],
    make_user: Callable[..., Awaitable[User]],
    make_org: Callable[[], Awaitable[Organization]],
    make_folder: Callable[..., Awaitable[Folder]],
    make_layer: Callable[..., Awaitable[Layer]],
) -> None:
    org = await make_org()
    owner, member = await make_user(org.id), await make_user(org.id)
    layer = await make_layer(owner, await make_folder(owner))
    await grant(
        db_session,
        roles,
        rtype="layer",
        rid=layer.id,
        gtype="user",
        gid=member.id,
        role_name="layer-viewer",
        by=owner.id,
    )
    add_r = await find_resource(db_session, "project/{project_id}/layer", "POST")
    assert await call_check_layer(
        db_session,
        resource_id=add_r,
        user_id=member.id,
        organization_id=org.id,
        layer_ids=[layer.id],
    )
    put_r = await find_resource(db_session, "layer/{layer_id}", "PUT")
    assert not await call_check_layer(
        db_session,
        resource_id=put_r,
        user_id=member.id,
        organization_id=org.id,
        layer_ids=[layer.id],
    )


@pytest.mark.asyncio
async def test_unknown_resource_raises_no_roles(
    db_session: AsyncSession,
    roles: dict[str, UUID],
    make_user: Callable[..., Awaitable[User]],
    make_folder: Callable[..., Awaitable[Folder]],
    make_layer: Callable[..., Awaitable[Layer]],
) -> None:
    owner = await make_user()
    layer = await make_layer(owner, await make_folder(owner))
    assert not await call_check_layer(
        db_session,
        resource_id=uuid4(),
        user_id=owner.id,
        organization_id=None,
        layer_ids=[layer.id],
    )
