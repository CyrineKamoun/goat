"""Folder grants must aggregate: the strongest role wins.

A user can reach the same folder through more than one grant (e.g. a viewer
grant via a team and an editor grant via the organization). ``check_project``
must derive access from the strongest role reached, not from whichever grant
row a single-row pick happens to return.
"""

from collections.abc import Awaitable, Callable
from uuid import UUID

import pytest
from core.core.config import settings
from core.db.models.folder import Folder
from core.db.models.organization import Organization
from core.db.models.project import Project
from core.db.models.team import Team
from core.db.models.user import User
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from tests.authz.conftest import call_check_project, find_resource

S = settings.SCHEMA


async def _grant(
    db: AsyncSession,
    *,
    folder_id: UUID,
    grantee_type: str,
    grantee_id: UUID,
    role_id: UUID,
    granted_by: UUID,
) -> None:
    await db.execute(
        text(
            f"INSERT INTO {S}.resource_grant "
            "(resource_type, resource_id, grantee_type, grantee_id, role_id, granted_by) "
            "VALUES ('folder', :rid, :gt, :gid, :role, :by)"
        ),
        {
            "rid": folder_id,
            "gt": grantee_type,
            "gid": grantee_id,
            "role": role_id,
            "by": granted_by,
        },
    )


@pytest.mark.asyncio
async def test_editor_grant_wins_over_viewer_grant_on_same_folder(
    db_session: AsyncSession,
    roles: dict[str, UUID],
    make_user: Callable[..., Awaitable[User]],
    make_org: Callable[[], Awaitable[Organization]],
    make_team: Callable[..., Awaitable[Team]],
    make_folder: Callable[..., Awaitable[Folder]],
    make_project: Callable[..., Awaitable[Project]],
) -> None:
    org = await make_org()
    owner = await make_user(org.id)
    user = await make_user(org.id)
    team = await make_team(user)
    folder = await make_folder(owner)
    project = await make_project(owner, folder)
    # viewer via team, editor via organization — order of insertion is the
    # order LIMIT 1 tends to return
    await _grant(
        db_session,
        folder_id=folder.id,
        grantee_type="team",
        grantee_id=team.id,
        role_id=roles["folder-viewer"],
        granted_by=owner.id,
    )
    await _grant(
        db_session,
        folder_id=folder.id,
        grantee_type="organization",
        grantee_id=org.id,
        role_id=roles["folder-editor"],
        granted_by=owner.id,
    )

    write_resource = await find_resource(
        db_session, "project/{project_id}/layer", "POST"
    )
    assert await call_check_project(
        db_session,
        resource_id=write_resource,
        user_id=user.id,
        organization_id=org.id,
        project_ids=[project.id],
    ), "folder-editor reached via the organization must allow a write even though a viewer grant also exists"


@pytest.mark.asyncio
async def test_viewer_only_grant_still_refuses_writes(
    db_session: AsyncSession,
    roles: dict[str, UUID],
    make_user: Callable[..., Awaitable[User]],
    make_org: Callable[[], Awaitable[Organization]],
    make_team: Callable[..., Awaitable[Team]],
    make_folder: Callable[..., Awaitable[Folder]],
    make_project: Callable[..., Awaitable[Project]],
) -> None:
    org = await make_org()
    owner = await make_user(org.id)
    user = await make_user(org.id)
    team = await make_team(user)
    folder = await make_folder(owner)
    project = await make_project(owner, folder)
    await _grant(
        db_session,
        folder_id=folder.id,
        grantee_type="team",
        grantee_id=team.id,
        role_id=roles["folder-viewer"],
        granted_by=owner.id,
    )
    write_resource = await find_resource(
        db_session, "project/{project_id}/layer", "POST"
    )
    assert not await call_check_project(
        db_session,
        resource_id=write_resource,
        user_id=user.id,
        organization_id=org.id,
        project_ids=[project.id],
    )
