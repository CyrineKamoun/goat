"""Listings (crud_project.get_projects, crud_layer.get_layers_with_filter)
and `get_my_role` must read `resource_grant`, not the legacy
`project_team`/`layer_team`/etc. link tables — including the `shared_with`
display a resource's owner sees in "My Content"."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from uuid import UUID

import pytest
from core.core.config import settings
from core.crud.crud_layer import layer as crud_layer
from core.crud.crud_project import project as crud_project
from core.db.models.folder import Folder
from core.db.models.layer import Layer
from core.db.models.organization import Organization
from core.db.models.project import Project
from core.db.models.team import Team
from core.db.models.user import User
from core.schemas.layer import ILayerGet
from fastapi_pagination import Params as PaginationParams
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

S = settings.SCHEMA

PAGE = PaginationParams(page=1, size=50)


@pytest.mark.asyncio
async def test_get_projects_in_team_context_uses_grants(
    db_session: AsyncSession,
    roles: dict[str, UUID],
    make_user: Callable[..., Awaitable[User]],
    make_org: Callable[[], Awaitable[Organization]],
    make_team: Callable[..., Awaitable[Team]],
    make_folder: Callable[..., Awaitable[Folder]],
    make_project: Callable[[User, Folder], Awaitable[Project]],
) -> None:
    """A project shared with a team via `resource_grant` shows up in that
    team's listing, and `get_my_role` resolves the grant's role."""
    org = await make_org()
    owner, member = await make_user(org.id), await make_user(org.id)
    team = await make_team(member)
    project = await make_project(owner, await make_folder(owner))
    await db_session.execute(
        text(
            f"INSERT INTO {S}.resource_grant "
            "(resource_type, resource_id, grantee_type, grantee_id, role_id, granted_by) "
            "VALUES ('project', :p, 'team', :t, :r, :o)"
        ),
        {"p": project.id, "t": team.id, "r": roles["project-viewer"], "o": owner.id},
    )
    await db_session.commit()

    page = await crud_project.get_projects(
        db_session,
        user_id=member.id,
        team_id=team.id,
        order_by="updated_at",
        order="descendent",
        page_params=PAGE,
    )
    ids = {item["id"] for item in page.items}
    assert project.id in ids
    assert (
        await crud_project.get_my_role(
            db_session, project_id=project.id, user_id=member.id
        )
        == "project-viewer"
    )


@pytest.mark.asyncio
async def test_get_projects_in_team_context_ignores_other_teams_grant(
    db_session: AsyncSession,
    roles: dict[str, UUID],
    make_user: Callable[..., Awaitable[User]],
    make_org: Callable[[], Awaitable[Organization]],
    make_team: Callable[..., Awaitable[Team]],
    make_folder: Callable[..., Awaitable[Folder]],
    make_project: Callable[[User, Folder], Awaitable[Project]],
) -> None:
    """A project shared with a different team is not visible for this team,
    and `get_my_role` returns None for a user with no path to the project."""
    org = await make_org()
    owner, member = await make_user(org.id), await make_user(org.id)
    other_member = await make_user(org.id)
    team = await make_team(member)
    other_team = await make_team(other_member)
    project = await make_project(owner, await make_folder(owner))
    await db_session.execute(
        text(
            f"INSERT INTO {S}.resource_grant "
            "(resource_type, resource_id, grantee_type, grantee_id, role_id, granted_by) "
            "VALUES ('project', :p, 'team', :t, :r, :o)"
        ),
        {
            "p": project.id,
            "t": other_team.id,
            "r": roles["project-viewer"],
            "o": owner.id,
        },
    )
    await db_session.commit()

    page = await crud_project.get_projects(
        db_session,
        user_id=member.id,
        team_id=team.id,
        order_by="updated_at",
        order="descendent",
        page_params=PAGE,
    )
    ids = {item["id"] for item in page.items}
    assert project.id not in ids
    assert (
        await crud_project.get_my_role(
            db_session, project_id=project.id, user_id=member.id
        )
        is None
    )


@pytest.mark.asyncio
async def test_get_my_role_none_without_any_grant_path(
    db_session: AsyncSession,
    roles: dict[str, UUID],
    make_user: Callable[..., Awaitable[User]],
    make_folder: Callable[..., Awaitable[Folder]],
    make_project: Callable[[User, Folder], Awaitable[Project]],
) -> None:
    """A user with no ownership, grant, team, org, or folder path gets None,
    not an owner/editor/viewer role."""
    owner = await make_user()
    outsider = await make_user()
    project = await make_project(owner, await make_folder(owner))
    await db_session.commit()

    assert (
        await crud_project.get_my_role(
            db_session, project_id=project.id, user_id=outsider.id
        )
        is None
    )


@pytest.mark.asyncio
async def test_owner_listing_reports_team_grant_in_shared_with(
    db_session: AsyncSession,
    roles: dict[str, UUID],
    make_user: Callable[..., Awaitable[User]],
    make_org: Callable[[], Awaitable[Organization]],
    make_team: Callable[..., Awaitable[Team]],
    make_folder: Callable[..., Awaitable[Folder]],
    make_layer: Callable[[User, Folder], Awaitable[Layer]],
) -> None:
    """A layer shared with a team via a direct `resource_grant` insert shows
    that team under `shared_with.teams` when its owner lists "My Content"
    (no team/org context) — the `create_query_shared_content` no-context
    branch and `fetch_grants_by_resource` must read `resource_grant`, not
    the legacy `layer_team` table, which never gets a row here."""
    org = await make_org()
    owner = await make_user(org.id)
    team = await make_team(owner)
    layer = await make_layer(owner, await make_folder(owner))
    await db_session.execute(
        text(
            f"INSERT INTO {S}.resource_grant "
            "(resource_type, resource_id, grantee_type, grantee_id, role_id, granted_by) "
            "VALUES ('layer', :l, 'team', :t, :r, :o)"
        ),
        {"l": layer.id, "t": team.id, "r": roles["layer-viewer"], "o": owner.id},
    )
    await db_session.commit()

    page = await crud_layer.get_layers_with_filter(
        db_session,
        user_id=owner.id,
        order_by="updated_at",
        order="descendent",
        page_params=PAGE,
        params=ILayerGet(),
    )
    item = next(i for i in page.items if i["id"] == layer.id)
    teams = item["shared_with"]["teams"]
    assert {t["id"] for t in teams} == {team.id}
    assert teams[0]["role"] == "layer-viewer"
    assert teams[0]["name"] == team.name


@pytest.mark.asyncio
async def test_owner_listing_reports_user_grant_in_shared_with(
    db_session: AsyncSession,
    roles: dict[str, UUID],
    make_user: Callable[..., Awaitable[User]],
    make_folder: Callable[..., Awaitable[Folder]],
    make_layer: Callable[[User, Folder], Awaitable[Layer]],
) -> None:
    """A layer shared directly with a user (grantee_type='user') shows that
    user under `shared_with.users` in the owner's "My Content" listing."""
    owner = await make_user()
    grantee = await make_user()
    layer = await make_layer(owner, await make_folder(owner))
    await db_session.execute(
        text(
            f"INSERT INTO {S}.resource_grant "
            "(resource_type, resource_id, grantee_type, grantee_id, role_id, granted_by) "
            "VALUES ('layer', :l, 'user', :u, :r, :o)"
        ),
        {"l": layer.id, "u": grantee.id, "r": roles["layer-editor"], "o": owner.id},
    )
    await db_session.commit()

    page = await crud_layer.get_layers_with_filter(
        db_session,
        user_id=owner.id,
        order_by="updated_at",
        order="descendent",
        page_params=PAGE,
        params=ILayerGet(),
    )
    item = next(i for i in page.items if i["id"] == layer.id)
    users = item["shared_with"]["users"]
    assert {u["id"] for u in users} == {grantee.id}
    assert users[0]["role"] == "layer-editor"
    assert users[0]["name"] == f"{grantee.firstname} {grantee.lastname}".strip()


@pytest.mark.asyncio
async def test_team_context_listing_reports_correct_team_fields_in_shared_with(
    db_session: AsyncSession,
    roles: dict[str, UUID],
    make_user: Callable[..., Awaitable[User]],
    make_org: Callable[[], Awaitable[Organization]],
    make_team: Callable[..., Awaitable[Team]],
    make_folder: Callable[..., Awaitable[Folder]],
    make_layer: Callable[[User, Folder], Awaitable[Layer]],
) -> None:
    """Regression test for the column-index bug fixed in
    `build_shared_with_object`'s team/org branch: it previously read
    columns 2/3/4 (the resource OWNER's valid_user_id/firstname/lastname)
    into the team/org entry's id/name/avatar instead of the correct 7/6/8
    (the team's own id/name/avatar). Listing in team context must report
    the TEAM's fields, not the owner's."""
    org = await make_org()
    owner = await make_user(org.id)
    member = await make_user(org.id)
    team = await make_team(member)
    layer = await make_layer(owner, await make_folder(owner))
    await db_session.execute(
        text(
            f"INSERT INTO {S}.resource_grant "
            "(resource_type, resource_id, grantee_type, grantee_id, role_id, granted_by) "
            "VALUES ('layer', :l, 'team', :t, :r, :o)"
        ),
        {"l": layer.id, "t": team.id, "r": roles["layer-viewer"], "o": owner.id},
    )
    await db_session.commit()

    page = await crud_layer.get_layers_with_filter(
        db_session,
        user_id=member.id,
        order_by="updated_at",
        order="descendent",
        page_params=PAGE,
        params=ILayerGet(),
        team_id=team.id,
    )
    item = next(i for i in page.items if i["id"] == layer.id)
    team_entry = item["shared_with"]["teams"][0]
    assert team_entry["role"] == "layer-viewer"
    assert team_entry["id"] == team.id
    assert team_entry["name"] == team.name
    # The pre-fix bug put the owner's id/firstname where the team's
    # id/name belong — assert those wrong values do NOT show up here.
    assert team_entry["id"] != owner.id
    assert team_entry["name"] != owner.firstname
