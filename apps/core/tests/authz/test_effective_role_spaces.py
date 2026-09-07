"""Space membership is the first rule: owner/admin -> owner; member -> the
space default; grants add; trash hides. Also: a non-catalog item whose
space_id is NULL (orphaned — its owning row is gone) is unreachable for
everyone, since only a space or a grant can reach it and it has neither."""

from typing import Awaitable, Callable
from uuid import UUID

import pytest
from core.core.config import settings
from core.db.models.folder import Folder
from core.db.models.layer import Layer
from core.db.models.organization import Organization
from core.db.models.project import Project
from core.db.models.space import Space, SpaceKind
from core.db.models.team import Team
from core.db.models.user import User
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

S = settings.SCHEMA


async def role(db: AsyncSession, rtype: str, rid: UUID, uid: UUID | None) -> str | None:
    return (
        await db.execute(
            text(f"SELECT {S}.effective_role(:t, :r, :u)"),
            {"t": rtype, "r": rid, "u": uid},
        )
    ).scalar()


async def move_to_space(
    db: AsyncSession, table: str, rid: UUID, space_id: UUID
) -> None:
    await db.execute(
        text(f"UPDATE {S}.{table} SET space_id = :s WHERE id = :r"),
        {"s": space_id, "r": rid},
    )


@pytest.mark.asyncio
async def test_team_space_owner_and_member_and_outsider(
    db_session: AsyncSession,
    roles: dict[str, UUID],
    make_user: Callable[..., Awaitable[User]],
    make_org: Callable[[], Awaitable[Organization]],
    make_team: Callable[..., Awaitable[Team]],
    make_space: Callable[..., Awaitable[Space]],
    make_folder: Callable[..., Awaitable[Folder]],
    make_layer: Callable[..., Awaitable[Layer]],
) -> None:
    org = await make_org()
    lead, member, outsider = [await make_user(org.id) for _ in range(3)]
    team = await make_team(lead, member, org=org)
    await db_session.execute(
        text(
            f"UPDATE {S}.user_team SET role_id = :r WHERE team_id = :t AND user_id = :u"
        ),
        {"r": roles["team-owner"], "t": team.id, "u": lead.id},
    )
    space = await make_space(SpaceKind.team, team=team)  # default editor
    folder = await make_folder(lead)
    layer = await make_layer(lead, folder)
    await move_to_space(db_session, "folder", folder.id, space.id)
    await move_to_space(db_session, "layer", layer.id, space.id)

    assert (
        await role(db_session, "layer", layer.id, lead.id) == "owner"
    ), "team owner = space admin"
    assert (
        await role(db_session, "layer", layer.id, member.id) == "editor"
    ), "D8: team default is editor"
    assert await role(db_session, "layer", layer.id, outsider.id) is None
    assert await role(db_session, "folder", folder.id, member.id) == "editor"

    await db_session.execute(
        text(f"UPDATE {S}.space SET default_role = 'viewer' WHERE id = :s"),
        {"s": space.id},
    )
    assert await role(db_session, "layer", layer.id, member.id) == "viewer"


@pytest.mark.asyncio
async def test_creator_keeps_nothing_special_after_transfer(
    db_session: AsyncSession,
    make_user: Callable[..., Awaitable[User]],
    make_org: Callable[[], Awaitable[Organization]],
    make_team: Callable[..., Awaitable[Team]],
    make_space: Callable[..., Awaitable[Space]],
    make_folder: Callable[..., Awaitable[Folder]],
    make_project: Callable[..., Awaitable[Project]],
) -> None:
    org = await make_org()
    creator, member = await make_user(org.id), await make_user(org.id)
    team = await make_team(member, org=org)  # creator is NOT in the team
    space = await make_space(SpaceKind.team, team=team)
    project = await make_project(creator, await make_folder(creator))
    await move_to_space(db_session, "project", project.id, space.id)
    assert (
        await role(db_session, "project", project.id, creator.id) is None
    ), "user_id is only 'created by'"
    assert await role(db_session, "project", project.id, member.id) == "editor"


@pytest.mark.asyncio
async def test_organisation_space_defaults_to_viewer_and_admins_own(
    db_session: AsyncSession,
    roles: dict[str, UUID],
    make_user: Callable[..., Awaitable[User]],
    make_org: Callable[[], Awaitable[Organization]],
    make_space: Callable[..., Awaitable[Space]],
    make_folder: Callable[..., Awaitable[Folder]],
    make_layer: Callable[..., Awaitable[Layer]],
) -> None:
    org = await make_org()
    admin, colleague = await make_user(org.id), await make_user(org.id)
    await db_session.execute(
        text(f"INSERT INTO {S}.user_role (user_id, role_id) VALUES (:u, :r)"),
        {"u": admin.id, "r": roles["organization-admin"]},
    )
    space = await make_space(SpaceKind.organization, org=org)
    layer = await make_layer(colleague, await make_folder(colleague))
    await move_to_space(db_session, "layer", layer.id, space.id)
    assert await role(db_session, "layer", layer.id, admin.id) == "owner"
    assert await role(db_session, "layer", layer.id, colleague.id) == "viewer"


@pytest.mark.asyncio
async def test_grants_still_add_on_top_of_the_space_default(
    db_session: AsyncSession,
    roles: dict[str, UUID],
    make_user: Callable[..., Awaitable[User]],
    make_org: Callable[[], Awaitable[Organization]],
    make_space: Callable[..., Awaitable[Space]],
    make_folder: Callable[..., Awaitable[Folder]],
    make_layer: Callable[..., Awaitable[Layer]],
) -> None:
    org = await make_org()
    owner, colleague = await make_user(org.id), await make_user(org.id)
    space = await make_space(SpaceKind.organization, org=org)  # viewer default
    layer = await make_layer(owner, await make_folder(owner))
    await move_to_space(db_session, "layer", layer.id, space.id)
    await db_session.execute(
        text(
            f"INSERT INTO {S}.resource_grant (resource_type, resource_id, grantee_type, grantee_id, role_id, granted_by) "
            "VALUES ('layer', :l, 'user', :u, :r, :o)"
        ),
        {"l": layer.id, "u": colleague.id, "r": roles["layer-editor"], "o": owner.id},
    )
    assert await role(db_session, "layer", layer.id, colleague.id) == "editor"


@pytest.mark.asyncio
async def test_folder_grant_walks_up_three_levels(
    db_session: AsyncSession,
    roles: dict[str, UUID],
    make_user: Callable[..., Awaitable[User]],
    make_org: Callable[[], Awaitable[Organization]],
    make_folder: Callable[..., Awaitable[Folder]],
    make_layer: Callable[..., Awaitable[Layer]],
) -> None:
    org = await make_org()
    owner, colleague = await make_user(org.id), await make_user(org.id)
    top = await make_folder(owner, "top")
    mid = await make_folder(owner, "mid")
    leaf = await make_folder(owner, "leaf")
    await db_session.execute(
        text(f"UPDATE {S}.folder SET parent_id = :p WHERE id = :c"),
        {"p": top.id, "c": mid.id},
    )
    await db_session.execute(
        text(f"UPDATE {S}.folder SET parent_id = :p WHERE id = :c"),
        {"p": mid.id, "c": leaf.id},
    )
    layer = await make_layer(owner, leaf)
    assert await role(db_session, "layer", layer.id, colleague.id) is None
    await db_session.execute(
        text(
            f"INSERT INTO {S}.resource_grant (resource_type, resource_id, grantee_type, grantee_id, role_id, granted_by) "
            "VALUES ('folder', :f, 'user', :u, :r, :o)"
        ),
        {"f": top.id, "u": colleague.id, "r": roles["folder-viewer"], "o": owner.id},
    )
    assert (
        await role(db_session, "layer", layer.id, colleague.id) == "viewer"
    ), "grant on the grandparent folder reaches the layer"
    assert await role(db_session, "folder", leaf.id, colleague.id) == "viewer"


@pytest.mark.asyncio
async def test_trash_is_visible_to_the_space_owner_only(
    db_session: AsyncSession,
    roles: dict[str, UUID],
    make_user: Callable[..., Awaitable[User]],
    make_org: Callable[[], Awaitable[Organization]],
    make_folder: Callable[..., Awaitable[Folder]],
    make_layer: Callable[..., Awaitable[Layer]],
) -> None:
    org = await make_org()
    owner, colleague = await make_user(org.id), await make_user(org.id)
    layer = await make_layer(owner, await make_folder(owner))
    await db_session.execute(
        text(
            f"INSERT INTO {S}.resource_grant (resource_type, resource_id, grantee_type, grantee_id, role_id, granted_by) "
            "VALUES ('layer', :l, 'user', :u, :r, :o)"
        ),
        {"l": layer.id, "u": colleague.id, "r": roles["layer-editor"], "o": owner.id},
    )
    assert await role(db_session, "layer", layer.id, colleague.id) == "editor"
    await db_session.execute(
        text(f"UPDATE {S}.layer SET deleted_at = now() WHERE id = :l"), {"l": layer.id}
    )
    assert (
        await role(db_session, "layer", layer.id, owner.id) == "owner"
    ), "the owner can still restore"
    assert (
        await role(db_session, "layer", layer.id, colleague.id) is None
    ), "nobody else sees trashed content"
    assert await role(db_session, "layer", layer.id, None) is None


@pytest.mark.asyncio
async def test_layer_losing_user_id_stays_in_its_team_space_not_catalog(
    db_session: AsyncSession,
    roles: dict[str, UUID],
    make_user: Callable[..., Awaitable[User]],
    make_org: Callable[[], Awaitable[Organization]],
    make_team: Callable[..., Awaitable[Team]],
    make_space: Callable[..., Awaitable[Space]],
    make_folder: Callable[..., Awaitable[Folder]],
    make_layer: Callable[..., Awaitable[Layer]],
) -> None:
    """Offboarding nulls a layer's user_id ("created by") but leaves
    space_id untouched. layer_is_catalog must not key off user_id alone —
    otherwise the layer would turn into a public, write-locked catalog row
    the moment its creator's account is removed."""
    org = await make_org()
    lead, member = await make_user(org.id), await make_user(org.id)
    team = await make_team(lead, member, org=org)
    await db_session.execute(
        text(
            f"UPDATE {S}.user_team SET role_id = :r WHERE team_id = :t AND user_id = :u"
        ),
        {"r": roles["team-owner"], "t": team.id, "u": lead.id},
    )
    space = await make_space(SpaceKind.team, team=team)
    folder = await make_folder(lead)
    layer = await make_layer(lead, folder)
    await move_to_space(db_session, "folder", folder.id, space.id)
    await move_to_space(db_session, "layer", layer.id, space.id)

    await db_session.execute(
        text(f"UPDATE {S}.layer SET user_id = NULL WHERE id = :l"), {"l": layer.id}
    )

    assert await role(db_session, "layer", layer.id, lead.id) == "owner"
    assert await role(db_session, "layer", layer.id, member.id) == "editor"
    assert await role(db_session, "layer", layer.id, None) is None


@pytest.mark.asyncio
async def test_orphaned_item_with_no_space_is_unreachable_for_everyone(
    db_session: AsyncSession,
    make_user: Callable[..., Awaitable[User]],
) -> None:
    """A non-catalog item whose space_id is NULL (its owning row is gone) has
    no owner left to answer for it; only a grant could still reach it, and
    there is none here, so effective_role is NULL for anyone, including the
    would-be creator. Uses a project rather than a layer: layer_is_catalog
    treats a NULL space_id AND NULL user_id together as the orphan/catalog
    placeholder, which would mask this case if projects went through it."""
    someone = await make_user()
    folder_id = (
        await db_session.execute(
            text(
                f"INSERT INTO {S}.folder (id, user_id, space_id, name, updated_at) VALUES (gen_random_uuid(), NULL, NULL, 'orphan-folder', now()) RETURNING id"
            )
        )
    ).scalar_one()
    project_id = (
        await db_session.execute(
            text(
                f"INSERT INTO {S}.project (id, user_id, space_id, folder_id, name, updated_at) "
                "VALUES (gen_random_uuid(), NULL, NULL, :f, 'orphan-project', now()) RETURNING id"
            ),
            {"f": folder_id},
        )
    ).scalar_one()
    assert await role(db_session, "project", project_id, someone.id) is None
    assert await role(db_session, "project", project_id, None) is None
