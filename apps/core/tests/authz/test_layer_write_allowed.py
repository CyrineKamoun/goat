"""``customer.layer_write_allowed``: space owner/admin, editor-or-owner via
``effective_role``, or the shared-workspace rule — a project containing the
layer where the requester holds project rank >= editor AND the layer's own
space (its actual owner) does too, directly or through one of that space's
admins. Project access alone never grants write, and a catalog layer with an
owner is writable by its space owner only — no grant path, including the
shared-workspace one, may raise it above viewer for anyone else."""

from collections.abc import Awaitable, Callable
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


async def allowed(db: AsyncSession, lid: UUID, uid: UUID) -> bool:
    return bool(
        (
            await db.execute(
                text(f"SELECT {S}.layer_write_allowed(:l, :u)"), {"l": lid, "u": uid}
            )
        ).scalar()
    )


@pytest.mark.asyncio
async def test_write_rule(
    db_session: AsyncSession,
    roles: dict[str, UUID],
    make_user: Callable[..., Awaitable[User]],
    make_org: Callable[[], Awaitable[Organization]],
    make_folder: Callable[..., Awaitable[Folder]],
    make_layer: Callable[..., Awaitable[Layer]],
    make_project: Callable[..., Awaitable[Project]],
) -> None:
    org = await make_org()
    owner, editor_via_project, viewer_via_project, laundry = [
        await make_user(org.id) for _ in range(4)
    ]
    folder = await make_folder(owner)
    layer = await make_layer(owner, folder)
    project = await make_project(owner, folder)
    await db_session.execute(
        text(
            f'INSERT INTO {S}.layer_project (layer_id, project_id, name, "order", updated_at) '
            "VALUES (:l, :p, 'l', 0, now())"
        ),
        {"l": layer.id, "p": project.id},
    )
    g = (
        f"INSERT INTO {S}.resource_grant (resource_type, resource_id, grantee_type, grantee_id, role_id, granted_by) "
        "VALUES ('project', :p, 'user', :u, :r, :o)"
    )
    await db_session.execute(
        text(g),
        {
            "p": project.id,
            "u": editor_via_project.id,
            "r": roles["project-editor"],
            "o": owner.id,
        },
    )
    await db_session.execute(
        text(g),
        {
            "p": project.id,
            "u": viewer_via_project.id,
            "r": roles["project-viewer"],
            "o": owner.id,
        },
    )

    assert await allowed(db_session, layer.id, owner.id)
    assert await allowed(
        db_session, layer.id, editor_via_project.id
    ), "owner and requester both edit a common project"
    assert not await allowed(db_session, layer.id, viewer_via_project.id)

    # escalation boundary: a stranger who owns a project and adds the layer to
    # it must NOT gain write — project access alone never grants write
    laundry_folder = await make_folder(laundry)
    theirs = await make_project(laundry, laundry_folder)
    await db_session.execute(
        text(
            f'INSERT INTO {S}.layer_project (layer_id, project_id, name, "order", updated_at) '
            "VALUES (:l, :p, 'l', 0, now())"
        ),
        {"l": layer.id, "p": theirs.id},
    )
    assert not await allowed(db_session, layer.id, laundry.id)


@pytest.mark.asyncio
async def test_catalog_layer_shared_workspace_grants_no_write(
    db_session: AsyncSession,
    roles: dict[str, UUID],
    make_user: Callable[..., Awaitable[User]],
    make_org: Callable[[], Awaitable[Organization]],
    make_folder: Callable[..., Awaitable[Folder]],
    make_layer: Callable[..., Awaitable[Layer]],
    make_project: Callable[..., Awaitable[Project]],
) -> None:
    """A catalog-flagged layer stays owner-only even through the
    shared-workspace path: the owner puts it in a project they own and
    grants a collaborator project-editor there, but the collaborator must
    not gain write on the catalog layer itself."""
    org = await make_org()
    owner, collaborator = await make_user(org.id), await make_user(org.id)
    folder = await make_folder(owner)
    layer = await make_layer(owner, folder)
    await db_session.execute(
        text(f"UPDATE {S}.layer SET in_catalog = TRUE WHERE id = :l"),
        {"l": layer.id},
    )
    project = await make_project(owner, folder)
    await db_session.execute(
        text(
            f'INSERT INTO {S}.layer_project (layer_id, project_id, name, "order", updated_at) '
            "VALUES (:l, :p, 'l', 0, now())"
        ),
        {"l": layer.id, "p": project.id},
    )
    await db_session.execute(
        text(
            f"INSERT INTO {S}.resource_grant (resource_type, resource_id, grantee_type, grantee_id, role_id, granted_by) "
            "VALUES ('project', :p, 'user', :u, :r, :o)"
        ),
        {
            "p": project.id,
            "u": collaborator.id,
            "r": roles["project-editor"],
            "o": owner.id,
        },
    )

    assert await allowed(db_session, layer.id, owner.id)
    assert not await allowed(db_session, layer.id, collaborator.id)


@pytest.mark.asyncio
async def test_direct_and_folder_grants_reach_the_write_rule(
    db_session: AsyncSession,
    roles: dict[str, UUID],
    make_user: Callable[..., Awaitable[User]],
    make_org: Callable[[], Awaitable[Organization]],
    make_folder: Callable[..., Awaitable[Folder]],
    make_layer: Callable[..., Awaitable[Layer]],
) -> None:
    """The ``effective_role('layer', ...)`` branch: a direct editor grant on
    the layer admits write with no shared project involved at all; a direct
    viewer grant does not; a folder-editor grant on the layer's folder also
    admits write."""
    org = await make_org()
    owner, direct_editor, direct_viewer, folder_editor = [
        await make_user(org.id) for _ in range(4)
    ]
    folder = await make_folder(owner)
    layer = await make_layer(owner, folder)
    g = (
        f"INSERT INTO {S}.resource_grant (resource_type, resource_id, grantee_type, grantee_id, role_id, granted_by) "
        "VALUES (:t, :r, 'user', :u, :role, :o)"
    )
    await db_session.execute(
        text(g),
        {
            "t": "layer",
            "r": layer.id,
            "u": direct_editor.id,
            "role": roles["layer-editor"],
            "o": owner.id,
        },
    )
    await db_session.execute(
        text(g),
        {
            "t": "layer",
            "r": layer.id,
            "u": direct_viewer.id,
            "role": roles["layer-viewer"],
            "o": owner.id,
        },
    )
    await db_session.execute(
        text(g),
        {
            "t": "folder",
            "r": folder.id,
            "u": folder_editor.id,
            "role": roles["folder-editor"],
            "o": owner.id,
        },
    )

    assert await allowed(db_session, layer.id, direct_editor.id)
    assert not await allowed(db_session, layer.id, direct_viewer.id)
    assert await allowed(db_session, layer.id, folder_editor.id)


@pytest.mark.asyncio
async def test_team_space_layer_write_follows_the_space_not_the_stale_creator(
    db_session: AsyncSession,
    make_user: Callable[..., Awaitable[User]],
    make_org: Callable[[], Awaitable[Organization]],
    make_team: Callable[..., Awaitable[Team]],
    make_space: Callable[..., Awaitable[Space]],
    make_folder: Callable[..., Awaitable[Folder]],
    make_layer: Callable[..., Awaitable[Layer]],
    make_project: Callable[..., Awaitable[Project]],
) -> None:
    """A layer's write rule follows its space, not the frozen `user_id`
    "created by" column (Task 1): once the layer and a project containing it
    both sit in the team's space, a team member who never created either can
    write the layer through plain space membership — the creator, who has
    never joined the team, cannot, even though `layer.user_id` still names
    them."""
    org = await make_org()
    creator, member = await make_user(org.id), await make_user(org.id)
    team = await make_team(member, org=org)  # creator is NOT in the team
    space = await make_space(SpaceKind.team, team=team)  # default editor
    folder = await make_folder(creator)
    layer = await make_layer(creator, folder)
    project = await make_project(creator, folder)
    await db_session.execute(
        text(
            f'INSERT INTO {S}.layer_project (layer_id, project_id, name, "order", updated_at) '
            "VALUES (:l, :p, 'l', 0, now())"
        ),
        {"l": layer.id, "p": project.id},
    )
    for table, rid in (
        ("folder", folder.id),
        ("layer", layer.id),
        ("project", project.id),
    ):
        await db_session.execute(
            text(f"UPDATE {S}.{table} SET space_id = :s WHERE id = :r"),
            {"s": space.id, "r": rid},
        )

    assert await allowed(db_session, layer.id, member.id)
    assert not await allowed(db_session, layer.id, creator.id)


@pytest.mark.asyncio
async def test_layer_write_survives_offboarding_of_the_creator(
    db_session: AsyncSession,
    roles: dict[str, UUID],
    make_user: Callable[..., Awaitable[User]],
    make_org: Callable[[], Awaitable[Organization]],
    make_team: Callable[..., Awaitable[Team]],
    make_space: Callable[..., Awaitable[Space]],
    make_folder: Callable[..., Awaitable[Folder]],
    make_layer: Callable[..., Awaitable[Layer]],
) -> None:
    """Offboarding nulls a team-space layer's user_id but must not turn it
    into a write-locked catalog row: the team-owner keeps owner-level write,
    and a plain team member keeps editor-level write through the space
    default — exactly as before the creator left."""
    org = await make_org()
    lead, member = await make_user(org.id), await make_user(org.id)
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
    await db_session.execute(
        text(f"UPDATE {S}.folder SET space_id = :s WHERE id = :f"),
        {"s": space.id, "f": folder.id},
    )
    await db_session.execute(
        text(f"UPDATE {S}.layer SET space_id = :s WHERE id = :l"),
        {"s": space.id, "l": layer.id},
    )

    await db_session.execute(
        text(f"UPDATE {S}.layer SET user_id = NULL WHERE id = :l"), {"l": layer.id}
    )

    assert await allowed(db_session, layer.id, lead.id), "team owner keeps write"
    assert await allowed(db_session, layer.id, member.id), "team editor keeps write"


@pytest.mark.asyncio
async def test_genuinely_orphaned_layer_is_never_writable(
    db_session: AsyncSession,
    make_user: Callable[..., Awaitable[User]],
) -> None:
    """A layer with neither a space nor a user (and no catalog flags) is a
    true orphan — nobody, including its would-be creator, can write it."""
    someone = await make_user()
    layer_id = (
        await db_session.execute(
            text(
                f"INSERT INTO {S}.layer "
                "(id, user_id, folder_id, space_id, name, type, feature_layer_type, "
                " feature_layer_geometry_type, updated_at) "
                "VALUES (gen_random_uuid(), NULL, NULL, NULL, 'orphan', 'feature', "
                " 'standard', 'polygon', now()) RETURNING id"
            )
        )
    ).scalar_one()
    assert not await allowed(db_session, layer_id, someone.id)


@pytest.mark.asyncio
async def test_stranger_project_with_stale_owner_as_editor_cannot_launder_write(
    db_session: AsyncSession,
    roles: dict[str, UUID],
    make_user: Callable[..., Awaitable[User]],
    make_org: Callable[[], Awaitable[Organization]],
    make_team: Callable[..., Awaitable[Team]],
    make_space: Callable[..., Awaitable[Space]],
    make_folder: Callable[..., Awaitable[Folder]],
    make_layer: Callable[..., Awaitable[Layer]],
    make_project: Callable[..., Awaitable[Project]],
) -> None:
    """The shared-workspace rule anchors the owner side on the layer's own
    SPACE, not on `layer.user_id`. A stranger's own personal project — where
    the stranger is trivially editor-or-owner — must not admit write on a
    team-space layer merely because the layer's stale creator (no longer, or
    never, part of the team) happens to also hold an editor grant there: no
    admin of the layer's actual space (the team-owner) has any access to
    that project, so the owner side of the rule fails and the write is
    refused."""
    org = await make_org()
    creator, stranger = await make_user(org.id), await make_user(org.id)
    team = await make_team(
        await make_user(org.id), org=org
    )  # creator is NOT in the team
    space = await make_space(SpaceKind.team, team=team)
    folder = await make_folder(creator)
    layer = await make_layer(creator, folder)
    await db_session.execute(
        text(f"UPDATE {S}.layer SET space_id = :s WHERE id = :l"),
        {"s": space.id, "l": layer.id},
    )

    strangers_project = await make_project(stranger, await make_folder(stranger))
    await db_session.execute(
        text(
            f'INSERT INTO {S}.layer_project (layer_id, project_id, name, "order", updated_at) '
            "VALUES (:l, :p, 'l', 0, now())"
        ),
        {"l": layer.id, "p": strangers_project.id},
    )
    # The creator holds an editor grant on the stranger's project for reasons
    # unrelated to the layer or the team — exactly the coincidence the old,
    # user_id-anchored rule could have laundered into write access.
    await db_session.execute(
        text(
            f"INSERT INTO {S}.resource_grant (resource_type, resource_id, grantee_type, grantee_id, role_id, granted_by) "
            "VALUES ('project', :p, 'user', :u, :r, :o)"
        ),
        {
            "p": strangers_project.id,
            "u": creator.id,
            "r": roles["project-editor"],
            "o": stranger.id,
        },
    )

    assert not await allowed(db_session, layer.id, stranger.id)
