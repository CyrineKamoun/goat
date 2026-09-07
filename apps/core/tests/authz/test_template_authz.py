"""Authorization for the `template` resource type (T1/T3/T4).

Templates plug into `effective_role` exactly like folders/projects/bundles —
space owner/admin, space default for members, Restricted (D9) suppresses the
default, direct grants still apply — plus one addition (T4): once
`catalog_status = 'published'`, every authenticated user gets at least
`viewer`, regardless of the space the template lives in.
"""

import json
from collections.abc import Awaitable, Callable
from typing import Any, cast
from uuid import UUID, uuid4

import pytest
from core.core import authz
from core.core.config import settings
from core.crud.crud_template import template as crud_template
from core.db.models.folder import Folder
from core.db.models.organization import Organization
from core.db.models.space import Space, SpaceDefaultRole, SpaceKind
from core.db.models.team import Team
from core.db.models.user import User
from core.db.models.workflow import Workflow
from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

S = settings.SCHEMA


async def _role(
    db: AsyncSession, rtype: str, rid: UUID, uid: UUID | None
) -> str | None:
    return (
        await db.execute(
            text(f"SELECT {S}.effective_role(:t, :r, :u)"),
            {"t": rtype, "r": rid, "u": uid},
        )
    ).scalar()


async def _make_template(
    db: AsyncSession,
    *,
    space_id: UUID,
    folder_id: UUID,
    creator: UUID | None,
    catalog_status: str = "none",
    restricted: bool = False,
) -> UUID:
    return (
        await db.execute(
            text(
                f"""
                INSERT INTO {S}.template
                    (id, name, space_id, folder_id, user_id, payload_kind,
                     catalog_status, restricted, updated_at)
                VALUES
                    (gen_random_uuid(), 't', :s, :f, :u, 'workflow', :cs, :r, now())
                RETURNING id
                """
            ),
            {
                "s": space_id,
                "f": folder_id,
                "u": creator,
                "cs": catalog_status,
                "r": restricted,
            },
        )
    ).scalar_one()


async def _team_space(
    db: AsyncSession,
    roles: dict[str, UUID],
    make_team: Callable[..., Awaitable[Team]],
    make_space: Callable[..., Awaitable[Space]],
    *,
    org: Organization,
    lead: User,
    member: User,
) -> Space:
    """A team space with the D8 editor default: `lead` is its admin
    (team-owner → space_rank 3), `member` a plain member."""
    team = await make_team(lead, member, org=org)
    await db.execute(
        text(
            f"UPDATE {S}.user_team SET role_id = :r WHERE team_id = :t AND user_id = :u"
        ),
        {"r": roles["team-owner"], "t": team.id, "u": lead.id},
    )
    space: Space = await make_space(
        SpaceKind.team, team=team, default_role=SpaceDefaultRole.editor
    )
    return space


async def _team_folder(db: AsyncSession, space_id: UUID, owner: UUID) -> UUID:
    """`make_folder` only targets personal spaces (see conftest); a
    team-space folder is hand-inserted here."""
    return (
        await db.execute(
            text(
                f"""
                INSERT INTO {S}.folder (id, name, user_id, space_id, updated_at)
                VALUES (gen_random_uuid(), :n, :u, :s, now())
                RETURNING id
                """
            ),
            {"n": f"f-{uuid4().hex[:6]}", "u": owner, "s": space_id},
        )
    ).scalar_one()


@pytest.mark.asyncio
async def test_creator_in_personal_space_is_owner(
    db_session: AsyncSession,
    authz_sql: None,
    make_user: Callable[..., Awaitable[User]],
    make_folder: Callable[..., Awaitable[Folder]],
) -> None:
    creator = await make_user()
    folder = await make_folder(creator)
    tid = await _make_template(
        db_session, space_id=folder.space_id, folder_id=folder.id, creator=creator.id
    )

    assert await _role(db_session, "template", tid, creator.id) == "owner"


@pytest.mark.asyncio
async def test_stranger_gets_no_access_while_unpublished(
    db_session: AsyncSession,
    authz_sql: None,
    make_user: Callable[..., Awaitable[User]],
    make_folder: Callable[..., Awaitable[Folder]],
) -> None:
    creator = await make_user()
    stranger = await make_user()
    folder = await make_folder(creator)
    tid = await _make_template(
        db_session, space_id=folder.space_id, folder_id=folder.id, creator=creator.id
    )

    assert await _role(db_session, "template", tid, stranger.id) is None


@pytest.mark.asyncio
async def test_stranger_gets_viewer_once_published_to_the_catalog(
    db_session: AsyncSession,
    authz_sql: None,
    make_user: Callable[..., Awaitable[User]],
    make_folder: Callable[..., Awaitable[Folder]],
) -> None:
    creator = await make_user()
    stranger = await make_user()
    folder = await make_folder(creator)
    tid = await _make_template(
        db_session,
        space_id=folder.space_id,
        folder_id=folder.id,
        creator=creator.id,
        catalog_status="published",
    )

    assert await _role(db_session, "template", tid, stranger.id) == "viewer"


@pytest.mark.asyncio
async def test_restricted_team_template_suppresses_member_but_not_team_owner(
    db_session: AsyncSession,
    roles: dict[str, UUID],
    make_user: Callable[..., Awaitable[User]],
    make_org: Callable[[], Awaitable[Organization]],
    make_team: Callable[..., Awaitable[Team]],
    make_space: Callable[..., Awaitable[Space]],
) -> None:
    org = await make_org()
    lead, member = await make_user(org.id), await make_user(org.id)
    space = await _team_space(
        db_session, roles, make_team, make_space, org=org, lead=lead, member=member
    )
    folder_id = await _team_folder(db_session, space.id, lead.id)
    tid = await _make_template(
        db_session,
        space_id=space.id,
        folder_id=folder_id,
        creator=lead.id,
        restricted=True,
    )

    assert await _role(db_session, "template", tid, member.id) is None
    assert await _role(db_session, "template", tid, lead.id) == "owner"


@pytest.mark.asyncio
async def test_published_template_keeps_a_member_editor_not_capped_to_viewer(
    db_session: AsyncSession,
    roles: dict[str, UUID],
    make_user: Callable[..., Awaitable[User]],
    make_org: Callable[[], Awaitable[Organization]],
    make_team: Callable[..., Awaitable[Team]],
    make_space: Callable[..., Awaitable[Space]],
) -> None:
    """The shelf is a fallback: a team member's editor default on the team's
    own published template still wins over the shelf's viewer."""
    org = await make_org()
    lead, member = await make_user(org.id), await make_user(org.id)
    stranger = await make_user()
    space = await _team_space(
        db_session, roles, make_team, make_space, org=org, lead=lead, member=member
    )
    folder_id = await _team_folder(db_session, space.id, lead.id)
    tid = await _make_template(
        db_session,
        space_id=space.id,
        folder_id=folder_id,
        creator=lead.id,
        catalog_status="published",
    )

    assert await _role(db_session, "template", tid, member.id) == "editor"
    assert await _role(db_session, "template", tid, stranger.id) == "viewer"


@pytest.mark.asyncio
async def test_authz_can_reads_a_template_without_raising(
    db_session: AsyncSession,
    authz_sql: None,
    make_user: Callable[..., Awaitable[User]],
    make_folder: Callable[..., Awaitable[Folder]],
) -> None:
    creator = await make_user()
    folder = await make_folder(creator)
    tid = await _make_template(
        db_session, space_id=folder.space_id, folder_id=folder.id, creator=creator.id
    )

    assert await authz.can(db_session, "template", tid, creator.id, "read") is True


async def _workflow_template(
    db: AsyncSession,
    *,
    space_id: UUID,
    folder_id: UUID,
    creator: UUID,
    source_project_id: UUID,
    source_workflow_id: UUID,
) -> UUID:
    """A workflow template whose `source_ref` points at someone else's project."""
    return cast(
        UUID,
        (
            await db.execute(
                text(
                    f"""
                    INSERT INTO {S}.template
                        (id, name, space_id, folder_id, user_id, payload_kind,
                         catalog_status, restricted, source_ref, inputs,
                         config, updated_at)
                    VALUES
                        (gen_random_uuid(), 'borrowed', :s, :f, :u, 'workflow',
                         'none', false, CAST(:sr AS jsonb), CAST('[]' AS jsonb),
                         CAST('{{}}' AS jsonb), now())
                    RETURNING id
                    """
                ),
                {
                    "s": space_id,
                    "f": folder_id,
                    "u": creator,
                    "sr": json.dumps(
                        {
                            "kind": "workflow",
                            "project_id": str(source_project_id),
                            "workflow_id": str(source_workflow_id),
                        }
                    ),
                },
            )
        ).scalar_one(),
    )


@pytest.mark.asyncio
async def test_refresh_requires_read_on_the_source_project(
    db_session: AsyncSession,
    authz_sql: None,
    roles: dict[str, UUID],
    make_user: Callable[..., Awaitable[User]],
    make_org: Callable[[], Awaitable[Organization]],
    make_folder: Callable[..., Awaitable[Folder]],
    make_project: Callable[..., Awaitable[Any]],
) -> None:
    """S5: `refresh` re-snapshots the source project, so it needs `read` on it.

    Owning the template only clears the `delete` rank the route itself asks
    for — without the source check, a template owner could pull the current
    node graph, labels and tool params out of a private project he holds no
    role on and then read them back with `include_config=true`.
    """
    org = await make_org()
    author = await make_user(org.id)
    borrower = await make_user(org.id)

    author_folder = await make_folder(author, "Theirs")
    source = await make_project(author, author_folder)
    workflow = Workflow(
        project_id=source.id,
        name="Private pipeline",
        config={"nodes": [], "edges": []},
    )
    db_session.add(workflow)
    await db_session.flush()
    assert workflow.id is not None

    borrower_folder = await make_folder(borrower, "Mine")
    tid = await _workflow_template(
        db_session,
        space_id=borrower_folder.space_id,
        folder_id=borrower_folder.id,
        creator=borrower.id,
        source_project_id=source.id,
        source_workflow_id=workflow.id,
    )
    await db_session.commit()

    # The template is his own (owner ⇒ clears `delete`) ...
    assert await authz.can(db_session, "template", tid, borrower.id, "delete") is True
    # ... but the source project is not readable to him, so refresh refuses.
    assert await authz.can(db_session, "project", source.id, borrower.id, "read") is (
        False
    )
    with pytest.raises(HTTPException) as refused:
        await crud_template.refresh(db_session, template_id=tid, user_id=borrower.id)
    assert refused.value.status_code == 403

    # Granted read on the source, the same call goes through.
    await db_session.execute(
        text(
            f"""
            INSERT INTO {S}.resource_grant
                (resource_type, resource_id, grantee_type, grantee_id,
                 role_id, granted_by)
            VALUES ('project', :r, 'user', :g, :role, :by)
            """
        ),
        {
            "r": source.id,
            "g": borrower.id,
            "role": roles["project-viewer"],
            "by": author.id,
        },
    )
    await db_session.commit()
    assert (
        await authz.can(db_session, "project", source.id, borrower.id, "read") is True
    )
    refreshed = await crud_template.refresh(
        db_session, template_id=tid, user_id=borrower.id
    )
    assert refreshed.id == tid
