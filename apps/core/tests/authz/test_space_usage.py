"""Space storage usage (spec D13) and the "personally owned datasets in this
project" health count that the Home page shows on team/organisation projects.

``GET /space/{space_id}/usage`` totals live (``deleted_at IS NULL``) rows only
and is gated to the space's own members (``space_rank >= 1``); a non-member
gets the same 404 a nonexistent space would, so membership is never revealed
by the status code. ``IProjectRead.personally_owned_layer_count`` counts a
project's linked live layers that live in a *personal* space — ``None`` for a
project that is itself in a personal space.
"""

from collections.abc import Awaitable, Callable
from uuid import UUID, uuid4

import pytest
from core.core.config import settings
from core.db.models.folder import Folder
from core.db.models.layer import Layer
from core.db.models.organization import Organization
from core.db.models.project import Project
from core.db.models.space import Space, SpaceDefaultRole, SpaceKind
from core.db.models.team import Team
from core.db.models.user import User
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from tests.authz.conftest import find_resource
from tests.authz.test_content_feed import _unverified_bearer

S = settings.SCHEMA


def _bearer(user_id: UUID) -> dict[str, str]:
    return {"Authorization": f"Bearer {_unverified_bearer(user_id)}"}


async def _move_to_space(
    db: AsyncSession, table: str, rid: UUID, space_id: UUID
) -> None:
    await db.execute(
        text(f"UPDATE {S}.{table} SET space_id = :s WHERE id = :r"),
        {"s": space_id, "r": rid},
    )


async def _set_size(db: AsyncSession, layer_id: UUID, size: int) -> None:
    await db.execute(
        text(f"UPDATE {S}.layer SET size = :s WHERE id = :l"),
        {"s": size, "l": layer_id},
    )


async def _trash(db: AsyncSession, layer_id: UUID) -> None:
    await db.execute(
        text(f"UPDATE {S}.layer SET deleted_at = now() WHERE id = :l"),
        {"l": layer_id},
    )


async def _team_space(
    db: AsyncSession,
    roles: dict[str, UUID],
    make_team: Callable[..., Awaitable[Team]],
    make_space: Callable[..., Awaitable[Space]],
    *,
    org: Organization,
    lead: User,
    members: list[User],
) -> tuple[Space, Team]:
    """A team space with `lead` as its admin (team-owner -> space_rank 3)."""
    team = await make_team(lead, *members, org=org)
    await db.execute(
        text(
            f"UPDATE {S}.user_team SET role_id = :r WHERE team_id = :t AND user_id = :u"
        ),
        {"r": roles["team-owner"], "t": team.id, "u": lead.id},
    )
    space: Space = await make_space(
        SpaceKind.team, team=team, default_role=SpaceDefaultRole.editor
    )
    return space, team


async def _authorized(db: AsyncSession, uid: UUID, pattern: str, path: str) -> bool:
    """`customer.authorization` — the URL gate `auth_z` runs. It raises rather
    than returning FALSE when it refuses, so a raise counts as refusal (the
    engine is AUTOCOMMIT, so a plain rollback clears the session bookkeeping)."""
    from sqlalchemy.exc import DBAPIError

    try:
        return bool(
            (
                await db.execute(
                    text(f"SELECT {S}.authorization(:u, :res, :path, :method)"),
                    {"u": uid, "res": pattern, "path": path, "method": "GET"},
                )
            ).scalar()
        )
    except DBAPIError:
        await db.rollback()
        return False


@pytest.mark.asyncio
async def test_usage_sums_live_layers_and_counts_projects_in_a_team_space(
    client: AsyncClient,
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
    org = await make_org()
    lead = await make_user(org.id)
    member = await make_user(org.id)
    space, _ = await _team_space(
        db_session, roles, make_team, make_space, org=org, lead=lead, members=[member]
    )

    folder = await make_folder(lead, "Team")
    await _move_to_space(db_session, "folder", folder.id, space.id)

    layer_a = await make_layer(lead, folder)
    layer_b = await make_layer(lead, folder)
    trashed = await make_layer(lead, folder)
    for layer in (layer_a, layer_b, trashed):
        await _move_to_space(db_session, "layer", layer.id, space.id)
    await _set_size(db_session, layer_a.id, 1000)
    await _set_size(db_session, layer_b.id, 2000)
    await _set_size(db_session, trashed.id, 999_999)
    await _trash(db_session, trashed.id)

    project = await make_project(lead, folder)
    await _move_to_space(db_session, "project", project.id, space.id)
    await db_session.commit()

    r = await client.get(
        f"{settings.API_V2_STR}/space/{space.id}/usage", headers=_bearer(member.id)
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["space_id"] == str(space.id)
    assert body["bytes"] == 3000, "the trashed layer's size must not count"
    assert body["layers"] == 2, "the trashed layer must not be counted"
    assert body["projects"] == 1


@pytest.mark.asyncio
async def test_usage_requires_membership(
    client: AsyncClient,
    db_session: AsyncSession,
    roles: dict[str, UUID],
    make_user: Callable[..., Awaitable[User]],
    make_org: Callable[[], Awaitable[Organization]],
    make_team: Callable[..., Awaitable[Team]],
    make_space: Callable[..., Awaitable[Space]],
) -> None:
    org = await make_org()
    lead = await make_user(org.id)
    space, _ = await _team_space(
        db_session, roles, make_team, make_space, org=org, lead=lead, members=[]
    )
    await db_session.commit()

    stranger_org = await make_org()
    stranger = await make_user(stranger_org.id)

    r = await client.get(
        f"{settings.API_V2_STR}/space/{space.id}/usage", headers=_bearer(stranger.id)
    )
    assert r.status_code == 404, "a non-member must not learn the space exists"

    r_unknown = await client.get(
        f"{settings.API_V2_STR}/space/{uuid4()}/usage", headers=_bearer(stranger.id)
    )
    assert r_unknown.status_code == 404


@pytest.mark.asyncio
async def test_usage_of_a_personal_space_reflects_only_its_owner(
    client: AsyncClient,
    db_session: AsyncSession,
    roles: dict[str, UUID],
    make_user: Callable[..., Awaitable[User]],
    make_org: Callable[[], Awaitable[Organization]],
    make_folder: Callable[..., Awaitable[Folder]],
    make_layer: Callable[..., Awaitable[Layer]],
) -> None:
    org = await make_org()
    owner = await make_user(org.id)
    other = await make_user(org.id)

    folder = await make_folder(owner, "Mine")
    layer = await make_layer(owner, folder)
    await _set_size(db_session, layer.id, 500)
    await db_session.commit()

    personal_space_id = (
        await db_session.execute(
            text(f"SELECT id FROM {S}.space WHERE user_id = :u"), {"u": owner.id}
        )
    ).scalar_one()

    r = await client.get(
        f"{settings.API_V2_STR}/space/{personal_space_id}/usage",
        headers=_bearer(owner.id),
    )
    assert r.status_code == 200, r.text
    assert r.json()["bytes"] == 500
    assert r.json()["layers"] == 1

    r_other = await client.get(
        f"{settings.API_V2_STR}/space/{personal_space_id}/usage",
        headers=_bearer(other.id),
    )
    assert r_other.status_code == 404, "someone else's personal space is not yours"


@pytest.mark.asyncio
async def test_authorization_gate_admits_the_usage_path(
    db_session: AsyncSession,
    roles: dict[str, UUID],
    make_user: Callable[..., Awaitable[User]],
    make_org: Callable[[], Awaitable[Organization]],
) -> None:
    """`seed_roles` registers the usage GET as a `customer.resource` row, so
    `auth_z` finds it. It carries the same `read-team` permission as `GET
    /space` — the real per-space membership check is `crud_space.usage`'s
    `space_rank` gate, done in code once the URL gate admits the request."""
    pattern = "space/{space_id}/usage"
    await find_resource(db_session, pattern, "GET")

    org = await make_org()
    caller = await make_user(org.id)
    await db_session.execute(
        text(f"INSERT INTO {S}.user_role (user_id, role_id) VALUES (:u, :r)"),
        {"u": caller.id, "r": roles["organization-owner"]},
    )
    path = f"space/{uuid4()}/usage"

    assert await _authorized(db_session, caller.id, pattern, path) is True
    assert (
        await _authorized(db_session, uuid4(), pattern, path) is False
    ), "an unprovisioned caller is refused"


@pytest.mark.asyncio
async def test_personally_owned_layer_count_on_a_team_project(
    client: AsyncClient,
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
    org = await make_org()
    lead = await make_user(org.id)
    space, _ = await _team_space(
        db_session, roles, make_team, make_space, org=org, lead=lead, members=[]
    )

    team_folder = await make_folder(lead, "Team")
    await _move_to_space(db_session, "folder", team_folder.id, space.id)

    personal_folder = await make_folder(lead, "Mine")
    personal_layer = await make_layer(
        lead, personal_folder
    )  # stays in lead's personal space

    team_layer = await make_layer(lead, team_folder)
    await _move_to_space(db_session, "layer", team_layer.id, space.id)

    project = await make_project(lead, team_folder)
    await _move_to_space(db_session, "project", project.id, space.id)
    await db_session.commit()

    added = await client.post(
        f"{settings.API_V2_STR}/project/{project.id}/layer",
        params={"layer_ids": [str(personal_layer.id), str(team_layer.id)]},
        headers=_bearer(lead.id),
    )
    assert added.status_code == 200, added.text

    r = await client.get(
        f"{settings.API_V2_STR}/project/{project.id}", headers=_bearer(lead.id)
    )
    assert r.status_code == 200, r.text
    assert r.json()["personally_owned_layer_count"] == 1


@pytest.mark.asyncio
async def test_personally_owned_layer_count_is_null_for_a_personal_project(
    client: AsyncClient,
    db_session: AsyncSession,
    fixture_create_user: UUID,
    fixture_get_home_folder: dict[str, object],
) -> None:
    home = str(fixture_get_home_folder["id"])
    created = await client.post(
        f"{settings.API_V2_STR}/project",
        json={
            "name": "personal project",
            "folder_id": home,
            "initial_view_state": {
                "zoom": 5,
                "pitch": 0,
                "bearing": 0,
                "latitude": 0,
                "longitude": 0,
                "min_zoom": 0,
                "max_zoom": 20,
            },
        },
    )
    assert created.status_code == 201, created.text
    body = created.json()
    assert body.get("space_kind") == "personal"
    assert (
        "personally_owned_layer_count" not in body
    ), "response_model_exclude_none drops it for a personal-space project"
