"""`GET`/`POST /folder` over team/organisation spaces.

A space owns its content (D1/D5), so its members browse the whole tree:
every folder in a space the caller is a member of is listed, including the
space's own `home` root (created with the space, `user_id NULL`) and folders
other members created. Creating a root folder in such a space needs write
rank on it (`space_rank >= 2`).
"""

import base64
import json
from typing import Any, Awaitable, Callable
from uuid import UUID

import pytest
from core.core.config import settings
from core.db.models._link_model import UserTeamLink
from core.db.models.organization import Organization
from core.db.models.user import User
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

S = settings.SCHEMA


def _unverified_bearer(user_id: UUID) -> str:
    """A JWT-shaped (but unsigned) bearer token carrying `sub`, so the test
    client can act as a second caller under `AUTH=False` — the same helper
    `test_content_feed.py` uses."""

    def _segment(payload: dict[str, str]) -> str:
        return (
            base64.urlsafe_b64encode(json.dumps(payload).encode()).rstrip(b"=").decode()
        )

    return f"{_segment({'alg': 'none', 'typ': 'JWT'})}.{_segment({'sub': str(user_id)})}.sig"


async def _team_with_space(
    client: AsyncClient, db_session: AsyncSession, name: str
) -> tuple[UUID, UUID]:
    """A team created by the default caller (who becomes its owner), plus the
    id of the space `crud_team.create_team` provisioned with it."""
    r = await client.post(f"{settings.API_V2_STR}/teams", json={"name": name})
    assert r.status_code == 200, r.text
    team_id = UUID(r.json()["id"])
    space_id = (
        await db_session.execute(
            text(f"SELECT id FROM {S}.space WHERE team_id = :t"), {"t": team_id}
        )
    ).scalar_one()
    return team_id, UUID(str(space_id))


async def _folders(client: AsyncClient, user_id: UUID | None = None) -> list[Any]:
    headers = (
        {"Authorization": f"Bearer {_unverified_bearer(user_id)}"} if user_id else {}
    )
    r = await client.get(f"{settings.API_V2_STR}/folder", headers=headers)
    assert r.status_code == 200, r.text
    return list(r.json())


@pytest.mark.asyncio
async def test_team_space_home_folder_is_provisioned_with_the_space(
    client: AsyncClient,
    db_session: AsyncSession,
    fixture_create_user: UUID,
    fixture_get_home_folder: dict[str, object],
    roles: dict[str, UUID],
) -> None:
    _, space_id = await _team_with_space(client, db_session, "Root team")
    row = (
        await db_session.execute(
            text(
                f"SELECT name, parent_id, user_id FROM {S}.folder "
                "WHERE space_id = :s AND deleted_at IS NULL"
            ),
            {"s": space_id},
        )
    ).all()
    assert [(r[0], r[1], r[2]) for r in row] == [("home", None, None)]


@pytest.mark.asyncio
async def test_member_lists_the_team_space_root_and_another_members_folder(
    client: AsyncClient,
    db_session: AsyncSession,
    fixture_create_user: UUID,
    fixture_get_home_folder: dict[str, object],
    roles: dict[str, UUID],
    make_user: Callable[..., Awaitable[User]],
) -> None:
    me = fixture_create_user
    team_id, space_id = await _team_with_space(client, db_session, "Mobility")

    caller = await db_session.get(User, me)
    assert caller is not None
    other = await make_user(caller.organization_id)
    db_session.add(
        UserTeamLink(user_id=other.id, team_id=team_id, role_id=roles["team-member"])
    )
    await db_session.commit()

    created = await client.post(
        f"{settings.API_V2_STR}/folder",
        json={"name": "Their folder", "space_id": str(space_id)},
        headers={"Authorization": f"Bearer {_unverified_bearer(other.id)}"},
    )
    assert created.status_code == 201, created.text
    assert created.json()["space_id"] == str(space_id)
    assert created.json()["parent_id"] is None

    rows = await _folders(client)
    in_space = {r["name"]: r for r in rows if r["space_id"] == str(space_id)}
    assert set(in_space) == {"home", "Their folder"}
    # The caller created the team but neither of these folders, and the team's
    # owner holds space_rank 3 on its space.
    for row in in_space.values():
        assert row["is_owned"] is False
        assert row["role"] == "folder-owner"
        assert row["shared_from_name"] == "Mobility"


@pytest.mark.asyncio
async def test_plain_member_gets_the_editor_default_and_a_non_member_sees_nothing(
    client: AsyncClient,
    db_session: AsyncSession,
    fixture_create_user: UUID,
    fixture_get_home_folder: dict[str, object],
    roles: dict[str, UUID],
    make_user: Callable[..., Awaitable[User]],
) -> None:
    me = fixture_create_user
    team_id, space_id = await _team_with_space(client, db_session, "Planning")
    caller = await db_session.get(User, me)
    assert caller is not None
    member = await make_user(caller.organization_id)
    db_session.add(
        UserTeamLink(user_id=member.id, team_id=team_id, role_id=roles["team-member"])
    )
    stranger = await make_user(None)
    await db_session.commit()

    # A plain member of an editor-default team space: folder-editor, not owner.
    member_rows = await _folders(client, member.id)
    member_in_space = [r for r in member_rows if r["space_id"] == str(space_id)]
    assert [r["name"] for r in member_in_space] == ["home"]
    assert member_in_space[0]["role"] == "folder-editor"
    assert member_in_space[0]["shared_from_name"] == "Planning"

    # Someone with no membership at all never sees the space's folders.
    stranger_rows = await _folders(client, stranger.id)
    assert [r for r in stranger_rows if r["space_id"] == str(space_id)] == []


@pytest.mark.asyncio
async def test_creating_a_root_folder_in_a_space_needs_write_rank(
    client: AsyncClient,
    db_session: AsyncSession,
    fixture_create_user: UUID,
    fixture_get_home_folder: dict[str, object],
    roles: dict[str, UUID],
    make_user: Callable[..., Awaitable[User]],
    make_org: Callable[[], Awaitable[Organization]],
) -> None:
    _, space_id = await _team_with_space(client, db_session, "Ops")
    org = await make_org()
    stranger = await make_user(org.id)
    await db_session.commit()

    refused = await client.post(
        f"{settings.API_V2_STR}/folder",
        json={"name": "Not mine", "space_id": str(space_id)},
        headers={"Authorization": f"Bearer {_unverified_bearer(stranger.id)}"},
    )
    assert refused.status_code == 403, refused.text

    # The team's own owner may, and the folder lands in the team space.
    allowed = await client.post(
        f"{settings.API_V2_STR}/folder",
        json={"name": "Ops plans", "space_id": str(space_id)},
    )
    assert allowed.status_code == 201, allowed.text
    assert allowed.json()["space_id"] == str(space_id)
    assert allowed.json()["parent_id"] is None


@pytest.mark.asyncio
async def test_a_folder_without_space_id_still_lands_in_the_personal_space(
    client: AsyncClient,
    db_session: AsyncSession,
    fixture_create_user: UUID,
    fixture_get_home_folder: dict[str, object],
) -> None:
    personal = (
        await db_session.execute(
            text(f"SELECT id FROM {S}.space WHERE user_id = :u"),
            {"u": fixture_create_user},
        )
    ).scalar_one()
    r = await client.post(f"{settings.API_V2_STR}/folder", json={"name": "Drafts"})
    assert r.status_code == 201, r.text
    assert r.json()["space_id"] == str(personal)
