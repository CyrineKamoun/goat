from typing import Awaitable, Callable
from uuid import UUID

import pytest
from core.core.config import settings
from core.db.models.organization import Organization
from core.db.models.team import Team
from core.db.models.user import User
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

S = settings.SCHEMA


@pytest.mark.asyncio
async def test_my_spaces_lists_personal_team_and_organisation(
    client: AsyncClient,
    db_session: AsyncSession,
    fixture_create_user: UUID,
    roles: dict[str, UUID],
    make_org: Callable[[], Awaitable[Organization]],
    make_team: Callable[..., Awaitable[Team]],
) -> None:
    me = fixture_create_user
    org = await make_org()
    await db_session.execute(
        text(f'UPDATE {S}."user" SET organization_id = :o WHERE id = :u'),
        {"o": org.id, "u": me},
    )
    me_user = await db_session.get(User, me)
    assert me_user is not None
    team = await make_team(me_user, org=org)
    await db_session.commit()

    r = await client.get(f"{settings.API_V2_STR}/space")
    assert r.status_code == 200, r.text
    kinds = {s["kind"]: s for s in r.json()}
    assert set(kinds) == {"personal", "team", "organization"}
    assert kinds["personal"]["my_role"] == "owner"
    assert (
        kinds["team"]["default_role"] == "editor"
        and kinds["team"]["my_role"] == "editor"
    ), "plain member gets the default"
    assert (
        kinds["organization"]["default_role"] == "viewer"
        and kinds["organization"]["my_role"] == "viewer"
    )
    assert kinds["team"]["team_id"] == str(team.id)


@pytest.mark.asyncio
async def test_only_a_team_owner_may_change_the_default_role(
    client: AsyncClient,
    db_session: AsyncSession,
    fixture_create_user: UUID,
    roles: dict[str, UUID],
    make_org: Callable[[], Awaitable[Organization]],
    make_user: Callable[..., Awaitable[User]],
    make_team: Callable[..., Awaitable[Team]],
) -> None:
    me = fixture_create_user
    org = await make_org()
    await db_session.execute(
        text(f'UPDATE {S}."user" SET organization_id = :o WHERE id = :u'),
        {"o": org.id, "u": me},
    )
    me_user = await db_session.get(User, me)
    assert me_user is not None
    lead = await make_user(org.id)
    team = await make_team(
        lead, me_user, org=org
    )  # lead and me are members; make lead the owner
    await db_session.execute(
        text(
            f"UPDATE {S}.user_team SET role_id = :r WHERE team_id = :t AND user_id = :u"
        ),
        {"r": roles["team-owner"], "t": team.id, "u": lead.id},
    )
    await db_session.commit()
    # `make_team` seeds the team row directly (bypassing crud_team.create_team,
    # which is what lazily creates a team's space on the real create path) —
    # list spaces once first so the team's space exists before we look it up.
    list_response = await client.get(f"{settings.API_V2_STR}/space")
    assert list_response.status_code == 200, list_response.text
    sid = (
        await db_session.execute(
            text(f"SELECT id FROM {S}.space WHERE team_id = :t"), {"t": team.id}
        )
    ).scalar_one()

    r = await client.patch(
        f"{settings.API_V2_STR}/space/{sid}", json={"default_role": "viewer"}
    )
    assert r.status_code == 403, "a plain member cannot change the team default"
    await db_session.execute(
        text(
            f"UPDATE {S}.user_team SET role_id = :r WHERE team_id = :t AND user_id = :u"
        ),
        {"r": roles["team-owner"], "t": team.id, "u": me},
    )
    await db_session.commit()
    r = await client.patch(
        f"{settings.API_V2_STR}/space/{sid}", json={"default_role": "viewer"}
    )
    assert r.status_code == 200 and r.json()["default_role"] == "viewer"


@pytest.mark.asyncio
async def test_personal_space_default_cannot_be_changed(
    client: AsyncClient, db_session: AsyncSession, fixture_create_user: UUID
) -> None:
    sid = (
        await db_session.execute(
            text(f"SELECT id FROM {S}.space WHERE user_id = :u"),
            {"u": fixture_create_user},
        )
    ).scalar_one()
    r = await client.patch(
        f"{settings.API_V2_STR}/space/{sid}", json={"default_role": "viewer"}
    )
    assert r.status_code == 400


@pytest.mark.asyncio
async def test_creating_a_team_creates_its_space(
    client: AsyncClient, db_session: AsyncSession, fixture_create_user: UUID
) -> None:
    r = await client.post(
        f"{settings.API_V2_STR}/teams", json={"name": "new team", "avatar": ""}
    )
    assert r.status_code in (200, 201), r.text
    tid = r.json()["id"]
    row = (
        await db_session.execute(
            text(f"SELECT default_role FROM {S}.space WHERE team_id = :t"), {"t": tid}
        )
    ).scalar_one_or_none()
    assert row == "editor"
