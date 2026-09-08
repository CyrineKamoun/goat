"""Name-uniqueness collisions across the folder relocation paths (I2):
POST/PUT /folder map the unique-index IntegrityError to a clear 409
instead of a raw 500, and a transfer whose moving root folder's name
collides with a live root in the target space is refused up front —
before any row is mutated — rather than half-completing under AUTOCOMMIT."""

from typing import Awaitable, Callable
from uuid import UUID

import pytest
from core.core.config import settings
from core.db.models.organization import Organization
from core.db.models.space import Space, SpaceKind
from core.db.models.team import Team
from core.db.models.user import User
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

S = settings.SCHEMA


async def _org_setup(
    db: AsyncSession,
    me: UUID,
    make_org: Callable[[], Awaitable[Organization]],
) -> Organization:
    org = await make_org()
    await db.execute(
        text(f'UPDATE {S}."user" SET organization_id = :o WHERE id = :u'),
        {"o": org.id, "u": me},
    )
    await db.commit()
    return org


@pytest.mark.asyncio
async def test_create_folder_with_a_duplicate_root_name_409s(
    client: AsyncClient, fixture_create_user: UUID
) -> None:
    r = await client.post(f"{settings.API_V2_STR}/folder", json={"name": "dup"})
    assert r.status_code in (200, 201), r.text
    r = await client.post(f"{settings.API_V2_STR}/folder", json={"name": "dup"})
    assert r.status_code == 409, r.text
    assert "already exists" in r.json()["detail"]


@pytest.mark.asyncio
async def test_create_folder_with_a_duplicate_child_name_409s(
    client: AsyncClient, fixture_create_user: UUID
) -> None:
    parent = (
        await client.post(f"{settings.API_V2_STR}/folder", json={"name": "parent"})
    ).json()
    r = await client.post(
        f"{settings.API_V2_STR}/folder",
        json={"name": "child", "parent_id": parent["id"]},
    )
    assert r.status_code in (200, 201), r.text
    r = await client.post(
        f"{settings.API_V2_STR}/folder",
        json={"name": "child", "parent_id": parent["id"]},
    )
    assert r.status_code == 409, r.text


@pytest.mark.asyncio
async def test_rename_folder_into_a_collision_409s(
    client: AsyncClient, fixture_create_user: UUID
) -> None:
    await client.post(f"{settings.API_V2_STR}/folder", json={"name": "taken"})
    mine = (
        await client.post(f"{settings.API_V2_STR}/folder", json={"name": "free"})
    ).json()
    r = await client.put(
        f"{settings.API_V2_STR}/folder/{mine['id']}", json={"name": "taken"}
    )
    assert r.status_code == 409, r.text
    # the folder itself is untouched
    r = await client.get(f"{settings.API_V2_STR}/folder/{mine['id']}")
    assert r.json()["name"] == "free"


@pytest.mark.asyncio
async def test_transfer_colliding_with_an_existing_target_root_is_refused_before_any_mutation(
    client: AsyncClient,
    db_session: AsyncSession,
    fixture_create_user: UUID,
    roles: dict[str, UUID],
    make_user: Callable[..., Awaitable[User]],
    make_org: Callable[[], Awaitable[Organization]],
    make_team: Callable[..., Awaitable[Team]],
    make_space: Callable[..., Awaitable[Space]],
) -> None:
    me = fixture_create_user
    org = await _org_setup(db_session, me, make_org)
    me_user = await db_session.get(User, me)
    assert me_user is not None
    team = await make_team(me_user, org=org)
    space = await make_space(SpaceKind.team, team=team)
    await db_session.execute(
        text(
            f"INSERT INTO {S}.folder (name, space_id, parent_id, updated_at) "
            "VALUES ('shared-name', :s, NULL, now())"
        ),
        {"s": space.id},
    )
    await db_session.commit()

    top = (
        await client.post(f"{settings.API_V2_STR}/folder", json={"name": "shared-name"})
    ).json()["id"]

    preview = await client.post(
        f"{settings.API_V2_STR}/content/transfer/preview",
        json={
            "items": [{"type": "folder", "id": top}],
            "target_space_id": str(space.id),
        },
    )
    assert preview.status_code == 200, preview.text
    assert preview.json()["name_collisions"] == ["shared-name"]

    r = await client.post(
        f"{settings.API_V2_STR}/content/transfer",
        json={
            "items": [{"type": "folder", "id": top}],
            "target_space_id": str(space.id),
            "dataset_ids": [],
            "leave_shortcut": False,
        },
    )
    assert r.status_code == 409, r.text

    # nothing moved: the folder is still in the caller's personal space
    row = (
        await db_session.execute(
            text(f"SELECT space_id FROM {S}.folder WHERE id = :f"), {"f": top}
        )
    ).scalar_one()
    assert row != space.id
