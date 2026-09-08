from typing import Awaitable, Callable
from uuid import UUID

import pytest
from core.core.config import settings
from core.db.models.organization import Organization
from core.db.models.user import User
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

S = settings.SCHEMA


async def _mk(
    client: AsyncClient, name: str, parent_id: str | None = None
) -> dict[str, object]:
    body: dict[str, object] = {"name": name}
    if parent_id:
        body["parent_id"] = parent_id
    r = await client.post(f"{settings.API_V2_STR}/folder", json=body)
    assert r.status_code in (200, 201), r.text
    return dict(r.json())


@pytest.mark.asyncio
async def test_nesting_stops_at_depth_three(
    client: AsyncClient, fixture_create_user: UUID
) -> None:
    a = await _mk(client, "a")
    b = await _mk(client, "b", str(a["id"]))
    c = await _mk(client, "c", str(b["id"]))
    r = await client.post(
        f"{settings.API_V2_STR}/folder", json={"name": "d", "parent_id": str(c["id"])}
    )
    assert r.status_code == 400 and "depth" in r.json()["detail"].lower()
    children = await client.get(
        f"{settings.API_V2_STR}/folder", params={"parent_id": str(a["id"])}
    )
    assert [f["id"] for f in children.json()] == [b["id"]]


@pytest.mark.asyncio
async def test_move_stays_inside_the_space_and_refuses_cycles(
    client: AsyncClient,
    db_session: AsyncSession,
    fixture_create_user: UUID,
    make_user: Callable[..., Awaitable[User]],
    make_org: Callable[[], Awaitable[Organization]],
    make_folder: Callable[..., Awaitable[object]],
) -> None:
    a = await _mk(client, "a")
    b = await _mk(client, "b", str(a["id"]))
    # cycle: a under b
    r = await client.put(
        f"{settings.API_V2_STR}/folder/{a['id']}", json={"parent_id": str(b["id"])}
    )
    assert r.status_code == 400
    # cross-space: parent owned by someone else (their personal space)
    other = await make_user((await make_org()).id)
    theirs = await make_folder(other, "theirs")
    await db_session.commit()
    r = await client.put(
        f"{settings.API_V2_STR}/folder/{b['id']}",
        json={"parent_id": str(theirs.id)},  # type: ignore[attr-defined]
    )
    assert r.status_code in (400, 404)
    # legal move: b to root
    r = await client.put(
        f"{settings.API_V2_STR}/folder/{b['id']}", json={"parent_id": None}
    )
    assert r.status_code == 200 and r.json()["parent_id"] is None


@pytest.mark.asyncio
async def test_db_trigger_refuses_depth_four_even_for_raw_sql(
    db_session: AsyncSession, fixture_create_user: UUID
) -> None:
    sid = (
        await db_session.execute(
            text(f"SELECT id FROM {S}.space WHERE user_id = :u"),
            {"u": fixture_create_user},
        )
    ).scalar_one()
    ids: list[UUID] = []
    parent: UUID | None = None
    for n in range(3):
        fid = (
            await db_session.execute(
                text(
                    f"INSERT INTO {S}.folder (id, user_id, space_id, parent_id, name, updated_at) VALUES (gen_random_uuid(), :u, :s, :p, :n, now()) RETURNING id"
                ),
                {"u": fixture_create_user, "s": sid, "p": parent, "n": f"lvl{n}"},
            )
        ).scalar_one()
        ids.append(fid)
        parent = fid
    with pytest.raises(Exception):
        await db_session.execute(
            text(
                f"INSERT INTO {S}.folder (id, user_id, space_id, parent_id, name, updated_at) VALUES (gen_random_uuid(), :u, :s, :p, 'lvl3', now())"
            ),
            {"u": fixture_create_user, "s": sid, "p": parent},
        )
    await db_session.rollback()


@pytest.mark.asyncio
async def test_move_accounts_for_descendant_depth(
    client: AsyncClient, db_session: AsyncSession, fixture_create_user: UUID
) -> None:
    """A move must account for the mover's own descendants, not just the
    mover itself — otherwise a 2-level subtree moved under an already-deep
    parent would silently land descendants past depth 3 (folder_chain's
    4-row cap would then truncate, not refuse, the query)."""
    p1 = await _mk(client, "p1")
    p2 = await _mk(client, "p2")
    await _mk(client, "c2", str(p2["id"]))

    # p2 (with child c2) moved under p1: c2 lands 3 levels deep -> allowed
    r = await client.put(
        f"{settings.API_V2_STR}/folder/{p2['id']}", json={"parent_id": str(p1["id"])}
    )
    assert r.status_code == 200, r.text

    p3 = await _mk(client, "p3")
    c3 = await _mk(client, "c3", str(p3["id"]))
    await _mk(client, "g3", str(c3["id"]))

    # p3 (with grandchild g3) moved under p1: g3 would land 4 levels deep -> refused
    r = await client.put(
        f"{settings.API_V2_STR}/folder/{p3['id']}", json={"parent_id": str(p1["id"])}
    )
    assert r.status_code == 400 and "depth" in r.json()["detail"].lower()

    # the trigger alone refuses the same move via raw SQL, independent of the API-level guard
    with pytest.raises(Exception):
        await db_session.execute(
            text(f"UPDATE {S}.folder SET parent_id = :p WHERE id = :f"),
            {"p": UUID(str(p1["id"])), "f": UUID(str(p3["id"]))},
        )
    await db_session.rollback()
