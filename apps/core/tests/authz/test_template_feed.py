"""Task 2: the Content feed, trash and transfer learn the `template`
resource type (T1-T3), and frozen template-source projects are hidden
everywhere (T2).
"""

from typing import Awaitable, Callable
from uuid import UUID

import pytest
from core.core.config import settings
from core.db.models.folder import Folder
from core.db.models.organization import Organization
from core.db.models.space import Space, SpaceKind
from core.db.models.team import Team
from core.db.models.user import User
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from tests.authz.test_content_feed import _feed

S = settings.SCHEMA
VIEW = {
    "latitude": 48.1,
    "longitude": 11.5,
    "zoom": 10,
    "min_zoom": 0,
    "max_zoom": 20,
    "bearing": 0,
    "pitch": 0,
}


async def _make_template(
    db: AsyncSession,
    *,
    space_id: UUID,
    folder_id: UUID,
    creator: UUID | None,
    name: str = "t",
    payload_kind: str = "workflow",
    catalog_status: str = "none",
    source_project_id: UUID | None = None,
    inputs: str = "[]",
) -> UUID:
    """Mirrors `test_template_authz.py`'s raw-SQL insert — a template's
    other columns (`categories`, `source_ref`, `restricted`, ...) all carry
    server defaults, so only what a test cares about is passed here."""
    return (
        await db.execute(
            text(
                f"""
                INSERT INTO {S}.template
                    (id, name, space_id, folder_id, user_id, payload_kind,
                     catalog_status, source_project_id, inputs, updated_at)
                VALUES
                    (gen_random_uuid(), :n, :s, :f, :u, :pk, :cs, :sp,
                     CAST(:inputs AS jsonb), now())
                RETURNING id
                """
            ),
            {
                "n": name,
                "s": space_id,
                "f": folder_id,
                "u": creator,
                "pk": payload_kind,
                "cs": catalog_status,
                "sp": source_project_id,
                "inputs": inputs,
            },
        )
    ).scalar_one()


@pytest.mark.asyncio
async def test_template_appears_in_space_view_recent_view_and_type_filter(
    client: AsyncClient,
    db_session: AsyncSession,
    fixture_create_user: UUID,
    fixture_get_home_folder: dict[str, object],
) -> None:
    me = fixture_create_user
    sid = (
        await db_session.execute(
            text(f"SELECT id FROM {S}.space WHERE user_id = :u"), {"u": me}
        )
    ).scalar_one()
    home = str(fixture_get_home_folder["id"])
    tid = await _make_template(
        db_session, space_id=sid, folder_id=home, creator=me, name="My workflow"
    )
    await db_session.commit()

    page = await _feed(client, space_id=str(sid))
    row = next(i for i in page["items"] if i["id"] == str(tid))
    assert row["type"] == "template"
    assert row["template_payload_kind"] == "workflow"
    assert row["template_kinds"] == ["workflow"]

    recent = await _feed(client, view="recent")
    assert str(tid) in {i["id"] for i in recent["items"]}

    filtered = await _feed(client, space_id=str(sid), types="template")
    assert {i["id"] for i in filtered["items"]} == {str(tid)}
    assert all(i["type"] == "template" for i in filtered["items"])


@pytest.mark.asyncio
async def test_published_template_reaches_a_stranger_via_recent_shelf_not_their_space_view(
    client: AsyncClient,
    db_session: AsyncSession,
    fixture_create_user: UUID,
    roles: dict[str, UUID],
    make_user: Callable[..., Awaitable[User]],
    make_org: Callable[[], Awaitable[Organization]],
    make_folder: Callable[..., Awaitable[Folder]],
) -> None:
    """T4's shelf: a published template is readable by every authenticated
    user regardless of the space it lives in — but only through the
    `recent` view's shelf reach, not by literally listing a space the
    template never lived in."""
    org = await make_org()
    owner = await make_user(org.id)
    owner_folder = await make_folder(owner, "owner-home")
    tid = await _make_template(
        db_session,
        space_id=owner_folder.space_id,
        folder_id=owner_folder.id,
        creator=owner.id,
        catalog_status="published",
    )
    await db_session.commit()

    stranger = fixture_create_user
    stranger_sid = (
        await db_session.execute(
            text(f"SELECT id FROM {S}.space WHERE user_id = :u"), {"u": stranger}
        )
    ).scalar_one()

    recent = await _feed(client, view="recent")
    assert str(tid) in {i["id"] for i in recent["items"]}

    own_space = await _feed(client, space_id=str(stranger_sid))
    assert str(tid) not in {i["id"] for i in own_space["items"]}


@pytest.mark.asyncio
async def test_project_payload_template_derives_kinds_from_the_frozen_source(
    client: AsyncClient,
    db_session: AsyncSession,
    fixture_create_user: UUID,
    fixture_get_home_folder: dict[str, object],
) -> None:
    """A project-payload template's `template_kinds` are derived (T1) from
    what its frozen `source_project_id` actually holds: Dashboard if it has
    a builder config, plus Workflow/Layout if it holds a `workflow`/
    `report_layout` row — never empty."""
    me = fixture_create_user
    sid = (
        await db_session.execute(
            text(f"SELECT id FROM {S}.space WHERE user_id = :u"), {"u": me}
        )
    ).scalar_one()
    home = str(fixture_get_home_folder["id"])

    source = await client.post(
        f"{settings.API_V2_STR}/project",
        json={"name": "frozen source", "folder_id": home, "initial_view_state": VIEW},
    )
    assert source.status_code in (200, 201), source.text
    source_id = source.json()["id"]
    await db_session.execute(
        text(
            f"UPDATE {S}.project SET is_template_source = TRUE, "
            "builder_config = '{}'::jsonb WHERE id = :p"
        ),
        {"p": source_id},
    )
    await db_session.execute(
        text(
            f"INSERT INTO {S}.workflow "
            "(id, name, project_id, is_default, config, updated_at) "
            "VALUES (gen_random_uuid(), 'wf', :p, FALSE, '{}'::jsonb, now())"
        ),
        {"p": source_id},
    )
    await db_session.commit()

    tid = await _make_template(
        db_session,
        space_id=sid,
        folder_id=home,
        creator=me,
        name="Dashboard template",
        payload_kind="project",
        source_project_id=source_id,
    )
    await db_session.commit()

    page = await _feed(client, space_id=str(sid), types="template")
    row = next(i for i in page["items"] if i["id"] == str(tid))
    assert row["template_payload_kind"] == "project"
    assert set(row["template_kinds"]) == {"dashboard", "workflow"}
    # The frozen source project itself never surfaces as a row of its own.
    assert source_id not in {i["id"] for i in page["items"]}


@pytest.mark.asyncio
async def test_frozen_template_source_project_never_appears_in_the_feed(
    client: AsyncClient,
    db_session: AsyncSession,
    fixture_create_user: UUID,
    fixture_get_home_folder: dict[str, object],
) -> None:
    sid = (
        await db_session.execute(
            text(f"SELECT id FROM {S}.space WHERE user_id = :u"),
            {"u": fixture_create_user},
        )
    ).scalar_one()
    home = str(fixture_get_home_folder["id"])
    p = await client.post(
        f"{settings.API_V2_STR}/project",
        json={"name": "frozen", "folder_id": home, "initial_view_state": VIEW},
    )
    assert p.status_code in (200, 201), p.text
    pid = p.json()["id"]
    await db_session.execute(
        text(f"UPDATE {S}.project SET is_template_source = TRUE WHERE id = :p"),
        {"p": pid},
    )
    await db_session.commit()

    page = await _feed(client, space_id=str(sid), folder_id=home)
    assert pid not in {i["id"] for i in page["items"]}
    recent = await _feed(client, view="recent")
    assert pid not in {i["id"] for i in recent["items"]}


@pytest.mark.asyncio
async def test_transfer_moves_a_template_from_my_content_to_a_team_space(
    client: AsyncClient,
    db_session: AsyncSession,
    fixture_create_user: UUID,
    fixture_get_home_folder: dict[str, object],
    roles: dict[str, UUID],
    make_org: Callable[[], Awaitable[Organization]],
    make_user: Callable[..., Awaitable[User]],
    make_team: Callable[..., Awaitable[Team]],
    make_space: Callable[..., Awaitable[Space]],
) -> None:
    me = fixture_create_user
    org = await make_org()
    await db_session.execute(
        text(f'UPDATE {S}."user" SET organization_id = :o WHERE id = :u'),
        {"o": org.id, "u": me},
    )
    me_user = await db_session.get(User, me)
    assert me_user is not None
    mate = await make_user(org.id)
    team = await make_team(me_user, mate, org=org)
    space = await make_space(SpaceKind.team, team=team)
    await db_session.commit()

    home = str(fixture_get_home_folder["id"])
    sid = (
        await db_session.execute(
            text(f"SELECT id FROM {S}.space WHERE user_id = :u"), {"u": me}
        )
    ).scalar_one()
    tid = await _make_template(
        db_session, space_id=sid, folder_id=home, creator=me, name="movable"
    )
    await db_session.commit()

    preview = await client.post(
        f"{settings.API_V2_STR}/content/transfer/preview",
        json={
            "items": [{"type": "template", "id": str(tid)}],
            "target_space_id": str(space.id),
        },
    )
    assert preview.status_code == 200, preview.text
    assert preview.json()["items"][0]["type"] == "template"

    r = await client.post(
        f"{settings.API_V2_STR}/content/transfer",
        json={
            "items": [{"type": "template", "id": str(tid)}],
            "target_space_id": str(space.id),
        },
    )
    assert r.status_code == 200, r.text
    assert r.json()["moved"]["template"] == 1
    assert (
        await db_session.execute(
            text(f"SELECT space_id FROM {S}.template WHERE id = :t"), {"t": tid}
        )
    ).scalar_one() == space.id
