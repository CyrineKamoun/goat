"""The template categories facet and the `categories=` list filter:
`GET /template/categories` counts the tags in use on the caller's readable
templates, and `GET /template?categories=` narrows the list to templates
carrying every listed tag."""

from typing import Awaitable, Callable
from uuid import UUID

import pytest
from core.core.config import settings
from core.db.models.folder import Folder
from core.db.models.organization import Organization
from core.db.models.user import User
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

S = settings.SCHEMA


async def _make_template(
    db: AsyncSession,
    *,
    space_id: UUID,
    folder_id: UUID,
    creator: UUID | None,
    name: str,
    categories: list[str],
    payload_kind: str = "workflow",
    catalog_status: str = "none",
    age_hours: int = 0,
) -> UUID:
    """Insert a template directly, the way `test_template_feed.py` does.

    `age_hours` backdates `created_at`: every row of one test transaction
    would otherwise share `now()`, and `created_at` is what decides which
    spelling of a category the facet reports.
    """
    return (
        await db.execute(
            text(
                f"""
                INSERT INTO {S}.template
                    (id, name, space_id, folder_id, user_id, payload_kind,
                     catalog_status, categories, created_at, updated_at)
                VALUES
                    (gen_random_uuid(), :n, :s, :f, :u, :pk, :cs,
                     CAST(:cats AS text[]),
                     now() - make_interval(hours => :age), now())
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
                "cats": categories,
                "age": age_hours,
            },
        )
    ).scalar_one()


async def _my_space(db: AsyncSession, user_id: UUID) -> UUID:
    return (
        await db.execute(
            text(f"SELECT id FROM {S}.space WHERE user_id = :u"), {"u": user_id}
        )
    ).scalar_one()


async def _facet(client: AsyncClient, **params: str) -> list[dict[str, object]]:
    resp = await client.get(
        f"{settings.API_V2_STR}/template/categories", params=params or None
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert isinstance(body, list)
    return body


async def _names(client: AsyncClient, **params: str) -> set[str]:
    resp = await client.get(f"{settings.API_V2_STR}/template", params=params)
    assert resp.status_code == 200, resp.text
    return {item["name"] for item in resp.json()["items"]}


@pytest.mark.asyncio
async def test_facet_groups_categories_case_insensitively_by_first_spelling(
    client: AsyncClient,
    db_session: AsyncSession,
    fixture_create_user: UUID,
    fixture_get_home_folder: dict[str, object],
) -> None:
    me = fixture_create_user
    sid = await _my_space(db_session, me)
    home = UUID(str(fixture_get_home_folder["id"]))
    # Inserted first but created later, so only `created_at` — not insert
    # order — can explain the reported spelling.
    await _make_template(
        db_session,
        space_id=sid,
        folder_id=home,
        creator=me,
        name="Later",
        categories=["mobility"],
        age_hours=1,
    )
    await _make_template(
        db_session,
        space_id=sid,
        folder_id=home,
        creator=me,
        name="Earlier",
        categories=["Mobility", "Transit"],
        age_hours=2,
    )
    await db_session.commit()

    facet = await _facet(client, source="mine")
    assert facet == [
        {"name": "Mobility", "count": 2},
        {"name": "Transit", "count": 1},
    ]


@pytest.mark.asyncio
async def test_facet_counts_a_template_carrying_two_spellings_once(
    client: AsyncClient,
    db_session: AsyncSession,
    fixture_create_user: UUID,
    fixture_get_home_folder: dict[str, object],
) -> None:
    me = fixture_create_user
    sid = await _my_space(db_session, me)
    home = UUID(str(fixture_get_home_folder["id"]))
    await _make_template(
        db_session,
        space_id=sid,
        folder_id=home,
        creator=me,
        name="Both spellings",
        categories=["Mobility", "MOBILITY"],
    )
    await db_session.commit()

    assert await _facet(client, source="mine") == [{"name": "Mobility", "count": 1}]


@pytest.mark.asyncio
async def test_facet_ignores_a_template_in_a_space_the_caller_cannot_read(
    client: AsyncClient,
    db_session: AsyncSession,
    fixture_create_user: UUID,
    fixture_get_home_folder: dict[str, object],
    make_org: Callable[[], Awaitable[Organization]],
    make_user: Callable[..., Awaitable[User]],
    make_folder: Callable[..., Awaitable[Folder]],
) -> None:
    me = fixture_create_user
    sid = await _my_space(db_session, me)
    home = UUID(str(fixture_get_home_folder["id"]))
    await _make_template(
        db_session,
        space_id=sid,
        folder_id=home,
        creator=me,
        name="Mine",
        categories=["Mine"],
    )

    org = await make_org()
    stranger = await make_user(org.id)
    stranger_folder = await make_folder(stranger, "stranger-home")
    await _make_template(
        db_session,
        space_id=stranger_folder.space_id,
        folder_id=stranger_folder.id,
        creator=stranger.id,
        name="Theirs",
        categories=["Theirs"],
    )
    await db_session.commit()

    assert await _facet(client) == [{"name": "Mine", "count": 1}]


@pytest.mark.asyncio
async def test_facet_narrows_by_kind_and_by_source(
    client: AsyncClient,
    db_session: AsyncSession,
    fixture_create_user: UUID,
    fixture_get_home_folder: dict[str, object],
    make_org: Callable[[], Awaitable[Organization]],
    make_user: Callable[..., Awaitable[User]],
    make_folder: Callable[..., Awaitable[Folder]],
) -> None:
    me = fixture_create_user
    sid = await _my_space(db_session, me)
    home = UUID(str(fixture_get_home_folder["id"]))
    await _make_template(
        db_session,
        space_id=sid,
        folder_id=home,
        creator=me,
        name="A workflow",
        categories=["Workflows"],
        payload_kind="workflow",
    )
    await _make_template(
        db_session,
        space_id=sid,
        folder_id=home,
        creator=me,
        name="A layout",
        categories=["Layouts"],
        payload_kind="layout",
    )

    org = await make_org()
    stranger = await make_user(org.id)
    stranger_folder = await make_folder(stranger, "shelf-home")
    await _make_template(
        db_session,
        space_id=stranger_folder.space_id,
        folder_id=stranger_folder.id,
        creator=stranger.id,
        name="On the shelf",
        categories=["Shelf"],
        catalog_status="published",
    )
    await db_session.commit()

    assert await _facet(client, source="mine", kind="workflow") == [
        {"name": "Workflows", "count": 1}
    ]
    assert await _facet(client, source="mine", kind="layout") == [
        {"name": "Layouts", "count": 1}
    ]
    assert await _facet(client, source="goat") == [{"name": "Shelf", "count": 1}]
    assert {row["name"] for row in await _facet(client)} == {
        "Workflows",
        "Layouts",
        "Shelf",
    }


@pytest.mark.asyncio
async def test_categories_filter_matches_only_templates_carrying_every_tag(
    client: AsyncClient,
    db_session: AsyncSession,
    fixture_create_user: UUID,
    fixture_get_home_folder: dict[str, object],
) -> None:
    me = fixture_create_user
    sid = await _my_space(db_session, me)
    home = UUID(str(fixture_get_home_folder["id"]))
    for name, categories in (
        ("Both", ["Mobility", "Transit"]),
        ("Only mobility", ["Mobility"]),
        ("Only transit", ["transit"]),
        ("Untagged", []),
    ):
        await _make_template(
            db_session,
            space_id=sid,
            folder_id=home,
            creator=me,
            name=name,
            categories=categories,
        )
    await db_session.commit()

    assert await _names(client, source="mine", categories="Mobility,Transit") == {
        "Both"
    }
    # Case-insensitive on both sides, and blanks in the list are ignored.
    assert await _names(client, source="mine", categories="mobility") == {
        "Both",
        "Only mobility",
    }
    assert await _names(client, source="mine", categories="TRANSIT , ") == {
        "Both",
        "Only transit",
    }
    assert await _names(client, source="mine", categories="unknown") == set()
    assert await _names(client, source="mine", categories="  ") == {
        "Both",
        "Only mobility",
        "Only transit",
        "Untagged",
    }
