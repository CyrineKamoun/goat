"""seed_templates (T11b): the five GOAT layout starters, idempotent by
`source_ref.seed_id`."""

from typing import Any

import pytest
from core.core.config import settings
from core.db.models.template import Template, TemplateCatalogStatus, TemplatePayloadKind
from core.db.seed_templates import STARTERS, seed_templates
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession


@pytest.mark.asyncio
async def test_seed_templates_is_idempotent(
    db_session: AsyncSession, fixture_create_user: Any
) -> None:
    await seed_templates(db_session)
    await seed_templates(db_session)

    rows = (
        (
            await db_session.execute(
                select(Template).where(
                    Template.catalog_status == TemplateCatalogStatus.published
                )
            )
        )
        .scalars()
        .all()
    )
    assert len(rows) == len(STARTERS)
    assert {row.source_ref.get("seed_id") for row in rows} == {
        s["seed_id"] for s in STARTERS
    }
    for row in rows:
        assert row.payload_kind == TemplatePayloadKind.layout
        assert row.categories == ["Reporting"]
        assert row.user_id is None
        assert row.config is not None
        assert row.config.get("elements") is not None


@pytest.mark.asyncio
async def test_seed_templates_updates_existing_row_on_rerun(
    db_session: AsyncSession, fixture_create_user: Any
) -> None:
    await seed_templates(db_session)

    row = (
        await db_session.execute(
            select(Template).where(Template.source_ref["seed_id"].astext == "blank")
        )
    ).scalar_one()
    row.name = "stale name"
    await db_session.commit()

    await seed_templates(db_session)

    refreshed = (
        await db_session.execute(
            select(Template).where(Template.source_ref["seed_id"].astext == "blank")
        )
    ).scalar_one()
    assert refreshed.id == row.id
    assert refreshed.name == "Blank"


@pytest.mark.asyncio
async def test_seed_templates_visible_via_goat_source(
    client: AsyncClient, db_session: AsyncSession, fixture_create_user: Any
) -> None:
    await seed_templates(db_session)

    r = await client.get(f"{settings.API_V2_STR}/template?source=goat")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["total"] == len(STARTERS)
    names = {item["name"] for item in body["items"]}
    assert names == {s["name"] for s in STARTERS}
    for item in body["items"]:
        assert item["catalog_status"] == "published"
        assert item["payload_kind"] == "layout"
