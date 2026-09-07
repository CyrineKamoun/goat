"""Opening a project stamps the caller's own link, so Home can order by
what *I* last opened rather than what anyone last edited (H4)."""

from uuid import UUID

import pytest
from core.core.config import settings
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

S = settings.SCHEMA


@pytest.mark.asyncio
async def test_reading_a_project_stamps_last_opened(
    client: AsyncClient,
    db_session: AsyncSession,
    fixture_create_user: UUID,
    fixture_get_home_folder: dict[str, object],
) -> None:
    r = await client.post(
        f"{settings.API_V2_STR}/project",
        json={
            "name": "last-opened-project",
            "folder_id": str(fixture_get_home_folder["id"]),
            "initial_view_state": {
                "latitude": 48.1,
                "longitude": 11.5,
                "zoom": 10,
                "min_zoom": 0,
                "max_zoom": 20,
                "bearing": 0,
                "pitch": 0,
            },
        },
    )
    assert r.status_code in (200, 201), r.text
    pid = r.json()["id"]

    before = (
        await db_session.execute(
            text(f"SELECT last_opened_at FROM {S}.user_project WHERE project_id = :p"),
            {"p": pid},
        )
    ).scalar_one()
    assert before is None

    r = await client.get(f"{settings.API_V2_STR}/project/{pid}")
    assert r.status_code == 200, r.text

    after = (
        await db_session.execute(
            text(f"SELECT last_opened_at FROM {S}.user_project WHERE project_id = :p"),
            {"p": pid},
        )
    ).scalar_one()
    assert after is not None
