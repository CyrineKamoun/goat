"""Per-user Home UI state lives on the existing settings row, not a second
table (Home H10): `onboarding_skipped_at`, `releases_seen_at` and
`spotlight_seen` ride along on `GET/PUT /system/settings`."""

from uuid import UUID

import pytest
from core.core.config import settings
from httpx import AsyncClient

URL = f"{settings.API_V2_STR}/system/settings"


@pytest.mark.asyncio
async def test_fresh_user_gets_null_home_state_alongside_settings_defaults(
    client: AsyncClient, fixture_create_user: UUID
) -> None:
    r = await client.get(URL)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["onboarding_skipped_at"] is None
    assert body["releases_seen_at"] is None
    assert body["spotlight_seen"] == []
    assert body["client_theme"] == "dark"
    assert body["preferred_language"] == "de"
    assert body["unit"] == "metric"


@pytest.mark.asyncio
async def test_patching_one_home_key_leaves_others_and_theme_settings_untouched(
    client: AsyncClient, fixture_create_user: UUID
) -> None:
    # Seed the row the same way the real usePreferences()/useSystemSettings()
    # flow does: a GET before the first PUT.
    await client.get(URL)

    r = await client.put(URL, json={"releases_seen_at": "2026-09-04T10:00:00Z"})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["releases_seen_at"].startswith("2026-09-04T10:00:00")
    assert body["spotlight_seen"] == []
    assert body["onboarding_skipped_at"] is None
    assert body["client_theme"] == "dark"
    assert body["preferred_language"] == "de"
    assert body["unit"] == "metric"

    r = await client.put(URL, json={"spotlight_seen": ["2026-09-content-spaces"]})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["spotlight_seen"] == ["2026-09-content-spaces"]
    assert body["releases_seen_at"] is not None, "earlier key survives"
    assert body["client_theme"] == "dark"
    assert body["preferred_language"] == "de"
    assert body["unit"] == "metric"


@pytest.mark.asyncio
async def test_onboarding_skipped_at_round_trips(
    client: AsyncClient, fixture_create_user: UUID
) -> None:
    await client.get(URL)

    r = await client.put(URL, json={"onboarding_skipped_at": "2026-09-04T11:00:00Z"})
    assert r.status_code == 200, r.text
    assert r.json()["onboarding_skipped_at"].startswith("2026-09-04T11:00:00")

    r = await client.get(URL)
    assert r.status_code == 200, r.text
    assert r.json()["onboarding_skipped_at"].startswith("2026-09-04T11:00:00")
