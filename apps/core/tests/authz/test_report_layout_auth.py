"""Layouts become shareable through templates, so their routes carry the
same authorization gate as workflows (T12)."""

from uuid import uuid4

import pytest
from core.core.config import settings
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_report_layout_routes_require_a_readable_project(
    client: AsyncClient,
) -> None:
    r = await client.get(f"{settings.API_V2_STR}/project/{uuid4()}/report-layout")
    assert r.status_code in (
        403,
        404,
    ), r.text  # not 200 for a project the caller cannot read


@pytest.mark.asyncio
async def test_profile_reports_superuser_flag(
    client: AsyncClient, fixture_create_user: None
) -> None:
    r = await client.get(f"{settings.API_V2_STR}/users/profile")
    assert r.status_code == 200, r.text
    assert (
        r.json()["is_superuser"] is True
    )  # default_user_claims() carries realm role "superuser"
