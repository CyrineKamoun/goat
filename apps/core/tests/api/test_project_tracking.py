"""Endpoint tests for per-dashboard analytics selection.

PUT /project/{id}/public/tracking assigns one of the org's analytics
instances to a published project (analytics_id); null switches tracking
off. GET /project/{id}/public resolves the assigned instance into the
``analytics`` block.
"""

from typing import Any
from uuid import UUID

import pytest
from core.db.models.project import ProjectPublic
from core.db.models.user import User
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select


@pytest.fixture
async def seeded_org(db_session: AsyncSession) -> str:
    from tests.utils import make_organization

    org = make_organization(name="test-org-for-tracking")
    db_session.add(org)
    await db_session.commit()
    await db_session.refresh(org)
    return str(org.id)


@pytest.fixture
async def org_member_user(
    db_session: AsyncSession, fixture_create_user: UUID, seeded_org: str
) -> UUID:
    """Attach the default test user to the seeded org so ownership
    validation in the tracking endpoint resolves to that org."""
    user = await db_session.get(User, fixture_create_user)
    assert user is not None
    user.organization_id = UUID(seeded_org)
    await db_session.commit()
    return fixture_create_user


@pytest.fixture
async def published_project_id(
    client: AsyncClient,
    org_member_user: UUID,
    fixture_create_folder: dict,
) -> str:
    """Create a project owned by the org member and publish it."""
    project_payload = {
        "folder_id": fixture_create_folder["id"],
        "name": "Test Project For Analytics Tracking",
        "description": "",
        "initial_view_state": {
            "latitude": 48.1502132,
            "longitude": 11.5696284,
            "zoom": 12,
            "min_zoom": 0,
            "max_zoom": 20,
            "bearing": 0,
            "pitch": 0,
        },
    }
    r = await client.post("/api/v2/project", json=project_payload)
    assert r.status_code in (200, 201), r.text
    project_id = r.json()["id"]

    r = await client.post(f"/api/v2/project/{project_id}/publish")
    assert r.status_code in (200, 201), r.text
    return project_id


def _matomo_payload(name: str = "P4B Matomo") -> dict[str, Any]:
    return {
        "name": name,
        "provider": "matomo",
        "config": {
            "provider": "matomo",
            "url": "https://matomo.example.org/",
            "site_id": "5",
        },
    }


async def _create_instance(
    client: AsyncClient, org_id: str, name: str = "P4B Matomo"
) -> str:
    r = await client.post(
        f"/api/v2/organizations/{org_id}/analytics/",
        json=_matomo_payload(name=name),
    )
    assert r.status_code == 201, r.text
    return r.json()["id"]


@pytest.mark.asyncio
async def test_assign_analytics_to_project(
    client: AsyncClient, seeded_org: str, published_project_id: str
) -> None:
    analytics_id = await _create_instance(client, seeded_org)

    r = await client.put(
        f"/api/v2/project/{published_project_id}/public/tracking",
        json={"analytics_id": analytics_id},
    )
    assert r.status_code == 200, r.text
    assert r.json()["analytics_id"] == analytics_id


@pytest.mark.asyncio
async def test_clear_analytics_turns_tracking_off(
    client: AsyncClient, seeded_org: str, published_project_id: str
) -> None:
    analytics_id = await _create_instance(client, seeded_org)
    r = await client.put(
        f"/api/v2/project/{published_project_id}/public/tracking",
        json={"analytics_id": analytics_id},
    )
    assert r.status_code == 200

    r = await client.put(
        f"/api/v2/project/{published_project_id}/public/tracking",
        json={"analytics_id": None},
    )
    assert r.status_code == 200, r.text
    assert r.json()["analytics_id"] is None


@pytest.mark.asyncio
async def test_consent_only_update_keeps_analytics(
    client: AsyncClient, seeded_org: str, published_project_id: str
) -> None:
    analytics_id = await _create_instance(client, seeded_org)
    await client.put(
        f"/api/v2/project/{published_project_id}/public/tracking",
        json={"analytics_id": analytics_id},
    )

    r = await client.put(
        f"/api/v2/project/{published_project_id}/public/tracking",
        json={"require_consent": False},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["analytics_id"] == analytics_id
    assert body["tracking_require_consent"] is False


@pytest.mark.asyncio
async def test_empty_payload_rejected(
    client: AsyncClient, published_project_id: str
) -> None:
    r = await client.put(
        f"/api/v2/project/{published_project_id}/public/tracking",
        json={},
    )
    assert r.status_code == 400


@pytest.mark.asyncio
async def test_assign_unknown_instance_404(
    client: AsyncClient, published_project_id: str
) -> None:
    r = await client.put(
        f"/api/v2/project/{published_project_id}/public/tracking",
        json={"analytics_id": "00000000-0000-4000-8000-000000000099"},
    )
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_assign_foreign_org_instance_rejected(
    client: AsyncClient,
    db_session: AsyncSession,
    published_project_id: str,
) -> None:
    """An instance owned by a different org must not be assignable."""
    from tests.utils import make_organization

    other_org = make_organization(name="other-org-for-tracking")
    db_session.add(other_org)
    await db_session.commit()
    await db_session.refresh(other_org)

    foreign_id = await _create_instance(
        client, str(other_org.id), name="Foreign instance"
    )

    r = await client.put(
        f"/api/v2/project/{published_project_id}/public/tracking",
        json={"analytics_id": foreign_id},
    )
    assert r.status_code == 400
    assert "organization" in r.json()["detail"].lower()


@pytest.mark.asyncio
async def test_public_project_includes_analytics_when_assigned(
    client: AsyncClient, seeded_org: str, published_project_id: str
) -> None:
    analytics_id = await _create_instance(client, seeded_org)
    await client.put(
        f"/api/v2/project/{published_project_id}/public/tracking",
        json={"analytics_id": analytics_id},
    )

    r = await client.get(f"/api/v2/project/{published_project_id}/public")
    assert r.status_code == 200
    body = r.json()
    assert body["analytics_id"] == analytics_id
    assert body["analytics"]["provider"] == "matomo"
    assert body["analytics"]["config"]["site_id"] == "5"


@pytest.mark.asyncio
async def test_public_project_omits_analytics_when_unassigned(
    client: AsyncClient, published_project_id: str
) -> None:
    r = await client.get(f"/api/v2/project/{published_project_id}/public")
    assert r.status_code == 200
    # response_model_exclude_none drops null fields entirely
    assert r.json().get("analytics") is None


@pytest.mark.asyncio
async def test_usage_count_reflects_assignment(
    client: AsyncClient, seeded_org: str, published_project_id: str
) -> None:
    analytics_id = await _create_instance(client, seeded_org)
    await client.put(
        f"/api/v2/project/{published_project_id}/public/tracking",
        json={"analytics_id": analytics_id},
    )

    r = await client.get(f"/api/v2/organizations/{seeded_org}/analytics/")
    assert r.status_code == 200
    (item,) = r.json()
    assert item["usage_count"] == 1


@pytest.mark.asyncio
async def test_delete_instance_clears_project_reference(
    client: AsyncClient,
    db_session: AsyncSession,
    seeded_org: str,
    published_project_id: str,
) -> None:
    analytics_id = await _create_instance(client, seeded_org)
    await client.put(
        f"/api/v2/project/{published_project_id}/public/tracking",
        json={"analytics_id": analytics_id},
    )

    r = await client.delete(
        f"/api/v2/organizations/{seeded_org}/analytics/{analytics_id}"
    )
    assert r.status_code == 204

    result = await db_session.execute(
        select(ProjectPublic).where(
            ProjectPublic.project_id == UUID(published_project_id)
        )
    )
    row = result.scalar_one()
    await db_session.refresh(row)
    assert row.analytics_id is None
