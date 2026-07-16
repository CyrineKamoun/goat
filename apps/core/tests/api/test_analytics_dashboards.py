"""Endpoint tests for admin bulk-assignment of dashboards to analytics
instances.

GET  /organizations/{org_id}/analytics/dashboards          — list published
PUT  /organizations/{org_id}/analytics/{id}/dashboards     — reconcile set
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

    org = make_organization(name="test-org-for-bulk-analytics")
    db_session.add(org)
    await db_session.commit()
    await db_session.refresh(org)
    return str(org.id)


@pytest.fixture
async def org_member_user(
    db_session: AsyncSession, fixture_create_user: UUID, seeded_org: str
) -> UUID:
    user = await db_session.get(User, fixture_create_user)
    assert user is not None
    user.organization_id = UUID(seeded_org)
    await db_session.commit()
    return fixture_create_user


def _matomo_payload(name: str = "P4B Matomo", site_id: str = "5") -> dict[str, Any]:
    return {
        "name": name,
        "provider": "matomo",
        "config": {
            "provider": "matomo",
            "url": "https://matomo.example.org/",
            "site_id": site_id,
        },
    }


async def _create_instance(client: AsyncClient, org_id: str, name: str) -> str:
    r = await client.post(
        f"/api/v2/organizations/{org_id}/analytics/",
        json=_matomo_payload(name=name),
    )
    assert r.status_code == 201, r.text
    return r.json()["id"]


async def _create_published_project(
    client: AsyncClient, folder_id: str, name: str
) -> str:
    payload = {
        "folder_id": folder_id,
        "name": name,
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
    r = await client.post("/api/v2/project", json=payload)
    assert r.status_code in (200, 201), r.text
    project_id = r.json()["id"]
    r = await client.post(f"/api/v2/project/{project_id}/publish")
    assert r.status_code in (200, 201), r.text
    return project_id


async def _get_project_public(
    db_session: AsyncSession, project_id: str
) -> ProjectPublic:
    result = await db_session.execute(
        select(ProjectPublic).where(ProjectPublic.project_id == UUID(project_id))
    )
    row = result.scalar_one()
    await db_session.refresh(row)
    return row


@pytest.mark.asyncio
async def test_list_dashboards_empty(
    client: AsyncClient, seeded_org: str, org_member_user: UUID
) -> None:
    r = await client.get(f"/api/v2/organizations/{seeded_org}/analytics/dashboards")
    assert r.status_code == 200
    assert r.json() == []


@pytest.mark.asyncio
async def test_list_dashboards_with_assignment(
    client: AsyncClient,
    seeded_org: str,
    org_member_user: UUID,
    fixture_create_folder: dict,
) -> None:
    instance_id = await _create_instance(client, seeded_org, "Our Matomo")
    p_a = await _create_published_project(
        client, fixture_create_folder["id"], "A dashboard"
    )
    p_b = await _create_published_project(
        client, fixture_create_folder["id"], "B dashboard"
    )

    r = await client.put(
        f"/api/v2/project/{p_a}/public/tracking",
        json={"analytics_id": instance_id},
    )
    assert r.status_code == 200, r.text

    r = await client.get(f"/api/v2/organizations/{seeded_org}/analytics/dashboards")
    assert r.status_code == 200
    body = r.json()
    assert [d["name"] for d in body] == ["A dashboard", "B dashboard"]
    by_id = {d["project_id"]: d for d in body}
    assert by_id[p_a]["analytics_id"] == instance_id
    assert by_id[p_b]["analytics_id"] is None


@pytest.mark.asyncio
async def test_put_assigns_listed(
    client: AsyncClient,
    seeded_org: str,
    org_member_user: UUID,
    fixture_create_folder: dict,
) -> None:
    instance_id = await _create_instance(client, seeded_org, "Our Matomo")
    p_a = await _create_published_project(
        client, fixture_create_folder["id"], "A dashboard"
    )
    p_b = await _create_published_project(
        client, fixture_create_folder["id"], "B dashboard"
    )

    r = await client.put(
        f"/api/v2/organizations/{seeded_org}/analytics/{instance_id}/dashboards",
        json={"project_ids": [p_a, p_b]},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert all(d["analytics_id"] == instance_id for d in body)


@pytest.mark.asyncio
async def test_put_steals_from_other_instance(
    client: AsyncClient,
    seeded_org: str,
    org_member_user: UUID,
    fixture_create_folder: dict,
) -> None:
    instance_a = await _create_instance(client, seeded_org, "Instance A")
    instance_b = await _create_instance(client, seeded_org, "Instance B")
    p = await _create_published_project(
        client, fixture_create_folder["id"], "Shared dashboard"
    )

    r = await client.put(
        f"/api/v2/organizations/{seeded_org}/analytics/{instance_a}/dashboards",
        json={"project_ids": [p]},
    )
    assert r.status_code == 200

    r = await client.put(
        f"/api/v2/organizations/{seeded_org}/analytics/{instance_b}/dashboards",
        json={"project_ids": [p]},
    )
    assert r.status_code == 200
    (row,) = [d for d in r.json() if d["project_id"] == p]
    assert row["analytics_id"] == instance_b


@pytest.mark.asyncio
async def test_put_empty_clears_only_this_instance(
    client: AsyncClient,
    seeded_org: str,
    org_member_user: UUID,
    fixture_create_folder: dict,
) -> None:
    instance_a = await _create_instance(client, seeded_org, "Instance A")
    instance_b = await _create_instance(client, seeded_org, "Instance B")
    p_a = await _create_published_project(client, fixture_create_folder["id"], "On A")
    p_b = await _create_published_project(client, fixture_create_folder["id"], "On B")

    await client.put(
        f"/api/v2/organizations/{seeded_org}/analytics/{instance_a}/dashboards",
        json={"project_ids": [p_a]},
    )
    await client.put(
        f"/api/v2/organizations/{seeded_org}/analytics/{instance_b}/dashboards",
        json={"project_ids": [p_b]},
    )

    r = await client.put(
        f"/api/v2/organizations/{seeded_org}/analytics/{instance_a}/dashboards",
        json={"project_ids": []},
    )
    assert r.status_code == 200
    by_id = {d["project_id"]: d for d in r.json()}
    assert by_id[p_a]["analytics_id"] is None
    assert by_id[p_b]["analytics_id"] == instance_b


@pytest.mark.asyncio
async def test_put_invalid_project_400_atomic(
    client: AsyncClient,
    seeded_org: str,
    org_member_user: UUID,
    fixture_create_folder: dict,
    db_session: AsyncSession,
) -> None:
    instance_id = await _create_instance(client, seeded_org, "Our Matomo")
    p = await _create_published_project(
        client, fixture_create_folder["id"], "Valid dashboard"
    )
    fake_id = "d2719f2a-8a4b-4f4e-9c1b-000000000099"

    r = await client.put(
        f"/api/v2/organizations/{seeded_org}/analytics/{instance_id}/dashboards",
        json={"project_ids": [p, fake_id]},
    )
    assert r.status_code == 400
    assert fake_id in r.json()["detail"]

    row = await _get_project_public(db_session, p)
    assert row.analytics_id is None


@pytest.mark.asyncio
async def test_put_unpublished_project_400(
    client: AsyncClient,
    seeded_org: str,
    org_member_user: UUID,
    fixture_create_folder: dict,
) -> None:
    instance_id = await _create_instance(client, seeded_org, "Our Matomo")
    payload = {
        "folder_id": fixture_create_folder["id"],
        "name": "Unpublished project",
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
    r = await client.post("/api/v2/project", json=payload)
    unpublished_id = r.json()["id"]

    r = await client.put(
        f"/api/v2/organizations/{seeded_org}/analytics/{instance_id}/dashboards",
        json={"project_ids": [unpublished_id]},
    )
    assert r.status_code == 400


@pytest.mark.asyncio
async def test_put_foreign_org_project_400(
    client: AsyncClient,
    seeded_org: str,
    org_member_user: UUID,
    fixture_create_folder: dict,
    db_session: AsyncSession,
) -> None:
    """A dashboard owned by org1 must not be assignable through org2's
    endpoint path (org2's instance)."""
    from tests.utils import make_organization

    p = await _create_published_project(
        client, fixture_create_folder["id"], "Org1 dashboard"
    )

    org2 = make_organization(name="other-org-for-bulk-analytics")
    db_session.add(org2)
    await db_session.commit()
    await db_session.refresh(org2)
    instance2 = await _create_instance(client, str(org2.id), "Org2 Matomo")

    r = await client.put(
        f"/api/v2/organizations/{org2.id}/analytics/{instance2}/dashboards",
        json={"project_ids": [p]},
    )
    assert r.status_code == 400

    row = await _get_project_public(db_session, p)
    assert row.analytics_id is None


@pytest.mark.asyncio
async def test_put_consent_untouched(
    client: AsyncClient,
    seeded_org: str,
    org_member_user: UUID,
    fixture_create_folder: dict,
    db_session: AsyncSession,
) -> None:
    instance_id = await _create_instance(client, seeded_org, "Our Matomo")
    p = await _create_published_project(
        client, fixture_create_folder["id"], "Consent dashboard"
    )

    r = await client.put(
        f"/api/v2/project/{p}/public/tracking",
        json={"analytics_id": instance_id, "require_consent": False},
    )
    assert r.status_code == 200

    await client.put(
        f"/api/v2/organizations/{seeded_org}/analytics/{instance_id}/dashboards",
        json={"project_ids": []},
    )
    row = await _get_project_public(db_session, p)
    assert row.analytics_id is None
    assert row.tracking_require_consent is False

    await client.put(
        f"/api/v2/organizations/{seeded_org}/analytics/{instance_id}/dashboards",
        json={"project_ids": [p]},
    )
    row = await _get_project_public(db_session, p)
    assert str(row.analytics_id) == instance_id
    assert row.tracking_require_consent is False


@pytest.mark.asyncio
async def test_put_unknown_instance_404(
    client: AsyncClient, seeded_org: str, org_member_user: UUID
) -> None:
    fake_instance = "d2719f2a-8a4b-4f4e-9c1b-000000000042"
    r = await client.put(
        f"/api/v2/organizations/{seeded_org}/analytics/{fake_instance}/dashboards",
        json={"project_ids": []},
    )
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_usage_count_reflects_bulk_assignment(
    client: AsyncClient,
    seeded_org: str,
    org_member_user: UUID,
    fixture_create_folder: dict,
) -> None:
    instance_id = await _create_instance(client, seeded_org, "Our Matomo")
    p_a = await _create_published_project(
        client, fixture_create_folder["id"], "A dashboard"
    )
    p_b = await _create_published_project(
        client, fixture_create_folder["id"], "B dashboard"
    )

    await client.put(
        f"/api/v2/organizations/{seeded_org}/analytics/{instance_id}/dashboards",
        json={"project_ids": [p_a, p_b]},
    )

    r = await client.get(f"/api/v2/organizations/{seeded_org}/analytics/")
    (item,) = r.json()
    assert item["usage_count"] == 2
