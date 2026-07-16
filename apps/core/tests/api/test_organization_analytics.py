"""Endpoint tests for /organizations/{org_id}/analytics/.

An organization can hold any number of analytics instances; these tests
cover the collection CRUD. usage_count behaviour with real dashboard
assignments is covered in test_project_tracking.py.
"""

from typing import Any

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession


@pytest.fixture
async def seeded_org(db_session: AsyncSession) -> str:
    """Insert a test Organization and return its id as a string."""
    from tests.utils import make_organization

    org = make_organization(name="test-org-for-analytics")
    db_session.add(org)
    await db_session.commit()
    await db_session.refresh(org)
    return str(org.id)


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


@pytest.mark.asyncio
async def test_list_empty(client: AsyncClient, seeded_org: str) -> None:
    r = await client.get(f"/api/v2/organizations/{seeded_org}/analytics/")
    assert r.status_code == 200
    assert r.json() == []


@pytest.mark.asyncio
async def test_create_instance(client: AsyncClient, seeded_org: str) -> None:
    r = await client.post(
        f"/api/v2/organizations/{seeded_org}/analytics/",
        json=_matomo_payload(),
    )
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["name"] == "P4B Matomo"
    assert body["provider"] == "matomo"
    assert body["config"]["site_id"] == "5"
    assert body["usage_count"] == 0


@pytest.mark.asyncio
async def test_create_multiple_instances_same_org(
    client: AsyncClient, seeded_org: str
) -> None:
    for name, site in (("Our Matomo", "1"), ("Client XY Matomo", "2")):
        r = await client.post(
            f"/api/v2/organizations/{seeded_org}/analytics/",
            json=_matomo_payload(name=name, site_id=site),
        )
        assert r.status_code == 201, r.text

    r = await client.get(f"/api/v2/organizations/{seeded_org}/analytics/")
    assert r.status_code == 200
    names = sorted(item["name"] for item in r.json())
    assert names == ["Client XY Matomo", "Our Matomo"]


@pytest.mark.asyncio
async def test_create_rejects_http_url(client: AsyncClient, seeded_org: str) -> None:
    payload = _matomo_payload()
    payload["config"]["url"] = "http://matomo.example.org/"
    r = await client.post(
        f"/api/v2/organizations/{seeded_org}/analytics/", json=payload
    )
    assert r.status_code == 422


@pytest.mark.asyncio
async def test_update_instance(client: AsyncClient, seeded_org: str) -> None:
    r = await client.post(
        f"/api/v2/organizations/{seeded_org}/analytics/",
        json=_matomo_payload(),
    )
    analytics_id = r.json()["id"]

    r = await client.put(
        f"/api/v2/organizations/{seeded_org}/analytics/{analytics_id}",
        json=_matomo_payload(name="Renamed", site_id="9"),
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["name"] == "Renamed"
    assert body["config"]["site_id"] == "9"


@pytest.mark.asyncio
async def test_update_404_for_unknown_id(client: AsyncClient, seeded_org: str) -> None:
    fake_id = "00000000-0000-0000-0000-000000000099"
    r = await client.put(
        f"/api/v2/organizations/{seeded_org}/analytics/{fake_id}",
        json=_matomo_payload(),
    )
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_update_404_for_foreign_org_instance(
    client: AsyncClient, seeded_org: str, db_session: AsyncSession
) -> None:
    """An instance belonging to another org must not be reachable."""
    from tests.utils import make_organization

    other_org = make_organization(name="other-org-for-analytics")
    db_session.add(other_org)
    await db_session.commit()
    await db_session.refresh(other_org)

    r = await client.post(
        f"/api/v2/organizations/{other_org.id}/analytics/",
        json=_matomo_payload(name="Other org instance"),
    )
    analytics_id = r.json()["id"]

    r = await client.put(
        f"/api/v2/organizations/{seeded_org}/analytics/{analytics_id}",
        json=_matomo_payload(name="Hijack attempt"),
    )
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_delete_instance(client: AsyncClient, seeded_org: str) -> None:
    r = await client.post(
        f"/api/v2/organizations/{seeded_org}/analytics/",
        json=_matomo_payload(),
    )
    analytics_id = r.json()["id"]

    r = await client.delete(
        f"/api/v2/organizations/{seeded_org}/analytics/{analytics_id}"
    )
    assert r.status_code == 204

    r = await client.get(f"/api/v2/organizations/{seeded_org}/analytics/")
    assert r.json() == []


@pytest.mark.asyncio
async def test_delete_404_for_unknown_id(client: AsyncClient, seeded_org: str) -> None:
    fake_id = "00000000-0000-0000-0000-000000000099"
    r = await client.delete(f"/api/v2/organizations/{seeded_org}/analytics/{fake_id}")
    assert r.status_code == 404
