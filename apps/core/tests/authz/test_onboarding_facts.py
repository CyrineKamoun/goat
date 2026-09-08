"""Home derives its stage and checklist from facts, not stored flags (H2, H9)."""

from typing import Awaitable, Callable
from uuid import UUID

import pytest
from core.core.config import settings
from core.crud.crud_onboarding import onboarding as crud_onboarding
from core.db.models.folder import Folder
from core.db.models.layer import Layer
from core.db.models.project import Project
from core.db.models.team import Team
from core.db.models.user import User
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

S = settings.SCHEMA
URL = f"{settings.API_V2_STR}/users/me/onboarding"


@pytest.mark.asyncio
async def test_facts_flip_as_content_appears(
    client: AsyncClient, fixture_get_home_folder: dict
) -> None:
    r = await client.get(URL)
    assert r.status_code == 200, r.text
    assert r.json() == {
        "has_project": False,
        "has_uploaded_layer": False,
        "has_catalog_layer": False,
        "has_team": False,
        "has_workflow": False,
    }

    p = await client.post(
        f"{settings.API_V2_STR}/project",
        json={
            "name": "first",
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
    assert p.status_code in (200, 201), p.text
    assert (await client.get(URL)).json()["has_project"] is True


@pytest.mark.asyncio
async def test_team_membership_flips_has_team(
    client: AsyncClient,
    db_session: AsyncSession,
    fixture_create_user: UUID,
    make_team: Callable[..., Awaitable[Team]],
) -> None:
    r = await client.get(URL)
    assert r.json()["has_team"] is False

    me = await db_session.get(User, fixture_create_user)
    await make_team(me)
    await db_session.commit()

    r = await client.get(URL)
    assert r.json()["has_team"] is True


@pytest.mark.asyncio
async def test_catalog_layer_is_not_counted_as_uploaded(
    client: AsyncClient,
    db_session: AsyncSession,
    fixture_create_user: UUID,
    make_folder: Callable[..., Awaitable[Folder]],
    make_layer: Callable[..., Awaitable[Layer]],
) -> None:
    me = await db_session.get(User, fixture_create_user)
    folder = await make_folder(me)
    layer = await make_layer(me, folder)
    await db_session.commit()

    r = await client.get(URL)
    body = r.json()
    assert body["has_uploaded_layer"] is True
    assert body["has_catalog_layer"] is False

    await db_session.execute(
        text(f"UPDATE {S}.layer SET catalog_external_uid = 'x' WHERE id = :id"),
        {"id": layer.id},
    )
    await db_session.commit()

    r = await client.get(URL)
    body = r.json()
    assert body["has_catalog_layer"] is True
    assert body["has_uploaded_layer"] is False


@pytest.mark.asyncio
async def test_workflow_creation_flips_has_workflow(
    client: AsyncClient, fixture_get_home_folder: dict
) -> None:
    assert (await client.get(URL)).json()["has_workflow"] is False

    p = await client.post(
        f"{settings.API_V2_STR}/project",
        json={
            "name": "workflow-project",
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
    assert p.status_code in (200, 201), p.text
    project_id = p.json()["id"]

    w = await client.post(
        f"{settings.API_V2_STR}/project/{project_id}/workflow",
        json={"name": "my-workflow", "config": {"nodes": [], "edges": []}},
    )
    assert w.status_code in (200, 201), w.text
    assert (await client.get(URL)).json()["has_workflow"] is True


@pytest.mark.asyncio
async def test_frozen_template_source_does_not_count_as_a_project(
    db_session: AsyncSession,
    make_user: Callable[..., Awaitable[User]],
    make_folder: Callable[..., Awaitable[Folder]],
    make_project: Callable[..., Awaitable[Project]],
) -> None:
    """A project marked `is_template_source` is the hidden frozen copy behind
    a template (T2) — invisible in every listing, so it must not be the reason
    Home stops asking for a first project.

    Read through the CRUD rather than the route: the route acts as the shared
    default identity, whose spaces other tests in this session leave projects
    in, and this needs a user with nothing but the one project.
    """
    me = await make_user()
    folder = await make_folder(me)
    project = await make_project(me, folder)
    await db_session.commit()

    assert (await crud_onboarding.facts(db_session, me.id)).has_project is True

    await db_session.execute(
        text(f"UPDATE {S}.project SET is_template_source = true WHERE id = :i"),
        {"i": project.id},
    )
    await db_session.commit()

    assert (await crud_onboarding.facts(db_session, me.id)).has_project is False
