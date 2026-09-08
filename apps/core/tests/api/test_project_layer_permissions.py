"""A project viewer may look, not add. The SQL gate cannot express this because
get_needed_roles unions the roles behind every listed permission and project-viewer
inherits read-layer; the endpoint has to hold the line."""

from typing import Any
from uuid import UUID, uuid4

import pytest
from core.core.config import settings
from core.crud.crud_space import space as crud_space
from core.db.models._link_model import ResourceGrant, UserTeamLink
from core.db.models.folder import Folder
from core.db.models.layer import Layer
from core.db.models.team import Team
from core.db.models.user import User
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

S = settings.SCHEMA


async def _role_id(db: AsyncSession, name: str, resource_type: str = "project") -> UUID:
    rid = (
        await db.execute(text(f"SELECT id FROM {S}.role WHERE name = :n"), {"n": name})
    ).scalar_one_or_none()
    if rid is None:
        rid = (
            await db.execute(
                text(
                    f"INSERT INTO {S}.role (name, resource_type) "
                    f"VALUES (:n, :rt) RETURNING id"
                ),
                {"n": name, "rt": resource_type},
            )
        ).scalar_one()
    return UUID(str(rid))


async def _grant_project(
    db: AsyncSession,
    *,
    project_id: UUID,
    grantee_type: str,
    grantee_id: UUID,
    role_id: UUID,
    granted_by: UUID,
) -> None:
    db.add(
        ResourceGrant(
            resource_type="project",
            resource_id=project_id,
            grantee_type=grantee_type,
            grantee_id=grantee_id,
            role_id=role_id,
            granted_by=granted_by,
        )
    )
    await db.flush()


async def _project_owned_by_someone_else(
    client: AsyncClient,
    db: AsyncSession,
    me: UUID,
    home_folder_id: UUID,
    my_role: str,
) -> tuple[UUID, UUID]:
    created = await client.post(
        f"{settings.API_V2_STR}/project",
        json={
            "name": "shared",
            "folder_id": str(home_folder_id),
            "initial_view_state": {
                "latitude": 48.1502132,
                "longitude": 11.5696284,
                "zoom": 12,
                "min_zoom": 0,
                "max_zoom": 20,
                "bearing": 0,
                "pitch": 0,
            },
        },
    )
    assert created.status_code in (200, 201), created.text
    project_id = UUID(created.json()["id"])
    other = User(
        id=uuid4(),
        email=f"own-{uuid4().hex[:6]}@goat.test",
        firstname="O",
        lastname="W",
        avatar="",
    )
    db.add(other)
    await db.flush()
    other_id = other.id
    # Re-home the project into a folder `other` owns, in `other`'s own
    # space: `effective_role` treats the space owner as the resource's
    # owner, so leaving the project in `me`'s own personal space would let
    # that path dominate the direct grant under test below.
    other_space_id = (await crud_space.ensure_personal(db, other_id)).id
    other_folder = Folder(
        id=uuid4(),
        user_id=other_id,
        space_id=other_space_id,
        name="other-home",
    )
    db.add(other_folder)
    await db.flush()
    other_folder_id = other_folder.id
    await db.execute(
        text(
            f"UPDATE {S}.project SET user_id = :o, folder_id = :f, space_id = :s WHERE id = :p"
        ),
        {
            "o": str(other_id),
            "f": str(other_folder_id),
            "s": str(other_space_id),
            "p": str(project_id),
        },
    )
    await db.execute(
        text(
            f"DELETE FROM {S}.resource_grant "
            f"WHERE resource_type = 'project' AND resource_id = :p"
        ),
        {"p": str(project_id)},
    )
    await _grant_project(
        db,
        project_id=project_id,
        grantee_type="user",
        grantee_id=me,
        role_id=await _role_id(db, my_role),
        granted_by=other_id,
    )
    layer = Layer(
        id=uuid4(),
        user_id=me,
        folder_id=home_folder_id,
        name="l",
        type="feature",
        feature_layer_type="standard",
        feature_layer_geometry_type="point",
    )
    db.add(layer)
    layer_id = layer.id
    await db.commit()
    return project_id, layer_id


@pytest.mark.asyncio
async def test_project_viewer_cannot_add_layers(
    client: AsyncClient,
    db_session: AsyncSession,
    fixture_create_user: Any,
    fixture_get_home_folder: dict[str, Any],
) -> None:
    project_id, layer_id = await _project_owned_by_someone_else(
        client,
        db_session,
        fixture_create_user,
        fixture_get_home_folder["id"],
        "project-viewer",
    )
    response = await client.post(
        f"{settings.API_V2_STR}/project/{project_id}/layer",
        params={"layer_ids": [str(layer_id)]},
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_project_viewer_cannot_add_catalog_items(
    client: AsyncClient,
    db_session: AsyncSession,
    fixture_create_user: Any,
    fixture_get_home_folder: dict[str, Any],
) -> None:
    """`layer-catalog` is seeded with read-layer/update-project, and
    project-viewer inherits read-layer, so the SQL gate alone lets a viewer
    through; the endpoint holds the line with the same owner/editor check as
    `add_layers_to_project`. A minimal (non-existent) catalog id is enough —
    the 403 must fire before the catalog mirror is even looked at."""
    project_id, _layer_id = await _project_owned_by_someone_else(
        client,
        db_session,
        fixture_create_user,
        fixture_get_home_folder["id"],
        "project-viewer",
    )
    response = await client.post(
        f"{settings.API_V2_STR}/project/{project_id}/layer-catalog",
        params={"catalog_ids": ["does-not-matter"]},
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_project_editor_can_add_layers(
    client: AsyncClient,
    db_session: AsyncSession,
    fixture_create_user: Any,
    fixture_get_home_folder: dict[str, Any],
) -> None:
    project_id, layer_id = await _project_owned_by_someone_else(
        client,
        db_session,
        fixture_create_user,
        fixture_get_home_folder["id"],
        "project-editor",
    )
    response = await client.post(
        f"{settings.API_V2_STR}/project/{project_id}/layer",
        params={"layer_ids": [str(layer_id)]},
    )
    assert response.status_code in (200, 201), response.text


@pytest.mark.asyncio
async def test_strongest_role_wins_across_paths(
    client: AsyncClient,
    db_session: AsyncSession,
    fixture_create_user: Any,
    fixture_get_home_folder: dict[str, Any],
) -> None:
    """DEFAULT user holds a direct project-viewer grant (priority 2) AND a
    project-editor grant via team membership (priority 3). Path priority must
    not shadow the stronger role reached by a lower-priority path — the max
    role across paths wins, so this user can add layers and reads back
    my_role == project-editor."""
    project_id, layer_id = await _project_owned_by_someone_else(
        client,
        db_session,
        fixture_create_user,
        fixture_get_home_folder["id"],
        "project-viewer",
    )
    team = Team(id=uuid4(), name=f"t-{uuid4().hex[:6]}", avatar="")
    db_session.add(team)
    await db_session.flush()
    db_session.add(
        UserTeamLink(
            user_id=fixture_create_user,
            team_id=team.id,
            role_id=await _role_id(db_session, "team-member", resource_type="team"),
        )
    )
    await _grant_project(
        db_session,
        project_id=project_id,
        grantee_type="team",
        grantee_id=team.id,
        role_id=await _role_id(db_session, "project-editor"),
        granted_by=fixture_create_user,
    )
    await db_session.commit()

    response = await client.post(
        f"{settings.API_V2_STR}/project/{project_id}/layer",
        params={"layer_ids": [str(layer_id)]},
    )
    assert response.status_code in (200, 201), response.text

    read = await client.get(f"{settings.API_V2_STR}/project/{project_id}")
    assert read.status_code == 200, read.text
    assert read.json()["my_role"] == "project-editor"
