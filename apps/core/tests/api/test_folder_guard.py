"""Content may move into any live folder of ITS OWN space that the caller may
write to (Task 9: `assert_same_space` + `authz.require(..., "write")`,
replacing Phase 0's owner-only `assert_owned_by`) — a folder outside that
space still 404s exactly as it did when the guard was ownership-based, and a
team-mate's folder within a shared team space is now allowed."""

from typing import Any
from uuid import uuid4

import pytest
from core.core.config import settings
from core.crud.crud_space import space as crud_space
from core.db.models._link_model import UserTeamLink
from core.db.models.folder import Folder
from core.db.models.layer import Layer
from core.db.models.project import Project
from core.db.models.space import Space, SpaceDefaultRole, SpaceKind
from core.db.models.team import Team
from core.db.models.user import User
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

S = settings.SCHEMA


async def _role_id(db: AsyncSession, name: str, resource_type: str) -> Any:
    role_id = (
        await db.execute(text(f"SELECT id FROM {S}.role WHERE name = :n"), {"n": name})
    ).scalar_one_or_none()
    if role_id is not None:
        return role_id
    return (
        await db.execute(
            text(
                f"INSERT INTO {S}.role (name, resource_type) "
                "VALUES (:n, :t) RETURNING id"
            ),
            {"n": name, "t": resource_type},
        )
    ).scalar_one()


async def _foreign_folder(db: AsyncSession) -> Folder:
    other = User(
        id=uuid4(),
        email=f"x-{uuid4().hex[:6]}@goat.test",
        firstname="X",
        lastname="Y",
        avatar="",
    )
    db.add(other)
    await db.flush()
    space_id = (await crud_space.ensure_personal(db, other.id)).id
    folder = Folder(id=uuid4(), user_id=other.id, space_id=space_id, name="theirs")
    db.add(folder)
    await db.commit()
    return folder


@pytest.mark.asyncio
async def test_layer_cannot_be_moved_into_a_foreign_folder(
    client: AsyncClient,
    db_session: AsyncSession,
    fixture_create_user: Any,
    fixture_get_home_folder: dict[str, Any],
) -> None:
    me = fixture_create_user
    my_space_id = (await crud_space.ensure_personal(db_session, me)).id
    layer = Layer(
        id=uuid4(),
        user_id=me,
        folder_id=fixture_get_home_folder["id"],
        space_id=my_space_id,
        name="mine",
        type="feature",
        feature_layer_type="standard",
        feature_layer_geometry_type="point",
    )
    db_session.add(layer)
    await db_session.commit()
    theirs = await _foreign_folder(db_session)

    response = await client.put(
        f"{settings.API_V2_STR}/layer/{layer.id}", json={"folder_id": str(theirs.id)}
    )
    assert response.status_code == 404

    mine = Folder(
        id=uuid4(),
        user_id=me,
        space_id=my_space_id,
        name="also mine",
    )
    db_session.add(mine)
    await db_session.commit()
    response = await client.put(
        f"{settings.API_V2_STR}/layer/{layer.id}", json={"folder_id": str(mine.id)}
    )
    assert response.status_code == 200, response.text


@pytest.mark.asyncio
async def test_layer_can_be_moved_into_a_teammates_folder_in_a_shared_team_space(
    client: AsyncClient,
    db_session: AsyncSession,
    fixture_create_user: Any,
) -> None:
    """A team space has many writers — the target folder need only be in the
    content's own (writable) space, not one the caller personally created."""
    me = fixture_create_user
    teammate = User(
        id=uuid4(),
        email=f"tm-{uuid4().hex[:6]}@goat.test",
        firstname="T",
        lastname="M",
        avatar="",
    )
    db_session.add(teammate)
    await db_session.flush()
    team = Team(id=uuid4(), name=f"team-{uuid4().hex[:6]}", avatar="")
    db_session.add(team)
    await db_session.flush()
    member_role_id = await _role_id(db_session, "team-member", "team")
    db_session.add_all(
        [
            UserTeamLink(user_id=me, team_id=team.id, role_id=member_role_id),
            UserTeamLink(user_id=teammate.id, team_id=team.id, role_id=member_role_id),
        ]
    )
    await db_session.flush()
    team_space = await crud_space.ensure_team(db_session, team.id)
    assert team_space.kind == SpaceKind.team

    my_folder_in_team_space = Folder(
        id=uuid4(), user_id=me, space_id=team_space.id, name="mine-in-team"
    )
    teammates_folder = Folder(
        id=uuid4(), user_id=teammate.id, space_id=team_space.id, name="teammate's"
    )
    db_session.add_all([my_folder_in_team_space, teammates_folder])
    await db_session.flush()
    layer = Layer(
        id=uuid4(),
        user_id=me,
        folder_id=my_folder_in_team_space.id,
        space_id=team_space.id,
        name="team layer",
        type="feature",
        feature_layer_type="standard",
        feature_layer_geometry_type="point",
    )
    db_session.add(layer)
    await db_session.commit()

    response = await client.put(
        f"{settings.API_V2_STR}/layer/{layer.id}",
        json={"folder_id": str(teammates_folder.id)},
    )
    assert response.status_code == 200, response.text


@pytest.mark.asyncio
async def test_project_copy_cannot_target_a_foreign_folder(
    client: AsyncClient,
    db_session: AsyncSession,
    fixture_create_user: Any,
    fixture_get_home_folder: dict[str, Any],
) -> None:
    created = await client.post(
        f"{settings.API_V2_STR}/project",
        json={
            "name": "source",
            "folder_id": str(fixture_get_home_folder["id"]),
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
    project_id = created.json()["id"]
    theirs = await _foreign_folder(db_session)

    response = await client.post(
        f"{settings.API_V2_STR}/project/{project_id}/copy",
        json={"folder_id": str(theirs.id)},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_raster_layer_cannot_be_created_in_a_foreign_folder(
    client: AsyncClient,
    db_session: AsyncSession,
    fixture_create_user: Any,
    fixture_get_home_folder: dict[str, Any],
) -> None:
    theirs = await _foreign_folder(db_session)

    response = await client.post(
        f"{settings.API_V2_STR}/layer/raster",
        json={
            "folder_id": str(theirs.id),
            "name": "r",
            "type": "raster",
            "url": "https://example.com/wms",
            "data_type": "wms",
        },
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_project_cannot_be_moved_into_a_foreign_folder(
    client: AsyncClient,
    db_session: AsyncSession,
    fixture_create_user: Any,
    fixture_get_home_folder: dict[str, Any],
) -> None:
    created = await client.post(
        f"{settings.API_V2_STR}/project",
        json={
            "name": "p",
            "folder_id": str(fixture_get_home_folder["id"]),
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
    project_id = created.json()["id"]
    theirs = await _foreign_folder(db_session)

    response = await client.put(
        f"{settings.API_V2_STR}/project/{project_id}",
        json={"folder_id": str(theirs.id)},
    )
    assert response.status_code == 404


async def _team_space(
    db: AsyncSession, member_id: Any, default_role: SpaceDefaultRole
) -> Space:
    """A team space with `member_id` as a plain (non-owner) member holding
    `default_role` — for testing the write-denial half of the folder guard:
    a plain member's effective role on the space's content IS the space's
    `default_role` (`customer.space_rank`), regardless of who created what."""
    team = Team(id=uuid4(), name=f"team-{uuid4().hex[:6]}", avatar="")
    db.add(team)
    await db.flush()
    member_role_id = await _role_id(db, "team-member", "team")
    db.add(UserTeamLink(user_id=member_id, team_id=team.id, role_id=member_role_id))
    space = Space(kind=SpaceKind.team, team_id=team.id, default_role=default_role)
    db.add(space)
    await db.flush()
    return space


@pytest.mark.asyncio
async def test_viewer_cannot_move_a_layer_into_a_writable_same_space_folder(
    client: AsyncClient,
    db_session: AsyncSession,
    fixture_create_user: Any,
) -> None:
    """Same-space is necessary but not sufficient: a team-space VIEWER
    (space default_role = viewer, plain member) may not move content into a
    folder of that space even though `assert_same_space` allows it — the
    `authz.require(..., "write")` half of the guard must still refuse."""
    me = fixture_create_user
    space = await _team_space(db_session, me, SpaceDefaultRole.viewer)
    layer_folder = Folder(id=uuid4(), user_id=me, space_id=space.id, name="layer-home")
    dest = Folder(id=uuid4(), user_id=me, space_id=space.id, name="dest")
    db_session.add_all([layer_folder, dest])
    await db_session.flush()
    layer = Layer(
        id=uuid4(),
        user_id=me,
        folder_id=layer_folder.id,
        space_id=space.id,
        name="l",
        type="feature",
        feature_layer_type="standard",
        feature_layer_geometry_type="point",
    )
    db_session.add(layer)
    await db_session.commit()

    response = await client.put(
        f"{settings.API_V2_STR}/layer/{layer.id}", json={"folder_id": str(dest.id)}
    )
    assert response.status_code == 403, response.text


@pytest.mark.asyncio
async def test_editor_can_move_a_layer_into_a_writable_same_space_folder(
    client: AsyncClient,
    db_session: AsyncSession,
    fixture_create_user: Any,
) -> None:
    me = fixture_create_user
    space = await _team_space(db_session, me, SpaceDefaultRole.editor)
    layer_folder = Folder(id=uuid4(), user_id=me, space_id=space.id, name="layer-home")
    dest = Folder(id=uuid4(), user_id=me, space_id=space.id, name="dest")
    db_session.add_all([layer_folder, dest])
    await db_session.flush()
    layer = Layer(
        id=uuid4(),
        user_id=me,
        folder_id=layer_folder.id,
        space_id=space.id,
        name="l",
        type="feature",
        feature_layer_type="standard",
        feature_layer_geometry_type="point",
    )
    db_session.add(layer)
    await db_session.commit()

    response = await client.put(
        f"{settings.API_V2_STR}/layer/{layer.id}", json={"folder_id": str(dest.id)}
    )
    assert response.status_code == 200, response.text


@pytest.mark.asyncio
async def test_viewer_cannot_move_a_project_into_a_writable_same_space_folder(
    client: AsyncClient,
    db_session: AsyncSession,
    fixture_create_user: Any,
) -> None:
    me = fixture_create_user
    space = await _team_space(db_session, me, SpaceDefaultRole.viewer)
    proj_folder = Folder(id=uuid4(), user_id=me, space_id=space.id, name="proj-home")
    dest = Folder(id=uuid4(), user_id=me, space_id=space.id, name="dest")
    db_session.add_all([proj_folder, dest])
    await db_session.flush()
    project = Project(
        id=uuid4(), user_id=me, folder_id=proj_folder.id, space_id=space.id, name="p"
    )
    db_session.add(project)
    await db_session.commit()

    response = await client.put(
        f"{settings.API_V2_STR}/project/{project.id}",
        json={"folder_id": str(dest.id)},
    )
    assert response.status_code == 403, response.text


@pytest.mark.asyncio
async def test_editor_can_move_a_project_into_a_writable_same_space_folder(
    client: AsyncClient,
    db_session: AsyncSession,
    fixture_create_user: Any,
) -> None:
    me = fixture_create_user
    space = await _team_space(db_session, me, SpaceDefaultRole.editor)
    proj_folder = Folder(id=uuid4(), user_id=me, space_id=space.id, name="proj-home")
    dest = Folder(id=uuid4(), user_id=me, space_id=space.id, name="dest")
    db_session.add_all([proj_folder, dest])
    await db_session.flush()
    project = Project(
        id=uuid4(), user_id=me, folder_id=proj_folder.id, space_id=space.id, name="p"
    )
    db_session.add(project)
    await db_session.commit()

    response = await client.put(
        f"{settings.API_V2_STR}/project/{project.id}",
        json={"folder_id": str(dest.id)},
    )
    assert response.status_code == 200, response.text


@pytest.mark.asyncio
async def test_viewer_cannot_copy_a_project_into_a_writable_same_space_folder(
    client: AsyncClient,
    db_session: AsyncSession,
    fixture_create_user: Any,
) -> None:
    """crud_project_copy's EXPLICIT target-folder path: a viewer may read
    (and thus copy) a project sitting in a team space, but may not place the
    copy into a folder of that space they cannot write to."""
    me = fixture_create_user
    space = await _team_space(db_session, me, SpaceDefaultRole.viewer)
    src_folder = Folder(id=uuid4(), user_id=me, space_id=space.id, name="src")
    dest = Folder(id=uuid4(), user_id=me, space_id=space.id, name="dest")
    db_session.add_all([src_folder, dest])
    await db_session.flush()
    project = Project(
        id=uuid4(), user_id=me, folder_id=src_folder.id, space_id=space.id, name="p"
    )
    db_session.add(project)
    await db_session.commit()

    response = await client.post(
        f"{settings.API_V2_STR}/project/{project.id}/copy",
        json={"folder_id": str(dest.id)},
    )
    assert response.status_code == 403, response.text


@pytest.mark.asyncio
async def test_viewer_cannot_copy_a_project_into_its_own_default_folder_either(
    client: AsyncClient,
    db_session: AsyncSession,
    fixture_create_user: Any,
) -> None:
    """crud_project_copy's DEFAULT path (no explicit `folder_id`) falls back
    to the source project's own folder — that default must get the same
    write check as an explicit choice: read access to the source says
    nothing about write access to the folder it happens to sit in."""
    me = fixture_create_user
    space = await _team_space(db_session, me, SpaceDefaultRole.viewer)
    src_folder = Folder(id=uuid4(), user_id=me, space_id=space.id, name="src")
    db_session.add(src_folder)
    await db_session.flush()
    project = Project(
        id=uuid4(), user_id=me, folder_id=src_folder.id, space_id=space.id, name="p"
    )
    db_session.add(project)
    await db_session.commit()

    response = await client.post(
        f"{settings.API_V2_STR}/project/{project.id}/copy", json={}
    )
    assert response.status_code == 403, response.text


@pytest.mark.asyncio
async def test_editor_can_copy_a_project_into_a_writable_same_space_folder(
    client: AsyncClient,
    db_session: AsyncSession,
    fixture_create_user: Any,
) -> None:
    me = fixture_create_user
    space = await _team_space(db_session, me, SpaceDefaultRole.editor)
    src_folder = Folder(id=uuid4(), user_id=me, space_id=space.id, name="src")
    dest = Folder(id=uuid4(), user_id=me, space_id=space.id, name="dest")
    db_session.add_all([src_folder, dest])
    await db_session.flush()
    project = Project(
        id=uuid4(), user_id=me, folder_id=src_folder.id, space_id=space.id, name="p"
    )
    db_session.add(project)
    await db_session.commit()

    response = await client.post(
        f"{settings.API_V2_STR}/project/{project.id}/copy",
        json={"folder_id": str(dest.id)},
    )
    assert response.status_code == 201, response.text


### POST /project (I7): folder_id must be validated, not merely accepted

_VIEW = {
    "latitude": 48.1502132,
    "longitude": 11.5696284,
    "zoom": 12,
    "min_zoom": 0,
    "max_zoom": 20,
    "bearing": 0,
    "pitch": 0,
}


@pytest.mark.asyncio
async def test_create_project_in_a_foreign_folder_404s(
    client: AsyncClient, db_session: AsyncSession, fixture_create_user: Any
) -> None:
    theirs = await _foreign_folder(db_session)
    response = await client.post(
        f"{settings.API_V2_STR}/project",
        json={"name": "p", "folder_id": str(theirs.id), "initial_view_state": _VIEW},
    )
    assert response.status_code == 404, response.text


@pytest.mark.asyncio
async def test_create_project_in_a_team_folder_as_viewer_403s(
    client: AsyncClient, db_session: AsyncSession, fixture_create_user: Any
) -> None:
    me = fixture_create_user
    space = await _team_space(db_session, me, SpaceDefaultRole.viewer)
    folder = Folder(id=uuid4(), user_id=me, space_id=space.id, name="dest")
    db_session.add(folder)
    await db_session.commit()

    response = await client.post(
        f"{settings.API_V2_STR}/project",
        json={"name": "p", "folder_id": str(folder.id), "initial_view_state": _VIEW},
    )
    assert response.status_code == 403, response.text


@pytest.mark.asyncio
async def test_create_project_in_a_team_folder_as_editor_takes_the_team_space(
    client: AsyncClient, db_session: AsyncSession, fixture_create_user: Any
) -> None:
    me = fixture_create_user
    space = await _team_space(db_session, me, SpaceDefaultRole.editor)
    folder = Folder(id=uuid4(), user_id=me, space_id=space.id, name="dest")
    db_session.add(folder)
    await db_session.commit()

    response = await client.post(
        f"{settings.API_V2_STR}/project",
        json={"name": "p", "folder_id": str(folder.id), "initial_view_state": _VIEW},
    )
    assert response.status_code == 201, response.text
    project_space_id = (
        await db_session.execute(
            text(f"SELECT space_id FROM {settings.SCHEMA}.project WHERE id = :p"),
            {"p": response.json()["id"]},
        )
    ).scalar_one()
    assert project_space_id == space.id, "takes the folder's own space, not personal"


### POST /bundle/import (I7): folder_id must be validated too


def _import_bundle_url() -> str:
    return f"{settings.API_V2_STR}/bundle/import"


def _import_payload(user_id: Any, folder_id: Any) -> dict[str, Any]:
    from core.services.s3 import s3_service

    upload_prefix = s3_service.build_s3_key(
        settings.S3_BUCKET_PATH, "users", str(user_id), "imports", "uploads"
    )
    return {
        "s3_key": f"{upload_prefix}/gtfs.zip",
        "folder_id": str(folder_id),
        "name": "b",
    }


@pytest.mark.asyncio
async def test_import_bundle_into_a_foreign_folder_404s(
    client: AsyncClient, db_session: AsyncSession, fixture_create_user: Any
) -> None:
    theirs = await _foreign_folder(db_session)
    response = await client.post(
        _import_bundle_url(),
        json=_import_payload(fixture_create_user, theirs.id),
    )
    assert response.status_code == 404, response.text


@pytest.mark.asyncio
async def test_import_bundle_into_a_team_folder_as_viewer_403s(
    client: AsyncClient, db_session: AsyncSession, fixture_create_user: Any
) -> None:
    me = fixture_create_user
    space = await _team_space(db_session, me, SpaceDefaultRole.viewer)
    folder = Folder(id=uuid4(), user_id=me, space_id=space.id, name="dest")
    db_session.add(folder)
    await db_session.commit()

    response = await client.post(
        _import_bundle_url(), json=_import_payload(me, folder.id)
    )
    assert response.status_code == 403, response.text


@pytest.mark.asyncio
async def test_import_bundle_into_a_team_folder_as_editor_passes_the_folder_gate(
    client: AsyncClient, db_session: AsyncSession, fixture_create_user: Any
) -> None:
    """The folder gate (existence, live, write access) must pass for a team
    folder the caller may write to — the import then fails downstream (no
    real S3 object sits behind the key in this test), which happens to also
    be a 404; distinguish it from the gate's own 404 by message rather than
    status code."""
    me = fixture_create_user
    space = await _team_space(db_session, me, SpaceDefaultRole.editor)
    folder = Folder(id=uuid4(), user_id=me, space_id=space.id, name="dest")
    db_session.add(folder)
    await db_session.commit()

    response = await client.post(
        _import_bundle_url(), json=_import_payload(me, folder.id)
    )
    assert "folder" not in response.text.lower(), response.text
