"""POST /share/... is a full replace written to resource_grant, and GET reads it back."""

from collections.abc import Awaitable, Callable, Sequence
from typing import Any
from uuid import UUID, uuid4

import pytest
from core.core.config import settings
from core.db.models.layer import Layer
from core.db.models.organization import Organization
from core.db.models.team import Team
from core.db.models.user import User
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

S = settings.SCHEMA


async def _rows(db: AsyncSession, rid: UUID | str) -> Sequence[Any]:
    return (
        await db.execute(
            text(
                f"SELECT grantee_type, grantee_id::text, r.name FROM {S}.resource_grant rg "
                f"JOIN {S}.role r ON r.id = rg.role_id WHERE rg.resource_id = :r ORDER BY 1,2"
            ),
            {"r": rid},
        )
    ).all()


@pytest.mark.asyncio
async def test_share_layer_writes_grants_and_replaces(
    client: AsyncClient,
    db_session: AsyncSession,
    roles: dict[str, UUID],
    fixture_create_user: Any,
    fixture_get_home_folder: dict[str, Any],
    make_user: Callable[..., Awaitable[User]],
    make_org: Callable[[], Awaitable[Organization]],
    make_team: Callable[..., Awaitable[Team]],
) -> None:
    me = fixture_create_user
    org = await make_org()
    await db_session.execute(
        text(f'UPDATE {S}."user" SET organization_id = :o WHERE id = :u'),
        {"o": org.id, "u": me},
    )
    friend = await make_user(org.id)
    team = await make_team(friend)
    layer = Layer(
        id=uuid4(),
        user_id=me,
        folder_id=fixture_get_home_folder["id"],
        name="l",
        type="feature",
        feature_layer_type="standard",
        feature_layer_geometry_type="point",
    )
    db_session.add(layer)
    await db_session.commit()
    assert layer.id is not None
    lid: UUID = layer.id

    body = {
        "teams": [{"id": str(team.id), "role": "layer-viewer"}],
        "users": [{"id": str(friend.id), "role": "layer-editor"}],
    }
    r = await client.post(
        f"{settings.API_V2_STR}/share/layer/{lid}",
        params={"team_ids": [str(team.id)]},
        json=body,
    )
    assert r.status_code == 200, r.text
    assert await _rows(db_session, lid) == [
        ("team", str(team.id), "layer-viewer"),
        ("user", str(friend.id), "layer-editor"),
    ]

    # I6: a family absent from the payload is left untouched, so a full
    # replace across all three families must say so explicitly — send `[]`
    # for teams/users to clear them alongside the new organizations grant.
    r = await client.post(
        f"{settings.API_V2_STR}/share/layer/{lid}",
        json={
            "organizations": [{"id": str(org.id), "role": "layer-viewer"}],
            "teams": [],
            "users": [],
        },
        params={"organization_ids": [str(org.id)]},
    )
    assert r.status_code == 200, r.text
    assert await _rows(db_session, lid) == [
        ("organization", str(org.id), "layer-viewer")
    ], "replace, not merge"

    g = await client.get(f"{settings.API_V2_STR}/share/layer/{lid}")
    assert g.status_code == 200
    assert g.json()["organizations"][0]["id"] == str(org.id)


@pytest.mark.asyncio
async def test_malformed_grantee_id_is_rejected_and_keeps_existing_grants(
    client: AsyncClient,
    db_session: AsyncSession,
    roles: dict[str, UUID],
    fixture_create_user: Any,
    fixture_get_home_folder: dict[str, Any],
    make_user: Callable[..., Awaitable[User]],
    make_org: Callable[[], Awaitable[Organization]],
    make_team: Callable[..., Awaitable[Team]],
) -> None:
    me = fixture_create_user
    org = await make_org()
    await db_session.execute(
        text(f'UPDATE {S}."user" SET organization_id = :o WHERE id = :u'),
        {"o": org.id, "u": me},
    )
    friend = await make_user(org.id)
    team = await make_team(friend)
    layer = Layer(
        id=uuid4(),
        user_id=me,
        folder_id=fixture_get_home_folder["id"],
        name="l",
        type="feature",
        feature_layer_type="standard",
        feature_layer_geometry_type="point",
    )
    db_session.add(layer)
    await db_session.commit()
    assert layer.id is not None
    lid: UUID = layer.id

    r = await client.post(
        f"{settings.API_V2_STR}/share/layer/{lid}",
        params={"team_ids": [str(team.id)]},
        json={"teams": [{"id": str(team.id), "role": "layer-viewer"}]},
    )
    assert r.status_code == 200, r.text
    original = [("team", str(team.id), "layer-viewer")]
    assert await _rows(db_session, lid) == original

    r = await client.post(
        f"{settings.API_V2_STR}/share/layer/{lid}",
        params={"team_ids": [str(team.id)]},
        json={
            "teams": [{"id": str(team.id), "role": "layer-viewer"}],
            "users": [{"id": "not-a-uuid", "role": "layer-editor"}],
        },
    )
    assert r.status_code == 400, r.text
    assert await _rows(db_session, lid) == original, "existing grants must survive"


@pytest.mark.asyncio
async def test_wrong_role_family_is_rejected_and_keeps_existing_grants(
    client: AsyncClient,
    db_session: AsyncSession,
    roles: dict[str, UUID],
    fixture_create_user: Any,
    fixture_get_home_folder: dict[str, Any],
    make_user: Callable[..., Awaitable[User]],
    make_org: Callable[[], Awaitable[Organization]],
    make_team: Callable[..., Awaitable[Team]],
) -> None:
    me = fixture_create_user
    org = await make_org()
    await db_session.execute(
        text(f'UPDATE {S}."user" SET organization_id = :o WHERE id = :u'),
        {"o": org.id, "u": me},
    )
    friend = await make_user(org.id)
    team = await make_team(friend)
    layer = Layer(
        id=uuid4(),
        user_id=me,
        folder_id=fixture_get_home_folder["id"],
        name="l",
        type="feature",
        feature_layer_type="standard",
        feature_layer_geometry_type="point",
    )
    db_session.add(layer)
    await db_session.commit()
    assert layer.id is not None
    lid: UUID = layer.id

    r = await client.post(
        f"{settings.API_V2_STR}/share/layer/{lid}",
        params={"team_ids": [str(team.id)]},
        json={"teams": [{"id": str(team.id), "role": "layer-viewer"}]},
    )
    assert r.status_code == 200, r.text
    original = [("team", str(team.id), "layer-viewer")]
    assert await _rows(db_session, lid) == original

    r = await client.post(
        f"{settings.API_V2_STR}/share/layer/{lid}",
        params={"team_ids": [str(team.id)]},
        json={
            "teams": [{"id": str(team.id), "role": "layer-viewer"}],
            "users": [{"id": str(friend.id), "role": "project-editor"}],
        },
    )
    assert r.status_code in (400, 422), r.text
    assert await _rows(db_session, lid) == original, "existing grants must survive"


@pytest.mark.asyncio
async def test_duplicate_grantee_in_payload_is_rejected_and_keeps_existing_grants(
    client: AsyncClient,
    db_session: AsyncSession,
    roles: dict[str, UUID],
    fixture_create_user: Any,
    fixture_get_home_folder: dict[str, Any],
    make_user: Callable[..., Awaitable[User]],
    make_org: Callable[[], Awaitable[Organization]],
    make_team: Callable[..., Awaitable[Team]],
) -> None:
    """C1: the same grantee twice in one payload must 400 before any write —
    under AUTOCOMMIT, a delete-then-insert flow would have already committed
    the delete and then 500'd on the unique constraint, wiping every grant."""
    me = fixture_create_user
    org = await make_org()
    await db_session.execute(
        text(f'UPDATE {S}."user" SET organization_id = :o WHERE id = :u'),
        {"o": org.id, "u": me},
    )
    friend = await make_user(org.id)
    team = await make_team(friend)
    layer = Layer(
        id=uuid4(),
        user_id=me,
        folder_id=fixture_get_home_folder["id"],
        name="l",
        type="feature",
        feature_layer_type="standard",
        feature_layer_geometry_type="point",
    )
    db_session.add(layer)
    await db_session.commit()
    assert layer.id is not None
    lid: UUID = layer.id

    r = await client.post(
        f"{settings.API_V2_STR}/share/layer/{lid}",
        params={"team_ids": [str(team.id)]},
        json={"teams": [{"id": str(team.id), "role": "layer-viewer"}]},
    )
    assert r.status_code == 200, r.text
    original = [("team", str(team.id), "layer-viewer")]
    assert await _rows(db_session, lid) == original

    r = await client.post(
        f"{settings.API_V2_STR}/share/layer/{lid}",
        params={"team_ids": [str(team.id)]},
        json={
            "teams": [
                {"id": str(team.id), "role": "layer-viewer"},
                {"id": str(team.id), "role": "layer-editor"},
            ]
        },
    )
    assert r.status_code == 400, r.text
    assert await _rows(db_session, lid) == original, "existing grants must survive"


@pytest.mark.asyncio
async def test_family_absent_from_payload_is_untouched(
    client: AsyncClient,
    db_session: AsyncSession,
    roles: dict[str, UUID],
    fixture_create_user: Any,
    fixture_get_home_folder: dict[str, Any],
    make_user: Callable[..., Awaitable[User]],
    make_org: Callable[[], Awaitable[Organization]],
    make_team: Callable[..., Awaitable[Team]],
) -> None:
    """I6: apps/web sends only the family it edited (e.g. only `teams`); a
    field absent from the JSON body (None) must leave the other families'
    grants alone. An explicit `[]` still clears the family it names."""
    me = fixture_create_user
    org = await make_org()
    await db_session.execute(
        text(f'UPDATE {S}."user" SET organization_id = :o WHERE id = :u'),
        {"o": org.id, "u": me},
    )
    friend = await make_user(org.id)
    team = await make_team(friend)
    layer = Layer(
        id=uuid4(),
        user_id=me,
        folder_id=fixture_get_home_folder["id"],
        name="l",
        type="feature",
        feature_layer_type="standard",
        feature_layer_geometry_type="point",
    )
    db_session.add(layer)
    await db_session.commit()
    assert layer.id is not None
    lid: UUID = layer.id

    # seed a users grant
    r = await client.post(
        f"{settings.API_V2_STR}/share/layer/{lid}",
        json={"users": [{"id": str(friend.id), "role": "layer-editor"}]},
    )
    assert r.status_code == 200, r.text
    assert await _rows(db_session, lid) == [("user", str(friend.id), "layer-editor")]

    # POST with only `teams` present must keep the existing `users` grant
    r = await client.post(
        f"{settings.API_V2_STR}/share/layer/{lid}",
        params={"team_ids": [str(team.id)]},
        json={"teams": [{"id": str(team.id), "role": "layer-viewer"}]},
    )
    assert r.status_code == 200, r.text
    assert await _rows(db_session, lid) == [
        ("team", str(team.id), "layer-viewer"),
        ("user", str(friend.id), "layer-editor"),
    ], "users absent from the payload must be untouched"

    # POST with `users: []` clears users but keeps the untouched teams grant
    r = await client.post(
        f"{settings.API_V2_STR}/share/layer/{lid}",
        json={"users": []},
    )
    assert r.status_code == 200, r.text
    assert await _rows(db_session, lid) == [
        ("team", str(team.id), "layer-viewer"),
    ], "users: [] clears users; teams (absent) stays untouched"


@pytest.mark.asyncio
async def test_grantee_outside_the_callers_organization_is_refused(
    client: AsyncClient,
    db_session: AsyncSession,
    roles: dict[str, UUID],
    fixture_create_user: Any,
    fixture_get_home_folder: dict[str, Any],
    make_user: Callable[..., Awaitable[User]],
    make_org: Callable[[], Awaitable[Organization]],
    make_team: Callable[..., Awaitable[Team]],
) -> None:
    """S4/D14: 1:1 sharing is in-organization only.

    The DB `authorization()` gate checks the CALLER's role on the resource, and
    the only grantee family it looks at (`team_ids`) is an optional query
    parameter with no `users` counterpart — so without this check a layer owner
    in org A could hand a user in org B editor on the layer and its data.
    Every grantee family is checked, whether or not the query parameters are
    sent, and an in-organization grantee still goes through.
    """
    me = fixture_create_user
    mine = await make_org()
    theirs = await make_org()
    await db_session.execute(
        text(f'UPDATE {S}."user" SET organization_id = :o WHERE id = :u'),
        {"o": mine.id, "u": me},
    )
    colleague = await make_user(mine.id)
    outsider = await make_user(theirs.id)
    foreign_team = await make_team(outsider, org=theirs)
    layer = Layer(
        id=uuid4(),
        user_id=me,
        folder_id=fixture_get_home_folder["id"],
        name="l",
        type="feature",
        feature_layer_type="standard",
        feature_layer_geometry_type="point",
    )
    db_session.add(layer)
    await db_session.commit()
    assert layer.id is not None
    lid: UUID = layer.id

    # A colleague of the caller is fine, and seeds a grant the refusals below
    # must leave alone.
    ok = await client.post(
        f"{settings.API_V2_STR}/share/layer/{lid}",
        json={"users": [{"id": str(colleague.id), "role": "layer-editor"}]},
    )
    assert ok.status_code == 200, ok.text
    original = [("user", str(colleague.id), "layer-editor")]
    assert await _rows(db_session, lid) == original

    # A user in another organization — the reported case, no query params.
    refused = await client.post(
        f"{settings.API_V2_STR}/share/layer/{lid}",
        json={"users": [{"id": str(outsider.id), "role": "layer-editor"}]},
    )
    assert refused.status_code == 403, refused.text
    assert await _rows(db_session, lid) == original, "existing grants must survive"

    # A team in another organization, again with no `team_ids` query param to
    # trigger the DB-side check.
    refused = await client.post(
        f"{settings.API_V2_STR}/share/layer/{lid}",
        json={"teams": [{"id": str(foreign_team.id), "role": "layer-viewer"}]},
    )
    assert refused.status_code == 403, refused.text
    assert await _rows(db_session, lid) == original

    # Another organization as the grantee itself.
    refused = await client.post(
        f"{settings.API_V2_STR}/share/layer/{lid}",
        json={"organizations": [{"id": str(theirs.id), "role": "layer-viewer"}]},
    )
    assert refused.status_code == 403, refused.text
    assert await _rows(db_session, lid) == original

    # A grantee id that names nothing is refused the same way.
    refused = await client.post(
        f"{settings.API_V2_STR}/share/layer/{lid}",
        json={"users": [{"id": str(uuid4()), "role": "layer-editor"}]},
    )
    assert refused.status_code == 403, refused.text
    assert await _rows(db_session, lid) == original

    # One good and one foreign grantee in the same payload: nothing is written.
    refused = await client.post(
        f"{settings.API_V2_STR}/share/layer/{lid}",
        json={
            "users": [
                {"id": str(colleague.id), "role": "layer-viewer"},
                {"id": str(outsider.id), "role": "layer-editor"},
            ]
        },
    )
    assert refused.status_code == 403, refused.text
    assert await _rows(db_session, lid) == original, "the whole payload is rejected"

    # `users: []` still clears the family — an empty family carries no grantee
    # to check.
    cleared = await client.post(
        f"{settings.API_V2_STR}/share/layer/{lid}",
        json={"users": []},
    )
    assert cleared.status_code == 200, cleared.text
    assert await _rows(db_session, lid) == []


@pytest.mark.asyncio
async def test_project_share_with_a_foreign_user_is_refused(
    client: AsyncClient,
    db_session: AsyncSession,
    roles: dict[str, UUID],
    fixture_create_user: Any,
    fixture_get_home_folder: dict[str, Any],
    make_user: Callable[..., Awaitable[User]],
    make_org: Callable[[], Awaitable[Organization]],
) -> None:
    """The project side of the same endpoint pair shares the guard."""
    me = fixture_create_user
    mine = await make_org()
    theirs = await make_org()
    await db_session.execute(
        text(f'UPDATE {S}."user" SET organization_id = :o WHERE id = :u'),
        {"o": mine.id, "u": me},
    )
    colleague = await make_user(mine.id)
    outsider = await make_user(theirs.id)
    await db_session.commit()

    created = await client.post(
        f"{settings.API_V2_STR}/project",
        json={
            "name": "Shared project",
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
    assert created.status_code in (200, 201), created.text
    pid = created.json()["id"]

    refused = await client.post(
        f"{settings.API_V2_STR}/share/project/{pid}",
        json={"users": [{"id": str(outsider.id), "role": "project-editor"}]},
    )
    assert refused.status_code == 403, refused.text
    assert await _rows(db_session, pid) == []

    ok = await client.post(
        f"{settings.API_V2_STR}/share/project/{pid}",
        json={"users": [{"id": str(colleague.id), "role": "project-editor"}]},
    )
    assert ok.status_code == 200, ok.text
    assert await _rows(db_session, pid) == [
        ("user", str(colleague.id), "project-editor")
    ]
