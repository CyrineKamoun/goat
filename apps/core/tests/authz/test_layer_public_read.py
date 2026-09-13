"""`layer.public_read` is the owner's decision: the layer PUT accepts it
from a caller whose effective role on the layer is owner, and refuses it
from an editor. Everything else on the PUT is unaffected."""

from typing import Any, Awaitable, Callable
from uuid import UUID, uuid4

import pytest
from core.core.config import settings
from core.db.models.folder import Folder
from core.db.models.layer import Layer
from core.db.models.space import Space, SpaceKind
from core.db.models.team import Team
from core.db.models.user import User
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from tests.authz.test_content_feed import _unverified_bearer

S = settings.SCHEMA


async def _flag(db_session: AsyncSession, layer_id: UUID) -> bool:
    return bool(
        (
            await db_session.execute(
                text(f"SELECT public_read FROM {S}.layer WHERE id = :id"),
                {"id": layer_id},
            )
        ).scalar_one()
    )


@pytest.mark.asyncio
async def test_owner_sets_and_clears_public_read_without_any_realm_role(
    client: AsyncClient,
    db_session: AsyncSession,
    make_user: Callable[..., Awaitable[User]],
    make_folder: Callable[..., Awaitable[Folder]],
    make_layer: Callable[..., Awaitable[Any]],
) -> None:
    owner = await make_user()
    folder = await make_folder(owner)
    layer = await make_layer(owner, folder)
    await db_session.commit()
    # `_unverified_bearer` carries no `realm_access` at all: a plain owner.
    headers = {"Authorization": f"Bearer {_unverified_bearer(owner.id)}"}

    resp = await client.put(
        f"{settings.API_V2_STR}/layer/{layer.id}",
        json={"public_read": True},
        headers=headers,
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["public_read"] is True
    assert await _flag(db_session, layer.id) is True

    resp = await client.put(
        f"{settings.API_V2_STR}/layer/{layer.id}",
        json={"public_read": False},
        headers=headers,
    )
    assert resp.status_code == 200, resp.text
    assert await _flag(db_session, layer.id) is False


@pytest.mark.asyncio
async def test_editor_cannot_set_public_read_but_still_edits(
    client: AsyncClient,
    db_session: AsyncSession,
    make_user: Callable[..., Awaitable[User]],
    make_team: Callable[..., Awaitable[Team]],
    make_space: Callable[..., Awaitable[Space]],
) -> None:
    """A team member is an editor of the team's datasets by the space
    default. Editing is not owning: the PUT takes a rename but refuses the
    Public switch, which belongs to the team's admins."""
    lead = await make_user()
    member = await make_user()
    team = await make_team(lead, member)
    space = await make_space(SpaceKind.team, team=team)
    folder = Folder(
        id=uuid4(), user_id=lead.id, space_id=space.id, name=f"team-{uuid4().hex[:6]}"
    )
    db_session.add(folder)
    await db_session.flush()
    layer = Layer(
        id=uuid4(),
        user_id=lead.id,
        folder_id=folder.id,
        space_id=space.id,
        name=f"team-layer-{uuid4().hex[:6]}",
        type="feature",
        feature_layer_type="standard",
        feature_layer_geometry_type="polygon",
    )
    db_session.add(layer)
    await db_session.commit()
    headers = {"Authorization": f"Bearer {_unverified_bearer(member.id)}"}

    resp = await client.put(
        f"{settings.API_V2_STR}/layer/{layer.id}",
        json={"public_read": True},
        headers=headers,
    )
    assert resp.status_code == 403, resp.text
    assert await _flag(db_session, layer.id) is False

    resp = await client.put(
        f"{settings.API_V2_STR}/layer/{layer.id}",
        json={"name": "renamed"},
        headers=headers,
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["name"] == "renamed"
