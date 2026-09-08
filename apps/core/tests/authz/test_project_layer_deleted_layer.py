"""A project-layer link whose dataset is soft-deleted resolves to nothing.

`crud_layer_project.get_by_ids` excludes a layer with `deleted_at` set while
the `layer_project` row survives the soft delete, so the single-link read has
an empty result to handle: it 404s instead of indexing into it.
"""

from __future__ import annotations

import base64
import json
from collections.abc import Awaitable, Callable
from uuid import UUID

import pytest
from core.core.config import settings
from core.db.models._link_model import LayerProjectLink
from core.db.models.folder import Folder
from core.db.models.layer import Layer
from core.db.models.organization import Organization
from core.db.models.project import Project
from core.db.models.user import User
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

S = settings.SCHEMA


def _bearer(user_id: UUID) -> dict[str, str]:
    """Authorization header making the test client act as `user_id`.

    `get_user_id` reads `sub` with `jwt.get_unverified_claims`, so an unsigned
    JWT-shaped token is enough under `AUTH=False`.
    """

    def _segment(payload: dict[str, str]) -> str:
        return (
            base64.urlsafe_b64encode(json.dumps(payload).encode()).rstrip(b"=").decode()
        )

    token = (
        f"{_segment({'alg': 'none', 'typ': 'JWT'})}."
        f"{_segment({'sub': str(user_id)})}.sig"
    )
    return {"Authorization": f"Bearer {token}"}


@pytest.mark.asyncio
async def test_link_to_a_trashed_layer_is_not_found(
    client: AsyncClient,
    db_session: AsyncSession,
    authz_sql: None,
    make_user: Callable[..., Awaitable[User]],
    make_org: Callable[[], Awaitable[Organization]],
    make_folder: Callable[..., Awaitable[Folder]],
    make_layer: Callable[..., Awaitable[Layer]],
    make_project: Callable[..., Awaitable[Project]],
) -> None:
    org = await make_org()
    owner = await make_user(org.id)
    folder = await make_folder(owner, "Mine")
    project = await make_project(owner, folder)
    layer = await make_layer(owner, folder)
    link = LayerProjectLink(
        project_id=project.id,
        layer_id=layer.id,
        name=layer.name,
        properties={},
        shareable=True,
    )
    db_session.add(link)
    await db_session.flush()
    link_id = link.id
    await db_session.commit()

    me = _bearer(owner.id)
    url = f"{settings.API_V2_STR}/project/{project.id}/layer/{link_id}"

    # The link reads fine while the layer is live.
    live = await client.get(url, headers=me)
    assert live.status_code == 200, live.text

    # Soft-deleting the layer leaves the link row in place.
    await db_session.execute(
        text(f"UPDATE {S}.layer SET deleted_at = now() WHERE id = :i"),
        {"i": layer.id},
    )
    await db_session.commit()
    still_linked = (
        await db_session.execute(
            text(f"SELECT 1 FROM {S}.layer_project WHERE id = :i"), {"i": link_id}
        )
    ).scalar_one_or_none()
    assert still_linked == 1, "the soft delete must not have removed the link"

    gone = await client.get(url, headers=me)
    assert gone.status_code == 404, gone.text
