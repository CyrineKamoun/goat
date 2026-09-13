"""A public dataset (`layer.public_read`) is readable by every signed-in
user in any organisation as viewer — a floor. It is not a catalog dataset:
the owner stays owner, a granted editor stays editor and keeps writing,
and anonymous callers still get nothing."""

from collections.abc import Awaitable, Callable
from uuid import UUID

import pytest
from core.db.models.folder import Folder
from core.db.models.layer import Layer
from core.db.models.user import User
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from tests.authz.test_effective_role_rules import S, can, grant, role


async def _make_public(db: AsyncSession, layer_id: UUID) -> None:
    await db.execute(
        text(f"UPDATE {S}.layer SET public_read = TRUE WHERE id = :l"),
        {"l": layer_id},
    )


async def _write_allowed(db: AsyncSession, layer_id: UUID, user_id: UUID) -> bool:
    return bool(
        (
            await db.execute(
                text(f"SELECT {S}.layer_write_allowed(:l, :u)"),
                {"l": layer_id, "u": user_id},
            )
        ).scalar_one()
    )


@pytest.mark.asyncio
async def test_public_dataset_is_viewer_for_every_signed_in_user_and_nobody_anonymous(
    db_session: AsyncSession,
    roles: dict[str, UUID],
    make_user: Callable[..., Awaitable[User]],
    make_org: Callable[..., Awaitable[object]],
    make_folder: Callable[..., Awaitable[Folder]],
    make_layer: Callable[..., Awaitable[Layer]],
) -> None:
    org_a, org_b = await make_org(), await make_org()
    owner = await make_user(org_a.id)  # type: ignore[attr-defined]
    outsider = await make_user(org_b.id)  # type: ignore[attr-defined]
    layer = await make_layer(owner, await make_folder(owner))

    assert await role(db_session, "layer", layer.id, outsider.id) is None
    await _make_public(db_session, layer.id)

    assert await role(db_session, "layer", layer.id, outsider.id) == "viewer"
    assert await can(db_session, "layer", layer.id, outsider.id, "read")
    assert not await can(db_session, "layer", layer.id, outsider.id, "write")
    assert not await can(db_session, "layer", layer.id, outsider.id, "share")
    assert not await can(db_session, "layer", layer.id, outsider.id, "delete")
    assert await role(db_session, "layer", layer.id, None) is None
    assert await role(db_session, "layer", layer.id, owner.id) == "owner"
    assert await _write_allowed(db_session, layer.id, owner.id)
    assert not await _write_allowed(db_session, layer.id, outsider.id)


@pytest.mark.asyncio
async def test_public_is_a_floor_a_granted_editor_keeps_editing(
    db_session: AsyncSession,
    roles: dict[str, UUID],
    make_user: Callable[..., Awaitable[User]],
    make_folder: Callable[..., Awaitable[Folder]],
    make_layer: Callable[..., Awaitable[Layer]],
) -> None:
    owner, editor = await make_user(), await make_user()
    layer = await make_layer(owner, await make_folder(owner))
    await grant(
        db_session,
        roles,
        rtype="layer",
        rid=layer.id,
        gtype="user",
        gid=editor.id,
        role_name="layer-editor",
        by=owner.id,
    )
    await _make_public(db_session, layer.id)

    assert await role(db_session, "layer", layer.id, editor.id) == "editor"
    assert await _write_allowed(db_session, layer.id, editor.id)
