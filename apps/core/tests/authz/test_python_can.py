"""Python face of the SQL authz rule: ``authz.can`` / ``authz.effective_role`` /
``authz.require`` (customer.effective_role / customer.can) exercised end to end
against the installed SQL functions."""

from collections.abc import Awaitable, Callable
from uuid import UUID, uuid4

import pytest
from core.core import authz
from core.db.models.folder import Folder
from core.db.models.layer import Layer
from core.db.models.user import User
from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession


@pytest.mark.asyncio
async def test_can_and_require(
    db_session: AsyncSession,
    roles: dict[str, UUID],
    make_user: Callable[..., Awaitable[User]],
    make_folder: Callable[[User], Awaitable[Folder]],
    make_layer: Callable[[User, Folder], Awaitable[Layer]],
) -> None:
    owner, stranger = await make_user(), await make_user()
    layer = await make_layer(owner, await make_folder(owner))
    await db_session.commit()
    assert layer.id is not None
    assert owner.id is not None
    assert stranger.id is not None

    assert await authz.can(db_session, "layer", layer.id, owner.id, "delete")
    assert not await authz.can(db_session, "layer", layer.id, stranger.id, "read")
    assert (
        await authz.effective_role(db_session, "layer", layer.id, owner.id) == "owner"
    )

    # owner clears every action — require() does not raise
    await authz.require(db_session, "layer", layer.id, owner.id, "delete")

    # existing resource, stranger has no access at all -> 403
    with pytest.raises(HTTPException) as exc_403:
        await authz.require(db_session, "layer", layer.id, stranger.id, "read")
    assert exc_403.value.status_code == 403

    # unknown resource id -> 404 (existence checked only once `can` is False,
    # so a stranger with no access still gets told the resource is missing,
    # not merely forbidden, when it in fact does not exist)
    with pytest.raises(HTTPException) as exc_404:
        await authz.require(db_session, "layer", uuid4(), stranger.id, "read")
    assert exc_404.value.status_code == 404
