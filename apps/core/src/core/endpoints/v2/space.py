from typing import Any
from uuid import UUID

from fastapi import APIRouter, Body, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from core.crud.crud_space import space as crud_space
from core.deps.auth import auth_z
from core.endpoints.deps import get_db, get_user_id
from core.schemas.space import SpaceRead, SpaceUpdate, SpaceUsage

router = APIRouter()


@router.get(
    "",
    summary="List the spaces the current user belongs to",
    response_model=list[SpaceRead],
    dependencies=[Depends(auth_z)],
)
async def get_my_spaces(
    *,
    db: AsyncSession = Depends(get_db),
    user_id: UUID = Depends(get_user_id),
) -> Any:
    """The user's personal space, their team spaces and their organisation
    space — each lazily created on first listing."""
    return await crud_space.my_spaces(db, user_id)


@router.get(
    "/{space_id}/usage",
    summary="Storage and content usage of a space",
    response_model=SpaceUsage,
    dependencies=[Depends(auth_z)],
)
async def get_space_usage(
    *,
    db: AsyncSession = Depends(get_db),
    user_id: UUID = Depends(get_user_id),
    space_id: UUID,
) -> Any:
    """Live-row usage (D13): total layer bytes, live layer count and live
    project count. Members only (``space_rank >= 1``); a non-member gets the
    same 404 as a nonexistent space."""
    return await crud_space.usage(db, space_id=space_id, user_id=user_id)


@router.patch(
    "/{space_id}",
    summary="Change a space's default role for its plain members",
    response_model=SpaceRead,
    dependencies=[Depends(auth_z)],
)
async def update_space_default_role(
    *,
    db: AsyncSession = Depends(get_db),
    user_id: UUID = Depends(get_user_id),
    space_id: UUID,
    body: SpaceUpdate = Body(...),
) -> Any:
    """Only the space's owner (team-owner, or organization-owner/-admin)
    may change its default role; a personal space has none to set."""
    updated = await crud_space.set_default_role(
        db, space_id=space_id, user_id=user_id, role=body.default_role
    )
    return await crud_space.to_read(db, space=updated, user_id=user_id)
