from typing import List, Literal
from uuid import UUID

from fastapi import APIRouter, Depends, Path, Query
from sqlalchemy import delete as sql_delete
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from core.db.models.favorite import Favorite
from core.db.session import AsyncSession
from core.deps.auth import auth_z
from core.endpoints.deps import get_db, get_user_id

router = APIRouter()

# What can be favourited. Extending this list is the whole change for a new
# kind — the table stores the type verbatim.
FavoriteItemType = Literal[
    "catalog_item", "workflow_template", "project", "dataset", "template"
]


@router.put(
    "/{item_type}/{item_id}",
    summary="Mark an item as a favourite",
    status_code=204,
    dependencies=[Depends(auth_z)],
)
async def put_favorite(
    async_session: AsyncSession = Depends(get_db),
    user_id: UUID = Depends(get_user_id),
    item_type: FavoriteItemType = Path(...),
    item_id: str = Path(..., max_length=255),
) -> None:
    """Upsert — favouriting twice is a no-op, not an error."""
    await async_session.execute(
        pg_insert(Favorite)
        .values(user_id=user_id, item_type=item_type, item_id=item_id)
        .on_conflict_do_nothing()
    )
    await async_session.commit()


@router.delete(
    "/{item_type}/{item_id}",
    summary="Remove an item from favourites",
    status_code=204,
    dependencies=[Depends(auth_z)],
)
async def delete_favorite(
    async_session: AsyncSession = Depends(get_db),
    user_id: UUID = Depends(get_user_id),
    item_type: FavoriteItemType = Path(...),
    item_id: str = Path(..., max_length=255),
) -> None:
    """Idempotent — deleting a non-favourite is a no-op."""
    await async_session.execute(
        sql_delete(Favorite).where(
            Favorite.user_id == user_id,
            Favorite.item_type == item_type,
            Favorite.item_id == item_id,
        )
    )
    await async_session.commit()


@router.get(
    "",
    summary="The caller's favourite ids of one kind",
    response_model=List[str],
    dependencies=[Depends(auth_z)],
)
async def list_favorites(
    async_session: AsyncSession = Depends(get_db),
    user_id: UUID = Depends(get_user_id),
    item_type: FavoriteItemType = Query(...),
) -> List[str]:
    """A flat id list — small by nature, and 'show my favourites' feeds it
    straight into a search filter."""
    result = await async_session.execute(
        select(Favorite.item_id)
        .where(Favorite.user_id == user_id, Favorite.item_type == item_type)
        .order_by(Favorite.created_at.desc())
    )
    return list(result.scalars().all())
