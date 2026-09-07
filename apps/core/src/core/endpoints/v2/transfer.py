from fastapi import APIRouter, Body, Depends
from pydantic import UUID4

from core.crud.crud_transfer import transfer as crud_transfer
from core.db.session import AsyncSession
from core.deps.auth import auth_z
from core.endpoints.deps import get_db, get_user_id
from core.schemas.transfer import (
    TransferPreview,
    TransferPreviewRequest,
    TransferRequest,
    TransferResult,
)

router = APIRouter()


@router.post(
    "/preview",
    summary="Preview a content transfer",
    response_model=TransferPreview,
    status_code=200,
    dependencies=[Depends(auth_z)],
)
async def preview_transfer(
    async_session: AsyncSession = Depends(get_db),
    user_id: UUID4 = Depends(get_user_id),
    body: TransferPreviewRequest = Body(...),
) -> TransferPreview:
    """Preview moving items from the caller's personal space into a team or
    the organisation space: which datasets a project would take along
    (owned vs. left behind), how many grants would be dropped and how many
    folders a folder move drags along. See `CRUDTransfer.preview`."""
    return await crud_transfer.preview(
        async_session,
        user_id=user_id,
        items=body.items,
        target_space_id=body.target_space_id,
    )


@router.post(
    "",
    summary="Transfer content to a team or the organisation",
    response_model=TransferResult,
    status_code=200,
    dependencies=[Depends(auth_z)],
)
async def transfer_content(
    async_session: AsyncSession = Depends(get_db),
    user_id: UUID4 = Depends(get_user_id),
    body: TransferRequest = Body(...),
) -> TransferResult:
    """Promote-only handoff of owned My Content into a team or the
    organisation space (spec §3.3, D3, D6, D14). See `CRUDTransfer.transfer`."""
    return await crud_transfer.transfer(
        async_session,
        user_id=user_id,
        items=body.items,
        target_space_id=body.target_space_id,
        dataset_ids=body.dataset_ids,
        leave_shortcut=body.leave_shortcut,
    )
