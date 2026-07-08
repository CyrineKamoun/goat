"""User-facing endpoints for the organization's analytics instances.

An organization can register any number of instances (e.g. its own Matomo
plus one per client); dashboards pick one in the Share dialog. Authz is
delegated to ``auth_z`` (same gate as the other organization endpoints —
UI gates by org-admin role).
"""

from fastapi import APIRouter, Body, Depends, HTTPException, Path, status
from pydantic import UUID4

from core.crud.crud_organization_analytics import (
    organization_analytics as crud,
)
from core.db.session import AsyncSession
from core.deps.auth import auth_z
from core.endpoints.deps import get_db, get_user_id
from core.schemas.organization_analytics import (
    OrganizationAnalyticsCreate,
    OrganizationAnalyticsRead,
)

router = APIRouter()


@router.get(
    "/",
    summary="List the organization's analytics instances",
    response_model=list[OrganizationAnalyticsRead],
    dependencies=[Depends(auth_z)],
)
async def list_analytics(
    *,
    organization_id: UUID4 = Path(...),
    async_session: AsyncSession = Depends(get_db),
    user_id: UUID4 = Depends(get_user_id),
) -> list[OrganizationAnalyticsRead]:
    """Each instance carries ``usage_count`` — how many published dashboards
    currently report to it — so the UI can hint at reuse before assigning."""
    rows = await crud.list_by_organization(
        async_session, organization_id=organization_id
    )
    return [
        OrganizationAnalyticsRead.model_validate(row).model_copy(
            update={"usage_count": count}
        )
        for row, count in rows
    ]


@router.post(
    "/",
    summary="Create an analytics instance",
    response_model=OrganizationAnalyticsRead,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(auth_z)],
)
async def create_analytics(
    *,
    organization_id: UUID4 = Path(...),
    payload: OrganizationAnalyticsCreate = Body(...),
    async_session: AsyncSession = Depends(get_db),
    user_id: UUID4 = Depends(get_user_id),
) -> OrganizationAnalyticsRead:
    row = await crud.create_instance(
        async_session,
        organization_id=organization_id,
        name=payload.name,
        provider=payload.provider.value,
        # The discriminated union validates fields; persist the plain dict
        # form (HttpUrl serializes to string via mode="json").
        config=payload.config.model_dump(mode="json", exclude={"provider"}),
    )
    return OrganizationAnalyticsRead.model_validate(row)


@router.put(
    "/{analytics_id}",
    summary="Update an analytics instance",
    response_model=OrganizationAnalyticsRead,
    dependencies=[Depends(auth_z)],
)
async def update_analytics(
    *,
    organization_id: UUID4 = Path(...),
    analytics_id: UUID4 = Path(...),
    payload: OrganizationAnalyticsCreate = Body(...),
    async_session: AsyncSession = Depends(get_db),
    user_id: UUID4 = Depends(get_user_id),
) -> OrganizationAnalyticsRead:
    """Full replace of the instance's name, provider, and config."""
    row = await crud.update_instance(
        async_session,
        organization_id=organization_id,
        analytics_id=analytics_id,
        name=payload.name,
        provider=payload.provider.value,
        config=payload.config.model_dump(mode="json", exclude={"provider"}),
    )
    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="analytics instance not found",
        )
    return OrganizationAnalyticsRead.model_validate(row)


@router.delete(
    "/{analytics_id}",
    summary="Delete an analytics instance",
    status_code=status.HTTP_204_NO_CONTENT,
    dependencies=[Depends(auth_z)],
)
async def delete_analytics(
    *,
    organization_id: UUID4 = Path(...),
    analytics_id: UUID4 = Path(...),
    async_session: AsyncSession = Depends(get_db),
    user_id: UUID4 = Depends(get_user_id),
) -> None:
    """Dashboards referencing this instance keep working but stop tracking:
    the FK is ON DELETE SET NULL, so their ``analytics_id`` is cleared."""
    deleted = await crud.delete_instance(
        async_session,
        organization_id=organization_id,
        analytics_id=analytics_id,
    )
    if not deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="analytics instance not found",
        )
