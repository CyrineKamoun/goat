"""User-facing endpoints for the organization-level analytics configuration.

One row per organization; PUT is idempotent upsert. Authz is delegated to
``auth_z`` (same gate as the other organization endpoints — UI gates by
org-admin role).
"""

from typing import Optional

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
    summary="Get the organization's analytics configuration",
    response_model=Optional[OrganizationAnalyticsRead],
    dependencies=[Depends(auth_z)],
)
async def get_analytics(
    *,
    organization_id: UUID4 = Path(...),
    async_session: AsyncSession = Depends(get_db),
    user_id: UUID4 = Depends(get_user_id),
) -> Optional[OrganizationAnalyticsRead]:
    """Returns null when no analytics has been configured yet.

    Returning 200+null rather than 404 keeps "feature not enabled yet" out
    of error reporters and the browser's network panel — the org itself
    exists, the config singleton just hasn't been set.
    """
    row = await crud.get_by_organization(
        async_session, organization_id=organization_id
    )
    if row is None:
        return None
    return OrganizationAnalyticsRead.model_validate(row)


@router.put(
    "/",
    summary="Create or update the organization's analytics configuration",
    response_model=OrganizationAnalyticsRead,
    dependencies=[Depends(auth_z)],
)
async def upsert_analytics(
    *,
    organization_id: UUID4 = Path(...),
    payload: OrganizationAnalyticsCreate = Body(...),
    async_session: AsyncSession = Depends(get_db),
    user_id: UUID4 = Depends(get_user_id),
) -> OrganizationAnalyticsRead:
    """Idempotent: overwrites whatever is currently stored for this org."""
    row = await crud.upsert(
        async_session,
        organization_id=organization_id,
        provider=payload.provider.value,
        # The discriminated union validates fields; persist the plain dict
        # form (HttpUrl serializes to string via mode="json").
        config=payload.config.model_dump(mode="json", exclude={"provider"}),
    )
    return OrganizationAnalyticsRead.model_validate(row)


@router.delete(
    "/",
    summary="Remove the organization's analytics configuration",
    status_code=status.HTTP_204_NO_CONTENT,
    dependencies=[Depends(auth_z)],
)
async def delete_analytics(
    *,
    organization_id: UUID4 = Path(...),
    async_session: AsyncSession = Depends(get_db),
    user_id: UUID4 = Depends(get_user_id),
) -> None:
    """Clearing the config also implicitly stops tracking on every project
    in the org that had ``tracking_enabled=true`` — the public-project
    endpoint joins to this row, so absence means analytics simply isn't
    served to dashboards. The per-project flag stays as-is in the DB so
    re-adding a config later resumes tracking without re-toggling each
    project."""
    deleted = await crud.delete_by_organization(
        async_session, organization_id=organization_id
    )
    if not deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="no analytics configuration to remove",
        )
