from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import UUID4, BaseModel
from sqlalchemy import select

from core.crud.crud_organization_domain import (
    organization_domain as crud_organization_domain,
)
from core.crud.crud_project import project as crud_project
from core.db.models.organization_domain import CertStatus
from core.db.models.project import ProjectPublic
from core.db.session import AsyncSession
from core.deps.auth import auth_z
from core.endpoints.deps import get_db, get_user_id
from core.schemas.project import ProjectPublicRead

router = APIRouter()


@router.get(
    "/{project_id}/public",
    summary="Get public project",
    response_model=ProjectPublicRead | None,
    response_model_exclude_none=True,
)
async def get_public_project(
    project_id: str,
    async_session: AsyncSession = Depends(get_db),
) -> ProjectPublicRead | None:
    """
    Get shared project
    """
    result = await crud_project.get_public_project(
        async_session=async_session, project_id=project_id
    )

    return result


@router.post(
    "/{project_id}/publish",
    summary="Publish a project",
    dependencies=[Depends(auth_z)],
)
async def publish_project(
    project_id: str,
    user_id: UUID4 = Depends(get_user_id),
    async_session: AsyncSession = Depends(get_db),
) -> ProjectPublicRead:
    """
    Publish a project
    """
    result = ProjectPublicRead.model_validate(
        await crud_project.publish_project(
            async_session=async_session, project_id=project_id
        )
    )

    return result


@router.delete(
    "/{project_id}/unpublish",
    summary="Unpublish a project",
    dependencies=[Depends(auth_z)],
)
async def unpublish_project(
    project_id: str,
    async_session: AsyncSession = Depends(get_db),
) -> None:
    """
    Unpublish a project
    """
    await crud_project.unpublish_project(
        async_session=async_session, project_id=project_id
    )


##############################################
### Custom domain assignment endpoints
##############################################


class AssignCustomDomainPayload(BaseModel):
    """Body of POST /project/{project_id}/public/custom-domain."""

    domain_id: UUID4


@router.post(
    "/{project_id}/public/custom-domain",
    summary="Assign a custom domain to a published project",
    dependencies=[Depends(auth_z)],
)
async def assign_custom_domain(
    project_id: str,
    payload: AssignCustomDomainPayload,
    async_session: AsyncSession = Depends(get_db),
    user_id: UUID4 = Depends(get_user_id),
) -> ProjectPublicRead:
    """Bind an active custom domain to a published project."""
    result = await async_session.execute(
        select(ProjectPublic).where(ProjectPublic.project_id == UUID(project_id))
    )
    project_public = result.scalar_one_or_none()
    if project_public is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="project is not published",
        )

    domain = await crud_organization_domain.get(async_session, id=payload.domain_id)
    if not domain:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="domain not found",
        )
    if domain.cert_status != CertStatus.ACTIVE:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="domain must be active before assignment",
        )

    project_public.custom_domain_id = payload.domain_id
    await async_session.commit()
    await async_session.refresh(project_public)
    return ProjectPublicRead.model_validate(project_public)


@router.delete(
    "/{project_id}/public/custom-domain",
    summary="Unassign the custom domain from a published project",
    status_code=status.HTTP_204_NO_CONTENT,
    dependencies=[Depends(auth_z)],
)
async def unassign_custom_domain(
    project_id: str,
    async_session: AsyncSession = Depends(get_db),
    user_id: UUID4 = Depends(get_user_id),
) -> None:
    """Clear the custom-domain assignment from a published project."""
    result = await async_session.execute(
        select(ProjectPublic).where(ProjectPublic.project_id == UUID(project_id))
    )
    project_public = result.scalar_one_or_none()
    if project_public is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="project is not published",
        )

    project_public.custom_domain_id = None
    await async_session.commit()


class TrackingTogglePayload(BaseModel):
    """Body of PUT /project/{project_id}/public/tracking.

    Both fields are optional and updated independently — clients can send
    either or both. Lets the Share dialog flip each Switch with a single
    PUT to the same endpoint.
    """

    enabled: bool | None = None
    require_consent: bool | None = None


@router.put(
    "/{project_id}/public/tracking",
    summary="Update analytics tracking settings for a published project",
    dependencies=[Depends(auth_z)],
)
async def set_tracking_settings(
    project_id: str,
    payload: TrackingTogglePayload,
    async_session: AsyncSession = Depends(get_db),
    user_id: UUID4 = Depends(get_user_id),
) -> ProjectPublicRead:
    """Per-project opt-in toggles for analytics behaviour.

    ``enabled`` controls whether the tracker fires at all; ``require_consent``
    controls whether the tracker waits for the visitor's consent banner.
    Both flags are independent project preferences; what's actually injected
    at view time also depends on the org having an analytics configuration.
    """
    if payload.enabled is None and payload.require_consent is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="at least one of 'enabled' or 'require_consent' is required",
        )

    result = await async_session.execute(
        select(ProjectPublic).where(ProjectPublic.project_id == UUID(project_id))
    )
    project_public = result.scalar_one_or_none()
    if project_public is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="project is not published",
        )

    if payload.enabled is not None:
        project_public.tracking_enabled = payload.enabled
    if payload.require_consent is not None:
        project_public.tracking_require_consent = payload.require_consent
    await async_session.commit()
    await async_session.refresh(project_public)
    return ProjectPublicRead.model_validate(project_public)
