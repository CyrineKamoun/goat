from typing import Any
from uuid import UUID

from fastapi import APIRouter, Body, Depends, HTTPException, Path, Query
from pydantic import UUID4
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.crud.crud_share import share as crud_share
from core.db.models._link_model import BundleLayerLink
from core.deps.auth import auth_z
from core.endpoints.deps import get_db, get_user_id
from core.schemas.share import ShareLayerSchema, ShareProjectSchema

router = APIRouter()


@router.post(
    "/layer/{layer_id}",
    summary="Share with users, teams and/or organizations",
    response_model=ShareLayerSchema,
    dependencies=[Depends(auth_z)],
)
async def share_orgs_teams_for_layer(
    layer_id: UUID4,
    db: AsyncSession = Depends(get_db),
    user_id: UUID4 = Depends(get_user_id),
    organization_ids: list[UUID] | None = Query(
        None,
        title="Organization IDs",
        description="List of organization IDs to share the layer with",
    ),
    team_ids: list[UUID] | None = Query(
        None,
        title="Team IDs",
        description="List of team IDs to share the layer with",
    ),
    shared_with: ShareLayerSchema = Body(
        ...,
    ),
) -> Any:
    """
    Share layer with users, teams and organizations
    """
    # Layers that belong to a bundle are never shared individually —
    # they inherit the bundle's sharing. Reject the attempt and point the
    # caller at the bundle share endpoint.
    in_bundle = (
        await db.execute(
            select(BundleLayerLink.id)
            .where(BundleLayerLink.layer_id == layer_id)
            .limit(1)
        )
    ).scalar_one_or_none()
    if in_bundle is not None:
        raise HTTPException(
            status_code=409,
            detail=(
                "This layer belongs to a bundle and cannot be shared "
                "individually. Share the bundle instead."
            ),
        )

    # check if there is any team_ids or organization_ids in the request body and if they match the query parameters
    if shared_with.teams:
        payload_team_ids = {str(team.id) for team in shared_with.teams}
        if team_ids and set(map(str, team_ids)) != payload_team_ids:
            raise HTTPException(
                status_code=400,
                detail="team_ids in query parameters do not match teams in payload",
            )

    if shared_with.organizations:
        payload_org_ids = {str(org.id) for org in shared_with.organizations}
        if organization_ids and set(map(str, organization_ids)) != payload_org_ids:
            raise HTTPException(
                status_code=400,
                detail="organization_ids in query parameters do not match organizations in payload",
            )

    try:
        result = await crud_share.share_resource(
            db=db,
            resource_type="layer",
            resource_id=layer_id,
            shared_with=shared_with,
            granted_by=user_id,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    return result


@router.get(
    "/layer/{layer_id}",
    summary="Get who a layer is shared with",
    response_model=ShareLayerSchema,
    dependencies=[Depends(auth_z)],
)
async def get_layer_shares(
    db: AsyncSession = Depends(get_db),
    layer_id: UUID4 = Path(..., description="The ID of the layer to get shares for"),
) -> Any:
    """
    Get the users, teams and organizations a layer is shared with
    """
    return await crud_share.get_grants(
        db=db, resource_type="layer", resource_id=layer_id
    )


@router.post(
    "/project/{project_id}",
    summary="Share with users, teams and/or organizations",
    response_model=ShareProjectSchema,
    dependencies=[Depends(auth_z)],
)
async def share_orgs_teams_for_project(
    project_id: UUID4,
    db: AsyncSession = Depends(get_db),
    user_id: UUID4 = Depends(get_user_id),
    organization_ids: list[UUID] | None = Query(
        None,
        title="Organization IDs",
        description="List of organization IDs to share the project with",
    ),
    team_ids: list[UUID] | None = Query(
        None,
        title="Team IDs",
        description="List of team IDs to share the project with",
    ),
    shared_with: ShareProjectSchema = Body(
        ...,
    ),
) -> Any:
    """
    Share project with users, teams and organizations
    """
    # check if there is any team_ids or organization_ids in the request body and if they match the query parameters
    if shared_with.teams:
        payload_team_ids = {str(team.id) for team in shared_with.teams}
        if team_ids and set(map(str, team_ids)) != payload_team_ids:
            raise HTTPException(
                status_code=400,
                detail="team_ids in query parameters do not match teams in payload",
            )

    if shared_with.organizations:
        payload_org_ids = {str(org.id) for org in shared_with.organizations}
        if organization_ids and set(map(str, organization_ids)) != payload_org_ids:
            raise HTTPException(
                status_code=400,
                detail="organization_ids in query parameters do not match organizations in payload",
            )

    try:
        result = await crud_share.share_resource(
            db=db,
            resource_type="project",
            resource_id=project_id,
            shared_with=shared_with,
            granted_by=user_id,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    return result


@router.get(
    "/project/{project_id}",
    summary="Get who a project is shared with",
    response_model=ShareProjectSchema,
    dependencies=[Depends(auth_z)],
)
async def get_project_shares(
    db: AsyncSession = Depends(get_db),
    project_id: UUID4 = Path(
        ..., description="The ID of the project to get shares for"
    ),
) -> Any:
    """
    Get the users, teams and organizations a project is shared with
    """
    return await crud_share.get_grants(
        db=db, resource_type="project", resource_id=project_id
    )
