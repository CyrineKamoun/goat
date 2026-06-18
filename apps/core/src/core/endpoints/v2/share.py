from typing import Any
from uuid import UUID

from fastapi import APIRouter, Body, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from core.crud.crud_share import share as crud_share
from core.deps.auth import auth_z, user_token
from core.endpoints.deps import get_db
from core.schemas.share import ShareLayerSchema, ShareProjectSchema

router = APIRouter()


@router.post(
    "/layer/{layer_id}",
    summary="Share with organizations and/or teams",
    response_model=ShareLayerSchema,
    dependencies=[Depends(auth_z)],
)
async def share_orgs_teams_for_layer(
    layer_id: str,
    db: AsyncSession = Depends(get_db),
    user_token: dict = Depends(user_token),
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
    Share layer with organizations and teams
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

    result = await crud_share.share_layer(
        db=db, layer_id=layer_id, shared_with=shared_with
    )

    return result


@router.post(
    "/project/{project_id}",
    summary="Share with organizations and/or teams",
    dependencies=[Depends(auth_z)],
)
async def share_orgs_teams_for_project(
    project_id: str,
    db: AsyncSession = Depends(get_db),
    user_token: dict = Depends(user_token),
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
    Share project with organizations and teams
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

    result = await crud_share.share_project(
        db=db, project_id=project_id, shared_with=shared_with
    )

    return result
