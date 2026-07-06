from typing import TYPE_CHECKING, Any, List, Optional, Type
from uuid import UUID

from fastapi import HTTPException, status
from pydantic import UUID4
from sqlalchemy import Row, and_, null, select, union
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import contains_eager, selectinload
from sqlalchemy.sql import Select
from sqlmodel import SQLModel

from core.db.models import (
    Layer,
    LayerOrganizationLink,
    LayerTeamLink,
    Organization,
    Project,
    ProjectOrganizationLink,
    ProjectTeamLink,
    ResourceGrant,
    Role,
    Team,
    User,
)
from core.db.models.folder import Folder

if TYPE_CHECKING:
    from core.crud.crud_layer import CRUDLayer
    from core.crud.crud_project import CRUDProject


async def read_content_by_id(
    async_session: AsyncSession,
    id: UUID4,
    model: Type[SQLModel],
    crud_content: "CRUDLayer",
    extra_fields: List[Any] = [],
) -> SQLModel:
    """Read a content by its ID."""
    content = await crud_content.get(async_session, id=id, extra_fields=extra_fields)

    if content is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=f"{model.__name__} not found"
        )

    return content


async def update_content_by_id(
    async_session: AsyncSession,
    id: UUID4,
    model: Type[SQLModel],
    crud_content: "CRUDProject",
    content_in: SQLModel,
) -> SQLModel | None:
    """Update a content by its ID."""
    db_obj = await crud_content.get(async_session, id=id)
    if db_obj is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=f"{model.__name__} not found"
        )
    content = await crud_content.update(async_session, db_obj=db_obj, obj_in=content_in)
    return content


def create_query_shared_content(
    model: type[Layer] | type[Project],
    team_link_model: type[LayerTeamLink] | type[ProjectTeamLink],
    organization_link_model: type[LayerOrganizationLink]
    | type[ProjectOrganizationLink],
    team_model: type[Team],
    organization_model: type[Organization],
    role_model: type[Role],
    filters: list[Any],
    team_id: UUID | None = None,
    organization_id: UUID | None = None,
) -> Select[Any]:
    """
    Creates a dynamic query for a given model (Layer or Project) and its associated team, organization, and owner user.

    :param model: The main model (Layer or Project)
    :param team_link_model: The model linking the main model with teams (LayerTeamLink or ProjectTeamLink)
    :param organization_link_model: The model linking the main model with organizations (LayerOrganizationLink or ProjectOrganizationLink)
    :param team_model: The Team model
    :param organization_model: The Organization model
    :param role_model: The Role model
    :param filters: Additional filters to apply
    :param team_id: ID of the team (optional)
    :param organization_id: ID of the organization (optional)
    :return: A SQLAlchemy query object
    """

    # Determine the link field based on the model
    link_field = f"{model.__tablename__}_id"

    # Get team or layer model
    if team_id:
        read_column = [
            team_model.name.label("team_name"),
            team_model.id,
            team_model.avatar.label("team_avatar"),
        ]
    elif organization_id:
        read_column = [
            organization_model.name.label("team_name"),
            organization_model.id,
            organization_model.avatar.label("team_avatar"),
        ]
    else:
        read_column = []

    # Basic query to join the User who owns the Layer or Project
    base_query = select(
        model,
        role_model.id.label("valid_role_id"),
        User.id.label("valid_user_id"),
        User.firstname.label("user_firstname"),
        User.lastname.label("user_lastname"),
        User.avatar.label("user_avatar"),
        *read_column,
    ).join(
        User,
        model.user_id == User.id,  # Join on owner_id field with User model
    )

    if team_id:
        query = (
            base_query.join(
                team_link_model, getattr(team_link_model, link_field) == model.id
            )  # Dynamically replace `layer_id` or `project_id`
            .join(role_model, team_link_model.role_id == role_model.id)
            .join(team_model, team_link_model.team_id == team_model.id)
            .where(
                and_(
                    team_link_model.team_id == team_id,
                    *filters,
                )
            )
            .options(
                contains_eager(getattr(model, "team_links"))
            )  # Adjust field as needed for relationships
        )
    elif organization_id:
        query = (
            base_query.join(
                organization_link_model,
                getattr(organization_link_model, link_field) == model.id,
            )  # Dynamically replace `layer_id` or `project_id`
            .join(role_model, organization_link_model.role_id == role_model.id)
            .join(
                organization_model,
                organization_link_model.organization_id == organization_model.id,
            )
            .where(
                and_(
                    organization_link_model.organization_id == organization_id,
                    *filters,
                )
            )
            .options(
                contains_eager(getattr(model, "organization_links"))
            )  # Adjust field as needed for relationships
        )
    else:
        # No team/org context (e.g. folder-grant path or plain "My Content").
        # Use null() for valid_role_id to preserve the column order that
        # build_shared_with_object relies on (positional indices), without
        # joining role_model — which would cause an implicit cross join and
        # multiply rows by the number of roles in the table.
        # selectinload handles relationship loading without row multiplication.
        query = (
            select(
                model,
                null().label("valid_role_id"),
                User.id.label("valid_user_id"),
                User.firstname.label("user_firstname"),
                User.lastname.label("user_lastname"),
                User.avatar.label("user_avatar"),
            )
            .join(User, model.user_id == User.id)
            .where(and_(*filters))
            .options(
                selectinload(getattr(model, "team_links")).selectinload(
                    team_link_model.team
                ),
                selectinload(getattr(model, "organization_links")).selectinload(
                    organization_link_model.organization
                ),
            )
        )
    return query


# TODO: Make a pydantic schema for shared_with and owned_by
def build_shared_with_object(
    items: list[Row[Any]],
    role_mapping: dict[UUID, str],
    team_key: str = "team_links",
    org_key: str = "organization_links",
    model_name: str = "layer",
    team_id: UUID | None = None,
    organization_id: UUID | None = None,
) -> list[dict[str, Any]]:
    """
    Builds the shared_with object for both Layer and Project models.

    :param items: The list of Layer or Project items
    :param role_mapping: The mapping of role IDs to role names
    :param team_key: The attribute name for team links (default is "team_links")
    :param org_key: The attribute name for organization links (default is "organization_links")
    :param model_name: The name of the model ("layer" or "project")
    :param team_id: Optional ID for team-specific sharing
    :param organization_id: Optional ID for organization-specific sharing
    :return: A list of dictionaries containing the model and the shared_with data
    """

    def get_owned_by(item: Row[Any]) -> dict[str, Any]:
        """Helper function to build the 'owned_by' dictionary."""
        return {
            "id": item[2],
            "firstname": item[3],
            "lastname": item[4],
            "avatar": item[5],
        }

    def process_links(
        item: Layer | Project, link_key: str, link_type: str
    ) -> list[dict[str, Any]]:
        """Helper function to process either team or organization links."""
        shared_with: list[dict[str, Any]] = []
        links = getattr(item, link_key, None)
        if not links:
            return shared_with
        for link in links:
            shared_with.append(
                {
                    "role": role_mapping[link.role_id],  # Role based on role_mapping
                    "id": getattr(link, link_type).id,
                    "name": getattr(link, link_type).name,
                    "avatar": getattr(link, link_type).avatar,
                }
            )
        return shared_with

    result_arr = []

    # Determine shared_with key
    shared_with_key = (
        "teams" if team_id else "organizations" if organization_id else None
    )

    for item in items:
        if team_id or organization_id:
            # Case where shared_with is for a specific team or organization
            shared_with = {
                shared_with_key: [
                    {
                        "role": role_mapping[item[1]],  # Role name
                        "id": item[3],  # Team or Organization ID
                        "name": item[2],  # Team or Organization name
                        "avatar": item[4],  # Team or Organization avatar
                    }
                ]
            }
        else:
            # Case where shared_with includes both teams and organizations
            shared_with = {
                "teams": process_links(item[0], team_key, "team"),
                "organizations": process_links(item[0], org_key, "organization"),
            }

        # Add owned_by information
        owned_by = get_owned_by(item)
        result_arr.append(
            {**item[0].dict(), "shared_with": shared_with, "owned_by": owned_by}
        )

    return result_arr


def create_query_accessible_folders(
    user_id: UUID,
    team_ids: list[UUID],
    organization_id: Optional[UUID],
) -> Any:
    """Return a UNION query of folder rows the user owns or has been granted access to.

    Columns: id, user_id, name, created_at, updated_at
    """
    owned = select(
        Folder.id,
        Folder.user_id,
        Folder.name,
    ).where(Folder.user_id == user_id)

    branches: list[Any] = [owned]

    if team_ids:
        shared_via_team = (
            select(
                Folder.id,
                Folder.user_id,
                Folder.name,
            )
            .join(
                ResourceGrant,
                and_(
                    ResourceGrant.resource_type == "folder",
                    ResourceGrant.resource_id == Folder.id,
                    ResourceGrant.grantee_type == "team",
                    ResourceGrant.grantee_id.in_(team_ids),
                ),
            )
            .where(Folder.user_id != user_id)
        )
        branches.append(shared_via_team)

    if organization_id:
        shared_via_org = (
            select(
                Folder.id,
                Folder.user_id,
                Folder.name,
            )
            .join(
                ResourceGrant,
                and_(
                    ResourceGrant.resource_type == "folder",
                    ResourceGrant.resource_id == Folder.id,
                    ResourceGrant.grantee_type == "organization",
                    ResourceGrant.grantee_id == organization_id,
                ),
            )
            .where(Folder.user_id != user_id)
        )
        branches.append(shared_via_org)

    return union(*branches)
