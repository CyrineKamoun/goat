from typing import Any, List
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.db.models._link_model import (
    LayerOrganizationLink,
    LayerTeamLink,
    ProjectOrganizationLink,
    ProjectTeamLink,
)
from core.db.models.role import Role
from core.schemas.share import (
    LayerShareRoleEnum,
    ProjectShareRoleEnum,
    ShareLayerSchema,
    ShareProjectSchema,
)

layer_share_role_values = [role.value for role in LayerShareRoleEnum]
project_share_role_values = [role.value for role in ProjectShareRoleEnum]


class CRUDShare:
    async def get_role_dict(self, *, db: AsyncSession, role_values: list[str]) -> dict:
        roles_query = select(Role).where(Role.name.in_(role_values))
        result = await db.execute(roles_query)
        roles = result.scalars().all()

        # Construct the dictionary
        role_dict = {role.name: role.id for role in roles}
        return role_dict

    async def get_layer_team_links(
        self, *, db: AsyncSession, layer_id: str, role_ids: List[UUID]
    ) -> Any:
        teams_query = select(LayerTeamLink).where(
            LayerTeamLink.layer_id == layer_id, LayerTeamLink.role_id.in_(role_ids)
        )
        result = await db.execute(teams_query)
        layer_team_links = result.scalars().all()
        return layer_team_links

    async def get_layer_organization_links(
        self, *, db: AsyncSession, layer_id: str, role_ids: List[UUID]
    ) -> Any:
        organization_query = select(LayerOrganizationLink).where(
            LayerOrganizationLink.layer_id == layer_id,
            LayerOrganizationLink.role_id.in_(role_ids),
        )
        result = await db.execute(organization_query)
        layer_organization_links = result.scalars().all()
        return layer_organization_links

    async def get_project_team_links(
        self, *, db: AsyncSession, project_id: str, role_ids: List[UUID]
    ) -> Any:
        teams_query = select(ProjectTeamLink).where(
            ProjectTeamLink.project_id == project_id,
            ProjectTeamLink.role_id.in_(role_ids),
        )
        result = await db.execute(teams_query)
        project_team_links = result.scalars().all()
        return project_team_links

    async def get_project_organization_links(
        self, *, db: AsyncSession, project_id: str, role_ids: List[UUID]
    ) -> Any:
        organization_query = select(ProjectOrganizationLink).where(
            ProjectOrganizationLink.project_id == project_id,
            ProjectOrganizationLink.role_id.in_(role_ids),
        )
        result = await db.execute(organization_query)
        project_organization_links = result.scalars().all()
        return project_organization_links

    async def share_layer(
        self,
        *,
        db: AsyncSession,
        layer_id: str,
        shared_with: ShareLayerSchema,
    ) -> ShareLayerSchema:
        roles = await self.get_role_dict(db=db, role_values=layer_share_role_values)
        # Get the existing links
        layer_team_links = await self.get_layer_team_links(
            db=db, layer_id=layer_id, role_ids=list(roles.values())
        )
        layer_organization_links = await self.get_layer_organization_links(
            db=db, layer_id=layer_id, role_ids=list(roles.values())
        )

        # Delete the existing links
        for layer_team_link in layer_team_links:
            await db.delete(layer_team_link)

        for layer_organization_link in layer_organization_links:
            await db.delete(layer_organization_link)

        # Create the new links
        for team in shared_with.teams or []:
            role_name = team.role
            if role_name not in layer_share_role_values:
                raise Exception("Invalid role")
            db.add(
                LayerTeamLink(
                    layer_id=layer_id, team_id=team.id, role_id=roles[role_name]
                )
            )

        for organization in shared_with.organizations or []:
            role_name = organization.role
            if role_name not in layer_share_role_values:
                raise Exception("Invalid role")
            db.add(
                LayerOrganizationLink(
                    layer_id=layer_id,
                    organization_id=organization.id,
                    role_id=roles[role_name],
                )
            )

        # Commit the transaction
        await db.commit()

        return shared_with

    async def share_project(
        self,
        *,
        db: AsyncSession,
        project_id: str,
        shared_with: ShareProjectSchema,
    ) -> ShareProjectSchema:
        roles = await self.get_role_dict(db=db, role_values=project_share_role_values)
        # Get the existing links
        project_team_links = await self.get_project_team_links(
            db=db, project_id=project_id, role_ids=list(roles.values())
        )
        project_organization_links = await self.get_project_organization_links(
            db=db, project_id=project_id, role_ids=list(roles.values())
        )

        # Delete the existing links
        for project_team_link in project_team_links:
            await db.delete(project_team_link)

        for project_organization_link in project_organization_links:
            await db.delete(project_organization_link)

        # Commit the transaction
        await db.commit()

        # Create the new links
        for team in shared_with.teams or []:
            role_name = team.role
            if role_name not in project_share_role_values:
                raise Exception("Invalid role")
            db.add(
                ProjectTeamLink(
                    project_id=project_id, team_id=team.id, role_id=roles[role_name]
                )
            )

        for organization in shared_with.organizations or []:
            role_name = organization.role
            if role_name not in project_share_role_values:
                raise Exception("Invalid role")
            db.add(
                ProjectOrganizationLink(
                    project_id=project_id,
                    organization_id=organization.id,
                    role_id=roles[role_name],
                )
            )

        # Commit the transaction
        await db.commit()

        return shared_with


share = CRUDShare()
