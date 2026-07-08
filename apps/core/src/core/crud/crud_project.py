import json
from typing import Any
from uuid import UUID

from fastapi_pagination import Page
from fastapi_pagination import Params as PaginationParams
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import and_, not_, or_

from core.core.content import (
    build_shared_with_object,
    create_query_shared_content,
    update_content_by_id,
)
from core.crud.base import CRUDBase
from core.crud.crud_layer_project import layer_project as crud_layer_project
from core.crud.crud_user_project import user_project as crud_user_project
from core.db.models import (
    Organization,
    Project,
    ProjectOrganizationLink,
    ProjectTeamLink,
    ResourceGrant,
    Role,
    Team,
)
from core.db.models._link_model import UserProjectLink
from core.db.models.project import ProjectPublic
from core.schemas.common import OrderEnum
from core.schemas.project import (
    InitialViewState,
    IProjectBaseUpdate,
    IProjectRead,
    ProjectPublicConfig,
    ProjectPublicProjectConfig,
    ProjectPublicRead,
)


class CRUDProject(CRUDBase[Project, Any, Any]):
    async def create(
        self,
        async_session: AsyncSession,
        project_in: Project,
        initial_view_state: InitialViewState,
    ) -> IProjectRead:
        """Create project"""

        # Create project
        project = await CRUDBase(Project).create(
            db=async_session,
            obj_in=project_in,
        )
        # Default initial view state
        initial_view_state = {
            "zoom": 5,
            "pitch": 0,
            "bearing": 0,
            "latitude": 51.01364693631891,
            "max_zoom": 20,
            "min_zoom": 0,
            "longitude": 9.576740589534126,
        }

        # Create link between user and project for initial view state
        await crud_user_project.create(
            async_session,
            obj_in=UserProjectLink(
                user_id=project.user_id,
                project_id=project.id,
                initial_view_state=initial_view_state,
            ).model_dump(),
        )
        # Doing unneeded type conversion to make sure the relations of project are not loaded
        return IProjectRead(**project.model_dump())

    async def get_projects(
        self,
        async_session: AsyncSession,
        user_id: UUID,
        page_params: PaginationParams,
        folder_id: UUID | None = None,
        search: str | None = None,
        order_by: str | None = None,
        order: OrderEnum | None = None,
        ids: list | None = None,
        team_id: UUID | None = None,
        organization_id: UUID | None = None,
        team_ids: list[UUID] | None = None,
        user_organization_id: UUID | None = None,
    ) -> Page[IProjectRead]:
        """Get projects for a user and folder"""

        # Build query and filters
        use_folder_grant_query = False
        if team_id or organization_id:
            grant_conditions = []
            if team_id:
                grant_conditions.append(
                    and_(ResourceGrant.grantee_type == "team", ResourceGrant.grantee_id == team_id)
                )
            if organization_id:
                grant_conditions.append(
                    and_(ResourceGrant.grantee_type == "organization", ResourceGrant.grantee_id == organization_id)
                )
            if folder_id:
                # Check whether this folder is accessible via a ResourceGrant for the
                # given team/org. If so, bypass the ProjectTeamLink join — projects in
                # folder-shared folders have no such link.
                grant_result = await async_session.execute(
                    select(ResourceGrant.id).where(
                        ResourceGrant.resource_type == "folder",
                        ResourceGrant.resource_id == folder_id,
                        or_(*grant_conditions),
                    ).limit(1)
                )
                use_folder_grant_query = grant_result.first() is not None
                filters = [Project.folder_id == folder_id]
            else:
                # At team/org root: exclude projects that live inside a folder
                # already shared with this team/org — those surface when navigating
                # into the folder, not at the root level.
                folder_granted_ids = (
                    select(ResourceGrant.resource_id).where(
                        ResourceGrant.resource_type == "folder",
                        or_(*grant_conditions),
                    )
                )
                # NULL-safe: folder_id IS NULL means no folder, so always include it.
                # Without this, NULL NOT IN (...) evaluates to UNKNOWN (= excluded).
                filters = [
                    or_(
                        Project.folder_id.is_(None),
                        not_(Project.folder_id.in_(folder_granted_ids)),
                    )
                ]
        elif folder_id:
            # Check if the folder is shared with the user via a grant.
            # If so, show all projects in the folder (not just the user's own).
            has_grant = False
            grant_conditions = []
            if team_ids:
                grant_conditions.append(
                    and_(ResourceGrant.grantee_type == "team", ResourceGrant.grantee_id.in_(team_ids))
                )
            if user_organization_id:
                grant_conditions.append(
                    and_(ResourceGrant.grantee_type == "organization", ResourceGrant.grantee_id == user_organization_id)
                )
            if grant_conditions:
                grant_result = await async_session.execute(
                    select(ResourceGrant.id).where(
                        ResourceGrant.resource_type == "folder",
                        ResourceGrant.resource_id == folder_id,
                        or_(*grant_conditions),
                    ).limit(1)
                )
                has_grant = grant_result.first() is not None

            filters = [Project.folder_id == folder_id]
            if not has_grant:
                filters.append(Project.user_id == user_id)
        else:
            filters = [Project.user_id == user_id]

        if ids:
            query = select(Project).where(Project.id.in_(ids))
        elif use_folder_grant_query:
            # Folder is shared via ResourceGrant — bypass team/org link join so we
            # see all projects in the folder regardless of ProjectTeamLink entries.
            query = create_query_shared_content(
                Project,
                ProjectTeamLink,
                ProjectOrganizationLink,
                Team,
                Organization,
                Role,
                filters,
                team_id=None,
                organization_id=None,
            )
        else:
            query = create_query_shared_content(
                Project,
                ProjectTeamLink,
                ProjectOrganizationLink,
                Team,
                Organization,
                Role,
                filters,
                team_id=team_id,
                organization_id=organization_id,
            )

        # Get roles
        roles = await CRUDBase(Role).get_all(
            async_session,
        )
        role_mapping = {role.id: role.name for role in roles}

        # Get projects
        projects = await self.get_multi(
            async_session,
            query=query,
            page_params=page_params,
            search_text={"name": search} if search else {},
            order_by=order_by,
            order=order,
        )
        effective_team_id = None if use_folder_grant_query else team_id
        effective_org_id = None if use_folder_grant_query else organization_id
        projects.items = build_shared_with_object(
            items=projects.items,
            role_mapping=role_mapping,
            team_key="team_links",
            org_key="organization_links",
            model_name="project",
            team_id=effective_team_id,
            organization_id=effective_org_id,
        )
        return projects

    async def update_base(
        self, async_session: AsyncSession, id: UUID, project: IProjectBaseUpdate
    ) -> IProjectRead:
        """Update project base"""

        # Update project
        updated_project = await update_content_by_id(
            async_session=async_session,
            id=id,
            model=Project,
            crud_content=self,
            content_in=project,
        )

        if updated_project is None:
            raise Exception("Project not found")

        return IProjectRead(**updated_project.model_dump())

    async def get_public_project(
        self, *, async_session: AsyncSession, project_id: UUID
    ) -> ProjectPublicRead | None:
        # Local imports to avoid module-level cycles between crud_project,
        # the analytics model (which imports the organization model), and
        # the project model.
        from core.db.models.organization_analytics import OrganizationAnalytics
        from core.schemas.project import PublicAnalytics

        result = await async_session.execute(
            select(ProjectPublic).where(ProjectPublic.project_id == project_id)
        )
        project_public = result.scalars().first()
        if not project_public:
            return None

        # Resolve the assigned analytics instance (if any) via the direct
        # FK; a dangling reference (instance deleted mid-request) simply
        # yields no analytics block.
        analytics: PublicAnalytics | None = None
        if project_public.analytics_id is not None:
            analytics_row = (
                (
                    await async_session.execute(
                        select(OrganizationAnalytics).where(
                            OrganizationAnalytics.id == project_public.analytics_id
                        )
                    )
                )
                .scalars()
                .first()
            )
            if analytics_row is not None:
                analytics = PublicAnalytics(
                    provider=analytics_row.provider,
                    config=analytics_row.config,
                )

        return ProjectPublicRead(
            **project_public.model_dump(),
            analytics=analytics,
        )

    async def publish_project(
        self, *, async_session: AsyncSession, project_id: UUID
    ) -> ProjectPublic:
        project = (
            (
                await async_session.execute(
                    select(Project).where(Project.id == project_id)
                )
            )
            .scalars()
            .first()
        )
        if not project:
            raise Exception("Project not found")
        project_public: ProjectPublic | None = (
            (
                await async_session.execute(
                    select(ProjectPublic).where(ProjectPublic.project_id == project_id)
                )
            )
            .scalars()
            .first()
        )
        user_project = (
            (
                await async_session.execute(
                    select(UserProjectLink).where(
                        and_(
                            UserProjectLink.project_id == project_id,
                            Project.user_id == UserProjectLink.user_id,
                        )
                    )
                )
            )
            .scalars()
            .first()
        )
        project_layers = await crud_layer_project.get_layers(
            async_session=async_session, project_id=project_id
        )

        # Import here to avoid circular imports
        from core.crud.crud_layer_project_group import layer_project_group
        from core.schemas.project import ILayerProjectGroupRead

        project_layer_groups_db = await layer_project_group.get_groups_by_project(
            async_session=async_session, project_id=project_id
        )

        # Convert to schema objects
        project_layer_groups = [
            ILayerProjectGroupRead(**group.model_dump())
            for group in project_layer_groups_db
        ]

        new_project_public_project_config = ProjectPublicProjectConfig(
            id=project.id,
            name=project.name,
            description=project.description,
            tags=project.tags,
            thumbnail_url=project.thumbnail_url,
            initial_view_state=user_project.initial_view_state,
            basemap=project.basemap,
            custom_basemaps=project.custom_basemaps,
            layer_order=project.layer_order,
            max_extent=project.max_extent,
            folder_id=project.folder_id,
            builder_config=project.builder_config,
        )
        new_project_public_config = ProjectPublicConfig(
            layers=project_layers,
            layer_groups=project_layer_groups,
            project=new_project_public_project_config,
        )
        new_config = json.loads(new_project_public_config.model_dump_json())

        # Update in place when a public row already exists so we preserve
        # custom_domain_id, password, subdomain, analytics_id across
        # re-publish. The previous delete+recreate flow silently dropped
        # the custom-domain assignment every time the user clicked "Update".
        if project_public:
            project_public.config = new_config
            await async_session.commit()
            await async_session.refresh(project_public)
            return project_public

        new_project_public = ProjectPublic(
            project_id=project_id,
            config=new_config,
        )
        async_session.add(new_project_public)
        await async_session.commit()
        return new_project_public

    async def unpublish_project(
        self, *, async_session: AsyncSession, project_id: str
    ) -> None:
        public_project = (
            (
                await async_session.execute(
                    select(ProjectPublic).where(ProjectPublic.project_id == project_id)
                )
            )
            .scalars()
            .first()
        )
        if public_project:
            await async_session.delete(public_project)
            await async_session.commit()
        else:
            raise Exception("Project not found")
        return None


project = CRUDProject(Project)
