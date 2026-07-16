"""CRUD operations for the OrganizationAnalytics model.

An organization can hold any number of analytics instances. All helpers are
org-scoped so an instance can never be read or mutated through another
organization's endpoint path.
"""

from typing import Any
from uuid import UUID

from sqlalchemy import func, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from core.db.models.organization_analytics import OrganizationAnalytics
from core.db.models.project import Project, ProjectPublic
from core.db.models.user import User

from .base import CRUDBase


class CRUDOrganizationAnalytics(CRUDBase[OrganizationAnalytics, Any, Any]):
    """CRUD operations for the OrganizationAnalytics model."""

    async def list_by_organization(
        self, async_session: AsyncSession, *, organization_id: UUID
    ) -> list[tuple[OrganizationAnalytics, int]]:
        """Return each instance with its usage count (number of published
        dashboards currently referencing it)."""
        statement = (
            select(self.model, func.count(ProjectPublic.id))
            .outerjoin(ProjectPublic, ProjectPublic.analytics_id == self.model.id)
            .where(self.model.organization_id == organization_id)
            .group_by(self.model.id)
            .order_by(self.model.created_at)
        )
        result = await async_session.execute(statement)
        return [(row, count) for row, count in result.all()]

    async def get_for_organization(
        self,
        async_session: AsyncSession,
        *,
        organization_id: UUID,
        analytics_id: UUID,
    ) -> OrganizationAnalytics | None:
        statement = select(self.model).where(
            self.model.id == analytics_id,
            self.model.organization_id == organization_id,
        )
        result = await async_session.execute(statement)
        return result.scalars().first()

    async def create_instance(
        self,
        async_session: AsyncSession,
        *,
        organization_id: UUID,
        name: str,
        provider: str,
        config: dict,
    ) -> OrganizationAnalytics:
        row = OrganizationAnalytics(
            organization_id=organization_id,
            name=name,
            provider=provider,
            config=config,
        )
        async_session.add(row)
        await async_session.commit()
        await async_session.refresh(row)
        return row

    async def update_instance(
        self,
        async_session: AsyncSession,
        *,
        organization_id: UUID,
        analytics_id: UUID,
        name: str,
        provider: str,
        config: dict,
    ) -> OrganizationAnalytics | None:
        row = await self.get_for_organization(
            async_session,
            organization_id=organization_id,
            analytics_id=analytics_id,
        )
        if row is None:
            return None
        row.name = name
        row.provider = provider
        row.config = config
        await async_session.commit()
        await async_session.refresh(row)
        return row

    async def delete_instance(
        self,
        async_session: AsyncSession,
        *,
        organization_id: UUID,
        analytics_id: UUID,
    ) -> bool:
        row = await self.get_for_organization(
            async_session,
            organization_id=organization_id,
            analytics_id=analytics_id,
        )
        if row is None:
            return False
        await async_session.delete(row)
        await async_session.commit()
        return True

    async def list_org_dashboards(
        self, async_session: AsyncSession, *, organization_id: UUID
    ) -> list[tuple[UUID, str, UUID | None]]:
        """(project_id, name, analytics_id) for every published dashboard
        owned by users of this organization, ordered by name."""
        statement = (
            select(Project.id, Project.name, ProjectPublic.analytics_id)
            .join(ProjectPublic, ProjectPublic.project_id == Project.id)
            .join(User, User.id == Project.user_id)
            .where(User.organization_id == organization_id)
            .order_by(Project.name)
        )
        result = await async_session.execute(statement)
        return [(pid, name, aid) for pid, name, aid in result.all()]

    async def set_instance_dashboards(
        self,
        async_session: AsyncSession,
        *,
        organization_id: UUID,
        analytics_id: UUID,
        project_ids: list[UUID],
    ) -> None:
        """Reconcile which dashboards report to this instance.

        Listed dashboards are assigned to it (including reassignment away
        from another instance). Dashboards currently on this instance but
        not listed are cleared. Dashboards on other instances and unlisted
        are untouched, and consent settings are never modified. Raises
        ``ValueError`` naming the offending ids when a listed id is not a
        published dashboard of this organization; nothing is applied then.
        """
        valid_rows = await self.list_org_dashboards(
            async_session, organization_id=organization_id
        )
        valid_ids = {pid for pid, _, _ in valid_rows}
        requested = set(project_ids)
        invalid = requested - valid_ids
        if invalid:
            raise ValueError(", ".join(sorted(str(pid) for pid in invalid)))

        currently_assigned = {pid for pid, _, aid in valid_rows if aid == analytics_id}
        to_assign = requested - currently_assigned
        to_clear = currently_assigned - requested

        if to_assign:
            await async_session.execute(
                update(ProjectPublic)
                .where(ProjectPublic.project_id.in_(to_assign))
                .values(analytics_id=analytics_id)
            )
        if to_clear:
            await async_session.execute(
                update(ProjectPublic)
                .where(ProjectPublic.project_id.in_(to_clear))
                .values(analytics_id=None)
            )
        await async_session.commit()


organization_analytics = CRUDOrganizationAnalytics(OrganizationAnalytics)
