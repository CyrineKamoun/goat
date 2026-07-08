"""CRUD operations for the OrganizationAnalytics model.

An organization can hold any number of analytics instances. All helpers are
org-scoped so an instance can never be read or mutated through another
organization's endpoint path.
"""

from typing import Any
from uuid import UUID

from sqlalchemy import func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from core.db.models.organization_analytics import OrganizationAnalytics
from core.db.models.project import ProjectPublic

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


organization_analytics = CRUDOrganizationAnalytics(OrganizationAnalytics)
