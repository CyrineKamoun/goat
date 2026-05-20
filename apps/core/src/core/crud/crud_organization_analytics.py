"""CRUD operations for the OrganizationAnalytics model.

One row per organization; the org-scoped helpers below handle the upsert
semantics that the PUT endpoint needs (idempotent overwrite).
"""

from typing import Any
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from core.db.models.organization_analytics import OrganizationAnalytics

from .base import CRUDBase


class CRUDOrganizationAnalytics(CRUDBase[OrganizationAnalytics, Any, Any]):
    """CRUD operations for the OrganizationAnalytics model."""

    async def get_by_organization(
        self, async_session: AsyncSession, *, organization_id: UUID
    ) -> OrganizationAnalytics | None:
        statement = select(self.model).where(
            self.model.organization_id == organization_id
        )
        result = await async_session.execute(statement)
        return result.scalars().first()

    async def upsert(
        self,
        async_session: AsyncSession,
        *,
        organization_id: UUID,
        provider: str,
        config: dict,
    ) -> OrganizationAnalytics:
        existing = await self.get_by_organization(
            async_session, organization_id=organization_id
        )
        if existing:
            existing.provider = provider
            existing.config = config
            await async_session.commit()
            await async_session.refresh(existing)
            return existing
        row = OrganizationAnalytics(
            organization_id=organization_id,
            provider=provider,
            config=config,
        )
        async_session.add(row)
        await async_session.commit()
        await async_session.refresh(row)
        return row

    async def delete_by_organization(
        self, async_session: AsyncSession, *, organization_id: UUID
    ) -> bool:
        existing = await self.get_by_organization(
            async_session, organization_id=organization_id
        )
        if not existing:
            return False
        await async_session.delete(existing)
        await async_session.commit()
        return True


organization_analytics = CRUDOrganizationAnalytics(OrganizationAnalytics)
