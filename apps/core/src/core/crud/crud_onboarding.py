from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from core.core.config import settings
from core.crud.crud_content import content as crud_content
from core.schemas.onboarding import OnboardingFacts


class CRUDOnboarding:
    async def facts(self, db: AsyncSession, user_id: UUID) -> OnboardingFacts:
        """Existence checks over every space the caller belongs to. A
        member of a busy team is not asked to create a first project."""
        # Read at call time, not import time: `settings.SCHEMA` is swapped to
        # the test schema by the test suite after this module is first
        # imported, same as every other raw-SQL CRUD in this package.
        schema = settings.SCHEMA
        space_ids = await crud_content.my_space_ids(db, user_id)
        row = (
            await db.execute(
                text(
                    f"""
                    SELECT
                      EXISTS (SELECT 1 FROM {schema}.project p WHERE p.deleted_at IS NULL AND p.space_id = ANY(:s)
                                AND NOT p.is_template_source) AS has_project,
                      EXISTS (SELECT 1 FROM {schema}.layer l WHERE l.deleted_at IS NULL AND l.space_id = ANY(:s)
                                AND l.catalog_external_uid IS NULL) AS has_uploaded_layer,
                      EXISTS (SELECT 1 FROM {schema}.layer l WHERE l.deleted_at IS NULL AND l.space_id = ANY(:s)
                                AND l.catalog_external_uid IS NOT NULL) AS has_catalog_layer,
                      EXISTS (SELECT 1 FROM {schema}.user_team ut WHERE ut.user_id = :u) AS has_team,
                      EXISTS (SELECT 1 FROM {schema}.workflow w JOIN {schema}.project p ON p.id = w.project_id
                                WHERE p.deleted_at IS NULL AND p.space_id = ANY(:s)
                                AND NOT p.is_template_source) AS has_workflow
                    """
                ),
                {"s": list(space_ids), "u": user_id},
            )
        ).one()
        return OnboardingFacts(**row._mapping)


onboarding = CRUDOnboarding()
