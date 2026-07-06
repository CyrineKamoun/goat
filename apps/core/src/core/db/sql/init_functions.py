import logging

from core.core.config import settings
from core.db.session import session_manager
from core.db.sql.create_functions import AsyncFunctionManager

logger = logging.getLogger(__name__)


async def init_functions() -> None:
    """
    Initialize PostgreSQL SQL functions using an async session from session_manager.
    """
    if settings.ASYNC_SQLALCHEMY_DATABASE_URI is None:
        raise ValueError("PostgreSQL database URI not configured.")

    async with session_manager.session() as session:
        try:
            manager = AsyncFunctionManager(
                session=session,
                path="functions",
                schema="basic",
                # Honor the configurable data schema: the `.sql` files use
                # `customer.` as the canonical placeholder, substituted to the
                # actual schema at install time (no-op when SCHEMA="customer").
                schema_mapping={"basic": "basic", "customer": settings.SCHEMA},
            )

            await manager.update_functions()
            await session.commit()

            logger.info("SQL functions updated successfully.")

        except Exception as e:
            logger.error("Error updating SQL functions — rolling back.")
            logger.exception(e)
            await session.rollback()
            raise
