"""Seed the ``bundle_type`` reference table.

The table is a projection of the code spec registry
(``goatlib.models.bundle.SPECS``): each row's ``name``/``description``/
``structure`` is generated from the spec. Idempotent — safe to re-run on every
deploy; rows are inserted when missing and refreshed to match the specs.
"""

import asyncio

from goatlib.models.bundle import SPECS
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.core.config import settings
from core.db.models import BundleType
from core.db.session import session_manager


async def seed_bundle_types(session: AsyncSession) -> None:
    """Insert missing bundle types and refresh their definitions from
    the goatlib spec registry."""
    existing = {
        str(row.type): row
        for row in (await session.execute(select(BundleType))).scalars()
    }
    for spec in SPECS.values():
        payload = {
            "type": spec.type.value,
            "name": spec.name,
            "description": spec.description,
            "structure": spec.to_structure(),
        }
        row = existing.get(spec.type.value)
        if row is None:
            session.add(BundleType(**payload))
        else:
            row.name = payload["name"]
            row.description = payload["description"]
            row.structure = payload["structure"]
    await session.commit()


async def main() -> None:
    session_manager.init(settings.ASYNC_SQLALCHEMY_DATABASE_URI)
    try:
        async with session_manager.session() as session:
            await seed_bundle_types(session)
    finally:
        await session_manager.close()


if __name__ == "__main__":
    asyncio.run(main())
