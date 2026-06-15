"""Backfill record_jsonb for existing customer.layer rows.

Run once after applying the d2e3f4a5b6c7 migration to populate
record_jsonb on all layers that don't already have it.

Usage (from apps/core):
    uv run python -m core.scripts.backfill_record_jsonb
"""

from __future__ import annotations

import asyncio
import logging

from core.db.models.layer import Layer
from core.db.session import AsyncSession, engine
from core.services.catalog import layer_to_record_jsonb
from sqlalchemy import select

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

BATCH_SIZE = 200


async def backfill() -> None:
    async with AsyncSession(engine) as session:
        offset = 0
        total_updated = 0

        while True:
            res = await session.execute(
                select(Layer)
                .where(Layer.record_jsonb.is_(None))
                .order_by(Layer.id)
                .limit(BATCH_SIZE)
                .offset(offset)
            )
            layers = list(res.scalars().all())
            if not layers:
                break

            for layer in layers:
                layer.record_jsonb = layer_to_record_jsonb(layer)
                session.add(layer)

            await session.commit()
            total_updated += len(layers)
            log.info("Backfilled %d layers (total so far: %d)", len(layers), total_updated)

            if len(layers) < BATCH_SIZE:
                break
            offset += BATCH_SIZE

        log.info("Backfill complete. Total updated: %d", total_updated)


if __name__ == "__main__":
    asyncio.run(backfill())
