"""Backfill record_jsonb.properties.themes from the flat data_category column.

For every in_catalog customer.layer row that has a data_category value but no
themes entry in record_jsonb.properties, write an `ai_data_category` override
into datacatalog.record_overrides (priority 50) AND patch the layer's
record_jsonb so the OGC API serves it without waiting for a re-harvest.

Usage (from apps/core):
    uv run python -m core.scripts.backfill_themes_from_data_category
"""

from __future__ import annotations

import asyncio
import logging
from copy import deepcopy
from typing import Any, Dict

from core.db.models.layer import Layer
from core.db.session import AsyncSession, engine
from core.services.catalog import _deep_merge, upsert_record_override
from sqlalchemy import select

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

BATCH_SIZE = 200
AI_SOURCE = "ai_data_category"
AI_PRIORITY = 50


def _has_themes(record_jsonb: Dict[str, Any] | None) -> bool:
    if not isinstance(record_jsonb, dict):
        return False
    props = record_jsonb.get("properties")
    if not isinstance(props, dict):
        return False
    themes = props.get("themes")
    if not isinstance(themes, list) or not themes:
        return False
    first = themes[0]
    if not isinstance(first, dict):
        return False
    concepts = first.get("concepts")
    return isinstance(concepts, list) and len(concepts) > 0


def _theme_overlay(value: str) -> Dict[str, Any]:
    return {"properties": {"themes": [{"concepts": [{"id": value}]}]}}


async def backfill() -> None:
    async with AsyncSession(engine) as session:
        offset = 0
        total_updated = 0

        while True:
            res = await session.execute(
                select(Layer)
                .where(Layer.in_catalog.is_(True))
                .where(Layer.data_category.is_not(None))
                .order_by(Layer.id)
                .limit(BATCH_SIZE)
                .offset(offset)
            )
            layers = list(res.scalars().all())
            if not layers:
                break

            updated_in_batch = 0
            for layer in layers:
                if _has_themes(layer.record_jsonb):
                    continue

                category_value = (
                    layer.data_category.value
                    if hasattr(layer.data_category, "value")
                    else str(layer.data_category)
                )
                overlay = _theme_overlay(category_value)

                await upsert_record_override(
                    session=session,
                    layer_id=layer.id,
                    source=AI_SOURCE,
                    priority=AI_PRIORITY,
                    overrides_jsonb=overlay,
                )

                merged = _deep_merge(deepcopy(layer.record_jsonb or {}), overlay)
                layer.record_jsonb = merged
                session.add(layer)
                updated_in_batch += 1

            await session.commit()
            total_updated += updated_in_batch
            log.info(
                "Batch processed: %d/%d layers patched (running total: %d)",
                updated_in_batch,
                len(layers),
                total_updated,
            )

            if len(layers) < BATCH_SIZE:
                break
            offset += BATCH_SIZE

        log.info("Backfill complete. Total layers patched: %d", total_updated)


if __name__ == "__main__":
    asyncio.run(backfill())
