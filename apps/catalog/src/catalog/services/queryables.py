"""Queryables document (OGC API - Features Part 3 §7 / STAC filter extension).

Advertises, as a JSON Schema, which properties a CQL2 filter may reference
against this catalog. The property list and each property's schema come from
``catalog.services.registry``, which derives both from the loaded table -- the
same registry ``catalog.services.cql`` validates filters against, so what is
advertised here and what is accepted there cannot drift.
"""

from typing import Any

from catalog.services.registry import QueryableRegistry

_JSON_SCHEMA_DIALECT = "https://json-schema.org/draft/2019-09/schema"


def queryables_schema(
    base_url: str,
    registry: QueryableRegistry,
    collection: str | None = None,
) -> dict[str, Any]:
    """Build the queryables JSON Schema document served at ``.../queryables``.

    ``base_url`` is the STAC API root (e.g. ``https://host/stac``);
    ``collection`` narrows the ``$id``/title to a single collection's
    queryables endpoint when given, matching
    ``GET /stac/collections/{cid}/queryables``.
    """
    queryables_id = (
        f"{base_url}/collections/{collection}/queryables"
        if collection is not None
        else f"{base_url}/queryables"
    )
    title = f"Queryables for collection {collection}" if collection else "Queryables"

    return {
        "$schema": _JSON_SCHEMA_DIALECT,
        "$id": queryables_id,
        "type": "object",
        "title": title,
        "properties": registry.schema_properties(),
        "additionalProperties": False,
    }
