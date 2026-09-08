"""NUTS spatial-filter helper endpoints (GOAT extension, Task 10).

Backed by ``CatalogStore.NUTS`` (the ``nuts`` table, loaded from
``nuts.parquet`` by ``catalog.store.CatalogStore._build`` -- see that
module's docstring: a missing ``nuts.parquet`` on disk is never an error,
just an empty typed table, so these endpoints degrade to an empty list /
404s rather than a 500 when the file hasn't been synced yet).

A tiny reference table (a few dozen rows at most), so ``q`` is a plain
``ILIKE`` substring match over ``nuts_name``/``nuts_id`` -- no FTS index
needed (unlike the catalog table's ``fts_main_cat``, see
``catalog.services.search``).

Included under the same ``/stac`` prefix and dependency stack
(``optional_auth`` + ``check_not_modified``) as ``catalog.routers.stac``, so
NUTS is equally public-readable and ETag-cached by the app-level middleware
in ``catalog.app``. The catalog page's spatial filter calls this, so it has
to work for an anonymous visitor like the rest of the read paths.
"""

import json
from typing import Any

from fastapi import APIRouter, Depends, Query

from catalog.auth import optional_auth
from catalog.deps import check_not_modified, get_store
from catalog.errors import ApiError
from catalog.limits import MAX_NUTS_LIMIT, clamp_limit
from catalog.store import CatalogStore

# Order matters: optional_auth MUST run before check_not_modified -- see
# catalog.routers.stac's router declaration for why.
router = APIRouter(
    prefix="/stac",
    tags=["NUTS"],
    dependencies=[Depends(optional_auth), Depends(check_not_modified)],
)


@router.get("/nuts", summary="Search NUTS regions (GOAT extension)")
async def list_nuts(
    store: CatalogStore = Depends(get_store),
    q: str | None = Query(
        None, description="Substring match (case-insensitive) on nuts_name or nuts_id"
    ),
    level: int | None = Query(None, description="Exact NUTS level match"),
    limit: int = Query(
        20,
        description=(
            "Maximum number of regions to return. A larger value is served as "
            f"the maximum ({MAX_NUTS_LIMIT}) rather than rejected."
        ),
    ),
) -> list[dict[str, Any]]:
    # Clamped rather than `le=`-rejected, so this GOAT helper answers an
    # oversized `limit` the same way the /stac endpoints do (see
    # catalog.limits) instead of being the one endpoint that errors on it.
    limit = clamp_limit(limit, MAX_NUTS_LIMIT)
    filters: list[str] = []
    params: list[Any] = []

    if q:
        pattern = f"%{q}%"
        filters.append("(nuts_name ILIKE ? OR nuts_id ILIKE ?)")
        params.extend([pattern, pattern])
    if level is not None:
        filters.append("level = ?")
        params.append(level)

    where = f"WHERE {' AND '.join(filters)}" if filters else ""
    params.append(limit)

    rows = store.query_dicts(
        f"""
        SELECT
            nuts_id, nuts_name, level, country,
            ST_XMin(geometry) AS xmin, ST_YMin(geometry) AS ymin,
            ST_XMax(geometry) AS xmax, ST_YMax(geometry) AS ymax
        FROM {store.NUTS}
        {where}
        ORDER BY nuts_id
        LIMIT ?
        """,
        params,
    )
    return [
        {
            "nuts_id": row["nuts_id"],
            "nuts_name": row["nuts_name"],
            "level": row["level"],
            "country": row["country"],
            "bbox": [row["xmin"], row["ymin"], row["xmax"], row["ymax"]],
        }
        for row in rows
    ]


@router.get(
    "/nuts/{nuts_id}/geometry",
    summary="A NUTS region's geometry (GOAT extension, GeoJSON Feature)",
)
async def nuts_geometry(
    nuts_id: str, store: CatalogStore = Depends(get_store)
) -> dict[str, Any]:
    rows = store.query_dicts(
        f"""
        SELECT nuts_id, nuts_name, level, country, ST_AsGeoJSON(geometry) AS geojson
        FROM {store.NUTS}
        WHERE nuts_id = ?
        """,
        [nuts_id],
    )
    if not rows:
        raise ApiError(404, f"NUTS region not found: {nuts_id!r}")

    row = rows[0]
    return {
        "type": "Feature",
        "properties": {
            "nuts_id": row["nuts_id"],
            "nuts_name": row["nuts_name"],
            "level": row["level"],
            "country": row["country"],
        },
        "geometry": json.loads(row["geojson"]),
    }
