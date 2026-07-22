"""OGC API - Records endpoint for catalog layers.

Exposes catalog layers as an OGC API Records collection so that external
systems (CSW clients, AI agents, discovery portals) **and the GOAT frontend**
can consume structured geospatial metadata.

Records are grouped by dataset (layer_group): one record per dataset (a
recordGeoJSON Feature). Member layers of a grouped dataset are exposed as
``rel="item"`` links; ``enclosure``/``via`` links carry the downloads/source.

Spec reference: https://ogcapi.ogc.org/records/

This is the READ/serving side of the catalog. It reads PostgreSQL metadata only
(``customer.layer``, ``customer.layer_group``, ``basic.nuts``) via geoapi's
asyncpg pool (``layer_service._pool``) — no DuckLake. The metadata WRITE path
(opencatalog_record_contribution → record_jsonb) lives in the ``core`` service.

The query logic is shared with the MCP server via ``geoapi.services.catalog_search``.

Extension parameters (beyond OGC core):
- ``bbox_boost``: spatial ranking without exclusion
- ``themes``/``license``/``language``/``year``/``publisher``/``type``/
  ``geographical_code``: faceted filters
- ``/items/aggregates``: facet value counts (OGC API Records Part 2: Facets shape)
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, Request
from goatlib.models import ConformanceDeclaration

from geoapi.models.records import (
    RecordCollection,
    RecordCollectionsResponse,
    RecordsLandingPage,
)
from geoapi.services.catalog_search import (
    _COLLECTION_DESCRIPTION,
    _COLLECTION_ID,
    _COLLECTION_TITLE,
    _build_filters,
    get_nuts_geometry,
    get_record,
    search_nuts,
    search_records,
)
from geoapi.services.layer_service import layer_service

router = APIRouter(tags=["Catalog"])


def _pool() -> Any:
    """Return the asyncpg pool or raise 503 if metadata DB is unavailable."""
    pool = layer_service._pool
    if pool is None:
        raise HTTPException(status_code=503, detail="Metadata database unavailable")
    return pool


def _records_base(request: Request) -> str:
    """Absolute base URL for this records API (root-mounted, no /api/v2)."""
    return f"{str(request.base_url).rstrip('/')}/catalog/records"


@router.get(
    "",
    summary="OGC API Records — landing page",
    response_model=RecordsLandingPage,
    status_code=200,
    include_in_schema=True,
)
async def ogc_records_landing(request: Request) -> Dict[str, Any]:
    """OGC API Records landing page with conformance classes and collection links."""
    base = _records_base(request)
    app_base = str(request.base_url).rstrip("/")  # OpenAPI lives at the app root
    return {
        "title": "GOAT Catalog — OGC API Records",
        "description": (
            "Discovery endpoint for geospatial catalog datasets. "
            "Machine-readable by AI agents and CSW/OGC clients."
        ),
        "links": [
            {
                "rel": "self",
                "type": "application/json",
                "title": "This document",
                "href": base,
            },
            {
                "rel": "service-desc",
                "type": "application/vnd.oai.openapi+json;version=3.0",
                "title": "OpenAPI definition",
                "href": f"{app_base}/api/openapi.json",
            },
            {
                "rel": "service-doc",
                "type": "text/html",
                "title": "API documentation",
                "href": f"{app_base}/api/docs",
            },
            {
                "rel": "conformance",
                "type": "application/json",
                "title": "Conformance",
                "href": f"{base}/conformance",
            },
            {
                "rel": "data",
                "type": "application/json",
                "title": "Collections",
                "href": f"{base}/collections",
            },
        ],
        "conformsTo": [
            "http://www.opengis.net/spec/ogcapi-common-1/1.0/conf/core",
            "http://www.opengis.net/spec/ogcapi-common-1/1.0/conf/landingPage",
            "http://www.opengis.net/spec/ogcapi-records-1/1.0/conf/record-core",
            "http://www.opengis.net/spec/ogcapi-records-1/1.0/conf/record-collection",
            "http://www.opengis.net/spec/ogcapi-records-1/1.0/conf/record-core-query-parameters",
            "http://www.opengis.net/spec/ogcapi-records-1/1.0/conf/sorting",
            "http://www.opengis.net/spec/ogcapi-records-1/1.0/conf/json",
        ],
    }


@router.get(
    "/conformance",
    summary="OGC API Records — conformance classes",
    response_model=ConformanceDeclaration,
    status_code=200,
)
async def ogc_records_conformance() -> Dict[str, Any]:
    return {
        "conformsTo": [
            "http://www.opengis.net/spec/ogcapi-common-1/1.0/conf/core",
            "http://www.opengis.net/spec/ogcapi-records-1/1.0/conf/core",
            "http://www.opengis.net/spec/ogcapi-records-1/1.0/conf/json",
        ]
    }


@router.get(
    "/collections",
    summary="OGC API Records — available collections",
    response_model=RecordCollectionsResponse,
    status_code=200,
)
async def ogc_records_collections(request: Request) -> Dict[str, Any]:
    records_base = _records_base(request)
    base = f"{records_base}/collections/{_COLLECTION_ID}"
    return {
        "links": [
            {
                "rel": "self",
                "type": "application/json",
                "title": "Collections",
                "href": f"{records_base}/collections",
            },
        ],
        "collections": [
            {
                "id": _COLLECTION_ID,
                "title": _COLLECTION_TITLE,
                "description": _COLLECTION_DESCRIPTION,
                "links": [
                    {
                        "rel": "items",
                        "type": "application/geo+json",
                        "title": "Records",
                        "href": f"{base}/items",
                    },
                ],
            }
        ],
    }


@router.get(
    f"/collections/{_COLLECTION_ID}",
    summary="OGC API Records — collection metadata",
    response_model=RecordCollection,
    status_code=200,
)
async def ogc_records_collection(request: Request) -> Dict[str, Any]:
    base = f"{_records_base(request)}/collections/{_COLLECTION_ID}"
    return {
        "id": _COLLECTION_ID,
        "title": _COLLECTION_TITLE,
        "description": _COLLECTION_DESCRIPTION,
        "links": [
            {
                "rel": "self",
                "type": "application/json",
                "title": "This collection",
                "href": base,
            },
            {
                "rel": "items",
                "type": "application/geo+json",
                "title": "Records",
                "href": f"{base}/items",
            },
        ],
    }


@router.get(
    f"/collections/{_COLLECTION_ID}/items",
    summary="OGC API Records — catalog dataset records",
    response_model=Dict[str, Any],
    status_code=200,
)
async def ogc_records_items(
    request: Request,
    limit: int = Query(10, ge=1, le=100, description="Number of records to return"),
    offset: int = Query(0, ge=0, description="Number of records to skip"),
    bbox: Optional[str] = Query(
        None,
        description="Bounding box hard filter as 'west,south,east,north' (WGS 84). Excludes non-matching.",
        example="9.5,47.2,13.8,55.1",
    ),
    bbox_boost: Optional[str] = Query(
        None,
        description="Bounding box for spatial ranking as 'west,south,east,north' (WGS 84). "
        "Records intersecting this box are ranked first; all records are still returned.",
        example="11.3,48.0,11.8,48.3",
    ),
    q: Optional[str] = Query(
        None,
        description="Free-text search across title, description, and keywords",
    ),
    themes: Optional[str] = Query(
        None,
        description="Filter by data category (comma-separated). "
        "Values: transportation, landuse, environment, places, people, imagery, boundary, basemap, other",
        example="transportation,landuse",
    ),
    language: Optional[str] = Query(
        None,
        description="Filter by ISO 639-1 language code (e.g. 'de', 'en')",
    ),
    year: Optional[int] = Query(
        None, description="Filter by data reference year", example=2023
    ),
    source_format: Optional[str] = Query(
        None,
        description="Filter by metadata source format: iso19139, dcat, synthetic, layer_model",
    ),
    license: Optional[str] = Query(
        None,
        description="Filter by license (comma-separated). E.g. CC_BY,CC_BY_SA",
    ),
    publisher: Optional[str] = Query(
        None,
        description="Filter by publisher/distributor name (comma-separated)",
    ),
    type: Optional[str] = Query(
        None,
        description="Filter by layer type (comma-separated): feature, raster, table",
    ),
    geographical_code: Optional[str] = Query(
        None,
        description="Filter by geographical code (comma-separated ISO 3166-1 alpha-2). E.g. DE,AT",
    ),
    datetime: Optional[str] = Query(
        None,
        description="OGC temporal filter. Single date or interval: "
        "'2023-01-01', '2023-01-01/2024-12-31', '2023-01-01/..', '../2024-12-31'",
        alias="datetime",
        example="2023-01-01/2024-12-31",
    ),
    sortby: Optional[str] = Query(
        None,
        description="Sort results by property. Prefix with - for descending, + for ascending (default). "
        "Supported: title, updated, created, type. E.g. '-updated,+title'",
        example="-updated",
    ),
) -> Dict[str, Any]:
    """Return catalog datasets as an OGC API Records GeoJSON FeatureCollection.

    Records are **grouped by dataset (layer_group)** — one record per dataset;
    a grouped dataset's member layers are ``rel="item"`` links.
    """
    base_url = f"{_records_base(request)}/collections/{_COLLECTION_ID}/items"
    return await search_records(
        _pool(),
        base_url=base_url,
        q=q,
        bbox=bbox,
        bbox_boost=bbox_boost,
        themes=themes,
        language=language,
        year=year,
        source_format=source_format,
        license=license,
        publisher=publisher,
        type=type,
        geographical_code=geographical_code,
        datetime=datetime,
        sortby=sortby,
        limit=limit,
        offset=offset,
    )


@router.get(
    f"/collections/{_COLLECTION_ID}/items/aggregates",
    summary="OGC API Records — filter value counts for faceted search",
    response_model=Dict[str, Any],
    status_code=200,
)
async def ogc_records_aggregates(
    bbox: Optional[str] = Query(
        None, description="Bounding box filter as 'west,south,east,north'"
    ),
    q: Optional[str] = Query(None, description="Free-text search"),
    themes: Optional[str] = Query(
        None, description="Filter by data category (comma-separated)"
    ),
    language: Optional[str] = Query(None, description="Filter by language code"),
    year: Optional[int] = Query(None, description="Filter by year"),
    source_format: Optional[str] = Query(None, description="Filter by source format"),
    license: Optional[str] = Query(
        None, description="Filter by license (comma-separated)"
    ),
    publisher: Optional[str] = Query(
        None, description="Filter by publisher (comma-separated)"
    ),
    type: Optional[str] = Query(
        None, description="Filter by layer type (comma-separated)"
    ),
    geographical_code: Optional[str] = Query(
        None, description="Filter by geographical code (comma-separated)"
    ),
    datetime: Optional[str] = Query(
        None, description="OGC temporal filter: '2023-01-01/2024-12-31'"
    ),
) -> Dict[str, Any]:
    """Return aggregated counts of filterable metadata values (faceted search)."""
    where_sql, params = _build_filters(
        q=q,
        bbox=bbox,
        themes=themes,
        language=language,
        year=year,
        source_format=source_format,
        license_=license,
        publisher=publisher,
        type_=type,
        geographical_code=geographical_code,
        datetime_=datetime,
    )
    cs = "customer"

    agg_queries = {
        "type": f"SELECT cl.type AS value, COUNT(*) AS count FROM {cs}.layer cl WHERE {where_sql} AND cl.type IS NOT NULL GROUP BY cl.type ORDER BY count DESC",
        "data_category": (
            f"SELECT cl.record_jsonb->'properties'->'themes'->0->'concepts'->0->>'id' AS value,"
            f" COUNT(*) AS count"
            f" FROM {cs}.layer cl WHERE {where_sql}"
            f" AND cl.record_jsonb->'properties'->'themes'->0->'concepts'->0->>'id' IS NOT NULL"
            f" GROUP BY value ORDER BY count DESC"
        ),
        "geographical_code": (
            f"SELECT cl.record_jsonb->'properties'->>'goat:geographical_code' AS value,"
            f" COUNT(*) AS count"
            f" FROM {cs}.layer cl WHERE {where_sql}"
            f" AND cl.record_jsonb->'properties'->>'goat:geographical_code' IS NOT NULL"
            f" GROUP BY value ORDER BY count DESC"
        ),
        "language_code": f"SELECT cl.record_jsonb->'properties'->'language'->>'code' AS value, COUNT(*) AS count FROM {cs}.layer cl WHERE {where_sql} AND cl.record_jsonb->'properties'->'language'->>'code' IS NOT NULL GROUP BY value ORDER BY count DESC",
        "distributor_name": f"SELECT cl.record_jsonb->'properties'->'contacts'->0->>'name' AS value, COUNT(*) AS count FROM {cs}.layer cl WHERE {where_sql} AND cl.record_jsonb->'properties'->'contacts'->0->>'name' IS NOT NULL GROUP BY value ORDER BY count DESC",
        "license": f"SELECT cl.record_jsonb->'properties'->>'license' AS value, COUNT(*) AS count FROM {cs}.layer cl WHERE {where_sql} AND cl.record_jsonb->'properties'->>'license' IS NOT NULL GROUP BY value ORDER BY count DESC",
    }

    # Source property path per facet (OGC API - Records - Part 2: Facets shape).
    facet_property = {
        "type": "type",
        "data_category": "properties.themes",
        "geographical_code": "properties.goat:geographical_code",
        "language_code": "properties.language.code",
        "distributor_name": "properties.contacts.name",
        "license": "properties.license",
    }

    facets: Dict[str, Any] = {}
    pool = _pool()
    async with pool.acquire() as conn:
        for key, query in agg_queries.items():
            rows = await conn.fetch(query, *params)
            facets[key] = {
                "type": "term",
                "property": facet_property.get(key, key),
                "buckets": [
                    {"value": row["value"], "count": row["count"]} for row in rows
                ],
                "more": False,
            }

    return {"facets": facets}


@router.get(
    f"/collections/{_COLLECTION_ID}/items/{{item_id}}",
    summary="OGC API Records — single catalog dataset record",
    response_model=Dict[str, Any],
    status_code=200,
)
async def ogc_records_item(item_id: str, request: Request) -> Dict[str, Any]:
    """Return a single dataset record by group id or layer UUID."""
    base_url = f"{_records_base(request)}/collections/{_COLLECTION_ID}/items"
    record = await get_record(_pool(), item_id, base_url=base_url)
    if record is None:
        raise HTTPException(status_code=404, detail="Record not found")
    return record


@router.get(
    "/nuts",
    summary="Search NUTS regions for spatial filtering",
    response_model=List[Dict[str, Any]],
)
async def nuts_search(
    q: Optional[str] = Query(None, description="Search by name or NUTS code"),
    level: Optional[int] = Query(None, ge=0, le=3, description="NUTS level (0-3)"),
    limit: int = Query(20, ge=1, le=100),
) -> List[Dict[str, Any]]:
    """Search NUTS regions from basic.nuts table. Returns matching regions with bbox."""
    return await search_nuts(_pool(), q, level, limit)


@router.get(
    "/nuts/{nuts_id}/geometry",
    summary="Get NUTS region boundary as GeoJSON",
    response_model=Dict[str, Any],
)
async def nuts_geometry(nuts_id: str) -> Dict[str, Any]:
    """Return the boundary of a NUTS region as a GeoJSON Feature."""
    feature = await get_nuts_geometry(_pool(), nuts_id)
    if feature is None:
        raise HTTPException(status_code=404, detail=f"NUTS region {nuts_id} not found")
    return feature
