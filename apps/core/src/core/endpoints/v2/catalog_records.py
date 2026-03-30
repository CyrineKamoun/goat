"""OGC API - Records endpoint for catalog layers.

Exposes catalog layers as an OGC API Records collection so that external
systems (CSW clients, AI agents, discovery portals) can consume structured
geospatial metadata without needing GOAT-specific authentication.

Spec reference: https://ogcapi.ogc.org/records/
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional
from uuid import UUID

from fastapi import APIRouter, Query
from sqlalchemy import select, text

from core.core.config import settings
from core.db.models.layer import DataCategory, DataLicense, Layer
from core.db.session import AsyncSession
from core.endpoints.deps import get_db

from fastapi import Depends

router = APIRouter()

_COLLECTION_ID = "datasets"
_COLLECTION_TITLE = "GOAT Catalog Datasets"
_COLLECTION_DESCRIPTION = (
    "Open geospatial datasets available in the GOAT catalog, "
    "harvested from CKAN and other sources. "
    "Each record corresponds to a catalog layer with ISO 19139 metadata."
)


def _bbox_to_polygon_coords(
    extent_wkt: str | None,
) -> list[list[list[float]]] | None:
    """Extract exterior ring from a WKT MultiPolygon/Polygon extent."""
    if not extent_wkt:
        return None
    import re

    nums = re.findall(r"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?", extent_wkt)
    floats = list(map(float, nums))
    if len(floats) < 8:
        return None
    xs = floats[0::2]
    ys = floats[1::2]
    west, east = min(xs), max(xs)
    south, north = min(ys), max(ys)
    return [[[west, south], [east, south], [east, north], [west, north], [west, south]]]


def _layer_to_record(layer: Layer, base_url: str) -> Dict[str, Any]:
    """Convert a Layer ORM object to an OGC Records Feature dict."""
    coords = _bbox_to_polygon_coords(layer.extent)
    geometry = (
        {"type": "Polygon", "coordinates": coords} if coords else None
    )

    links: List[Dict[str, Any]] = [
        {
            "rel": "self",
            "type": "application/geo+json",
            "title": "This record",
            "href": f"{base_url}/{layer.id}",
        },
        {
            "rel": "collection",
            "type": "application/json",
            "title": _COLLECTION_TITLE,
            "href": f"{base_url}",
        },
    ]
    if layer.xml_metadata:
        links.append(
            {
                "rel": "alternate",
                "type": "application/xml",
                "title": "ISO 19139 metadata",
                "href": f"{base_url}/{layer.id}?f=xml",
            }
        )
    if layer.distribution_url:
        links.append(
            {
                "rel": "enclosure",
                "type": "application/octet-stream",
                "title": "Download",
                "href": str(layer.distribution_url),
            }
        )

    keywords: List[str] = list(layer.tags or [])

    props: Dict[str, Any] = {
        "type": "dataset",
        "title": layer.name,
        "description": layer.description,
        "keywords": keywords,
        "language": layer.language_code,
        "created": layer.created_at.isoformat() if layer.created_at else None,
        "updated": layer.updated_at.isoformat() if layer.updated_at else None,
        "publisher": {
            "name": layer.distributor_name,
            "email": layer.distributor_email,
        }
        if layer.distributor_name
        else None,
        "license": layer.license.value if layer.license else None,
        "themes": [{"concepts": [{"id": layer.data_category.value}]}]
        if layer.data_category
        else [],
        "extent": {
            "spatial": {"bbox": [coords[0][0][:2] + coords[0][2][:2]]}
            if coords
            else None,
            "temporal": {"interval": [[layer.data_reference_year, None]]}
            if layer.data_reference_year
            else None,
        },
        "goat_layer_id": str(layer.id),
        "links": links,
    }

    return {
        "id": str(layer.id),
        "type": "Feature",
        "geometry": geometry,
        "properties": props,
    }


@router.get(
    "",
    summary="OGC API Records — landing page",
    response_model=Dict[str, Any],
    status_code=200,
    include_in_schema=True,
)
async def ogc_records_landing() -> Dict[str, Any]:
    """OGC API Records landing page with conformance classes and collection links."""
    base = f"{settings.API_V2_STR}/catalog/records"
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
            "http://www.opengis.net/spec/ogcapi-records-1/1.0/conf/core",
            "http://www.opengis.net/spec/ogcapi-records-1/1.0/conf/json",
        ],
    }


@router.get(
    "/conformance",
    summary="OGC API Records — conformance classes",
    response_model=Dict[str, Any],
    status_code=200,
    include_in_schema=False,
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
    response_model=Dict[str, Any],
    status_code=200,
    include_in_schema=False,
)
async def ogc_records_collections() -> Dict[str, Any]:
    base = f"{settings.API_V2_STR}/catalog/records/collections/{_COLLECTION_ID}"
    return {
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
                    }
                ],
            }
        ]
    }


@router.get(
    f"/collections/{_COLLECTION_ID}",
    summary="OGC API Records — collection metadata",
    response_model=Dict[str, Any],
    status_code=200,
    include_in_schema=False,
)
async def ogc_records_collection() -> Dict[str, Any]:
    base = f"{settings.API_V2_STR}/catalog/records/collections/{_COLLECTION_ID}"
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
    async_session: AsyncSession = Depends(get_db),
    limit: int = Query(10, ge=1, le=100, description="Number of records to return"),
    offset: int = Query(0, ge=0, description="Number of records to skip"),
    bbox: Optional[str] = Query(
        None,
        description="Bounding box filter as 'west,south,east,north'",
        example="-180,-90,180,90",
    ),
    q: Optional[str] = Query(None, description="Free-text search across title and description"),
    data_category: Optional[DataCategory] = Query(None, description="Filter by data category"),
    language: Optional[str] = Query(None, description="Filter by ISO 639-1 language code"),
    license: Optional[DataLicense] = Query(None, description="Filter by license"),
) -> Dict[str, Any]:
    """Return catalog layers as an OGC API Records GeoJSON FeatureCollection.

    This endpoint is intentionally open (no authentication required) to support
    AI agents, search crawlers, and CSW/OGC clients discovering available datasets.
    """
    schema = settings.CUSTOMER_SCHEMA

    filters = ["in_catalog = TRUE", "type = 'feature'"]
    bind: Dict[str, Any] = {}

    if q:
        filters.append(
            "(lower(name) LIKE :q OR lower(description) LIKE :q OR lower(distributor_name) LIKE :q)"
        )
        bind["q"] = f"%{q.lower()}%"

    if data_category:
        filters.append("data_category = :data_category")
        bind["data_category"] = data_category.value

    if language:
        filters.append("language_code = :language")
        bind["language"] = language

    if license:
        filters.append("license = :license")
        bind["license"] = license.value

    if bbox:
        parts = bbox.split(",")
        if len(parts) == 4:
            filters.append(
                "extent && ST_MakeEnvelope(:bbox_west, :bbox_south, :bbox_east, :bbox_north, 4326)"
            )
            bind["bbox_west"] = float(parts[0])
            bind["bbox_south"] = float(parts[1])
            bind["bbox_east"] = float(parts[2])
            bind["bbox_north"] = float(parts[3])

    where_sql = " AND ".join(filters)

    count_res = await async_session.execute(
        text(f"SELECT COUNT(*) FROM {schema}.layer WHERE {where_sql}"),
        bind,
    )
    number_matched: int = count_res.scalar() or 0

    layer_ids_res = await async_session.execute(
        text(
            f"SELECT id FROM {schema}.layer WHERE {where_sql}"
            f" ORDER BY updated_at DESC LIMIT :limit OFFSET :offset"
        ),
        {**bind, "limit": limit, "offset": offset},
    )
    layer_ids = [row[0] for row in layer_ids_res.fetchall()]

    layers: list[Layer] = []
    if layer_ids:
        layers_res = await async_session.execute(
            select(Layer).where(Layer.id.in_(layer_ids)).order_by(Layer.updated_at.desc())
        )
        layers = list(layers_res.scalars().all())

    base_url = (
        f"{settings.API_V2_STR}/catalog/records/collections/{_COLLECTION_ID}/items"
    )

    features = [_layer_to_record(layer, base_url) for layer in layers]

    return {
        "type": "FeatureCollection",
        "numberMatched": number_matched,
        "numberReturned": len(features),
        "links": [
            {
                "rel": "self",
                "type": "application/geo+json",
                "href": base_url,
            }
        ],
        "features": features,
    }


@router.get(
    f"/collections/{_COLLECTION_ID}/items/{{item_id}}",
    summary="OGC API Records — single catalog dataset record",
    response_model=Dict[str, Any],
    status_code=200,
)
async def ogc_records_item(
    item_id: UUID,
    async_session: AsyncSession = Depends(get_db),
) -> Dict[str, Any]:
    """Return a single catalog layer as an OGC Records Feature."""
    schema = settings.CUSTOMER_SCHEMA
    res = await async_session.execute(
        select(Layer).where(
            Layer.id == item_id,
            Layer.in_catalog == True,  # noqa: E712
        )
    )
    layer = res.scalars().first()
    if layer is None:
        from fastapi import HTTPException

        raise HTTPException(status_code=404, detail="Record not found")

    base_url = (
        f"{settings.API_V2_STR}/catalog/records/collections/{_COLLECTION_ID}/items"
    )
    return _layer_to_record(layer, base_url)
