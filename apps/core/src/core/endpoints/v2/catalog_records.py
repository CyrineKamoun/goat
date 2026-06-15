"""OGC API - Records endpoint for catalog layers.

Exposes catalog layers as an OGC API Records collection so that external
systems (CSW clients, AI agents, discovery portals) **and the GOAT frontend**
can consume structured geospatial metadata.

Records are grouped by CKAN package: one record per dataset, with individual
layers listed as ``distributions`` inside ``properties``.

Spec reference: https://ogcapi.ogc.org/records/

Extension parameters (beyond OGC core):
- ``bbox_boost``: spatial ranking without exclusion
- ``license``, ``publisher``, ``type``: additional faceted filters
"""

from __future__ import annotations

import copy
import json
import re
from collections import defaultdict
from typing import Any, Dict, List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select, text

from core.core.config import settings
from core.db.models.layer import Layer
from core.db.session import AsyncSession
from core.endpoints.deps import get_db

router = APIRouter()

_COLLECTION_ID = "datasets"
_COLLECTION_TITLE = "GOAT Catalog Datasets"
_COLLECTION_DESCRIPTION = (
    "Open geospatial datasets available in the GOAT catalog, "
    "harvested from CKAN and other sources. "
    "Each record represents a dataset with one or more distributions (layers)."
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _extent_to_bbox_and_coords(
    extent,
) -> tuple[list[float] | None, list[list[list[float]]] | None]:
    """Extract bbox [w,s,e,n] and polygon ring from an extent value.

    Handles GeoAlchemy2 WKBElement, WKT strings, and None.
    Returns (bbox, polygon_coords) or (None, None).
    """
    if extent is None:
        return None, None
    try:
        from geoalchemy2.shape import to_shape

        shape = to_shape(extent)
        west, south, east, north = shape.bounds
    except Exception:
        # Fallback: parse WKT string
        if not isinstance(extent, str):
            return None, None
        nums = re.findall(r"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?", extent)
        floats = list(map(float, nums))
        if len(floats) < 8:
            return None, None
        xs = floats[0::2]
        ys = floats[1::2]
        west, east = min(xs), max(xs)
        south, north = min(ys), max(ys)
    bbox = [west, south, east, north]
    coords = [[[west, south], [east, south], [east, north], [west, north], [west, south]]]
    return bbox, coords


def _parse_bbox_param(bbox_str: str) -> Dict[str, float] | None:
    """Parse 'west,south,east,north' into bind params, or None if malformed."""
    parts = bbox_str.split(",")
    if len(parts) != 4:
        return None
    try:
        return {
            "bbox_west": float(parts[0]),
            "bbox_south": float(parts[1]),
            "bbox_east": float(parts[2]),
            "bbox_north": float(parts[3]),
        }
    except ValueError:
        return None


def _build_filters(
    q: str | None,
    bbox: str | None,
    themes: str | None,
    language: str | None,
    year: int | None,
    source_format: str | None,
    license_: str | None,
    publisher: str | None,
    type_: str | None,
    geographical_code: str | None = None,
    datetime_: str | None = None,
) -> tuple[str, Dict[str, Any]]:
    """Build WHERE clause and bind params from OGC query parameters.

    Operates on table alias ``cl`` (customer.layer).
    """
    filters = ["cl.in_catalog = TRUE"]
    bind: Dict[str, Any] = {}

    if q:
        # Build prefix tsquery: "park platz" → "park:* & platz:*"
        # This matches "Parkdaten", "Parkplätze", etc.
        q_terms = [t.strip() for t in q.split() if t.strip()]
        q_prefix = " & ".join(f"{t}:*" for t in q_terms) if q_terms else q
        filters.append(
            """
            (
                cl.record_jsonb IS NOT NULL AND (
                    to_tsvector('simple',
                        coalesce(cl.record_jsonb->'properties'->>'title', '') || ' ' ||
                        coalesce(cl.record_jsonb->'properties'->>'description', '') || ' ' ||
                        coalesce(
                            (SELECT string_agg(kw->>'value', ' ')
                             FROM jsonb_array_elements(
                                 coalesce(cl.record_jsonb->'properties'->'keywords', '[]'::jsonb)
                             ) AS kw),
                            ''
                        )
                    ) @@ to_tsquery('simple', :q)
                )
                OR (
                    cl.record_jsonb IS NULL AND (
                        lower(cl.name) LIKE :q_like
                        OR lower(coalesce(cl.description, '')) LIKE :q_like
                    )
                )
            )
            """
        )
        bind["q"] = q_prefix
        bind["q_like"] = f"%{q.lower()}%"

    if themes:
        theme_list = [t.strip() for t in themes.split(",") if t.strip()]
        theme_clauses = []
        for i, theme_val in enumerate(theme_list):
            param = f"theme_{i}"
            theme_clauses.append(
                f"cl.record_jsonb->'properties'->'themes' @> CAST(:{param} AS jsonb)"
            )
            bind[param] = json.dumps([{"concepts": [{"id": theme_val}]}])
        if theme_clauses:
            filters.append("(" + " OR ".join(theme_clauses) + ")")

    if language:
        lang_list = [v.strip() for v in language.split(",") if v.strip()]
        filters.append(
            "cl.record_jsonb->'properties'->>'language' = ANY(:lang_vals)"
        )
        bind["lang_vals"] = lang_list

    if year:
        filters.append(
            "cl.record_jsonb IS NOT NULL AND "
            "(cl.record_jsonb->'properties'->'extent'->'temporal'->'interval'->0->0)::int = :year"
        )
        bind["year"] = year

    if source_format:
        filters.append(
            "cl.record_jsonb IS NOT NULL AND "
            "cl.record_jsonb->'properties'->>'source_format' = :source_format"
        )
        bind["source_format"] = source_format

    if license_:
        lic_list = [v.strip() for v in license_.split(",") if v.strip()]
        filters.append(
            "cl.record_jsonb->'properties'->>'license' = ANY(:license_vals)"
        )
        bind["license_vals"] = lic_list

    if publisher:
        pub_list = [v.strip() for v in publisher.split(",") if v.strip()]
        filters.append(
            "cl.record_jsonb->'properties'->'publisher'->>'name' = ANY(:publisher_vals)"
        )
        bind["publisher_vals"] = pub_list

    if type_:
        type_list = [v.strip() for v in type_.split(",") if v.strip()]
        filters.append("cl.type = ANY(:type_vals)")
        bind["type_vals"] = type_list

    if geographical_code:
        geo_list = [v.strip() for v in geographical_code.split(",") if v.strip()]
        filters.append(
            "cl.record_jsonb->'properties'->>'geographical_code' = ANY(:geo_vals)"
        )
        bind["geo_vals"] = geo_list

    if datetime_:
        # OGC datetime filter on SOURCE dates (record_jsonb.properties.created/updated),
        # falling back to cl.created_at/updated_at for layers without record_jsonb.
        # Supports: "2023-01-01/..", "../2024-12-31", "2023-01-01/2024-12-31", "2023-01-01"
        _dt_col = (
            "COALESCE("
            "  (cl.record_jsonb->'properties'->>'updated')::timestamptz,"
            "  (cl.record_jsonb->'properties'->>'created')::timestamptz,"
            "  cl.updated_at"
            ")"
        )
        from datetime import date, datetime as dt_mod

        def _parse_dt(s: str) -> dt_mod | date | None:
            for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
                try:
                    return dt_mod.strptime(s, fmt)
                except ValueError:
                    continue
            return None

        parts = datetime_.split("/")
        if len(parts) == 2:
            dt_start_s, dt_end_s = parts[0].strip(), parts[1].strip()
            if dt_start_s and dt_start_s != "..":
                parsed = _parse_dt(dt_start_s)
                if parsed:
                    filters.append(f"{_dt_col} >= :dt_start")
                    bind["dt_start"] = parsed
            if dt_end_s and dt_end_s != "..":
                parsed = _parse_dt(dt_end_s)
                if parsed:
                    filters.append(f"{_dt_col} <= :dt_end")
                    bind["dt_end"] = parsed
        elif len(parts) == 1 and datetime_.strip():
            parsed = _parse_dt(datetime_.strip())
            if parsed:
                filters.append(f"CAST({_dt_col} AS date) = CAST(:dt_exact AS date)")
                bind["dt_exact"] = parsed

    if bbox:
        bbox_params = _parse_bbox_param(bbox)
        if bbox_params:
            bind.update(bbox_params)
            # `&&` is a cheap bbox-overlap index pre-filter, but two adjacent regions'
            # bounding rectangles overlap even when neither contains the other (e.g.
            # Niederbayern's box clips into München). Refine with a real area test: keep
            # a dataset only when its extent overlaps the selected region by >= 30% of the
            # SMALLER of the two — so a region-covering dataset (Oberbayern⊇München) and a
            # dataset within the region both match, but a mere edge sliver is dropped.
            _env_sql = "ST_MakeEnvelope(:bbox_west, :bbox_south, :bbox_east, :bbox_north, 4326)"
            filters.append(
                f"cl.extent && {_env_sql} "
                f"AND ST_Area(ST_Intersection(cl.extent::geometry, {_env_sql})) "
                f">= 0.3 * LEAST(ST_Area(cl.extent::geometry), ST_Area({_env_sql}))"
            )

    return " AND ".join(filters), bind


def _enum_str(v: Any) -> str | None:
    """Return the string value of an Enum, or the value itself if already a str.

    DataCategory and DataLicense columns can hold values outside their enum
    (e.g. 'other'), which SQLAlchemy returns as plain strings.
    """
    if v is None:
        return None
    return v.value if hasattr(v, "value") else str(v)


def _build_record(
    rep_layer: Layer,
    distributions: List[Dict[str, Any]],
    base_url: str,
    record_id: str,
    package_record_jsonb: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """Build a grouped OGC Record Feature.

    Prefers package-level record_jsonb (from datacatalog.package) as the
    dataset metadata source. Falls back to the representative layer's
    record_jsonb, then to flat columns.
    """
    source_jsonb = package_record_jsonb or rep_layer.record_jsonb
    if source_jsonb:
        # record_jsonb is the single source of truth for catalog layers.
        # User edits flow into it via record_overrides (source=user, priority 100)
        # and merge_record_overrides. No flat-column overlay here.
        record = copy.deepcopy(source_jsonb)
        record["id"] = record_id
        props = record.get("properties") or {}
    else:
        bbox, coords = _extent_to_bbox_and_coords(rep_layer.extent)
        geometry = {"type": "Polygon", "coordinates": coords} if coords else None
        keywords: List[str] = list(rep_layer.tags or [])
        props: Dict[str, Any] = {
            "type": "dataset",
            "title": rep_layer.name,
            "description": rep_layer.description,
            "keywords": keywords,
            "language": rep_layer.language_code,
            "created": rep_layer.created_at.isoformat() if rep_layer.created_at else None,
            "updated": rep_layer.updated_at.isoformat() if rep_layer.updated_at else None,
            "publisher": (
                {"name": rep_layer.distributor_name, "email": rep_layer.distributor_email}
                if rep_layer.distributor_name
                else None
            ),
            "license": _enum_str(rep_layer.license),
            "themes": (
                [{"concepts": [{"id": _enum_str(rep_layer.data_category)}]}]
                if rep_layer.data_category
                else []
            ),
            "extent": {
                "spatial": {"bbox": [bbox]} if bbox else None,
                "temporal": (
                    {"interval": [[rep_layer.data_reference_year, None]]}
                    if rep_layer.data_reference_year
                    else None
                ),
            },
            "goat_layer_id": str(rep_layer.id),
        }
        record = {
            "id": record_id,
            "type": "Feature",
            "geometry": geometry,
            "properties": props,
        }

    # Always inject thumbnail and distributions from the Layer model
    if rep_layer.thumbnail_url:
        props["thumbnail_url"] = rep_layer.thumbnail_url
    props["distributions"] = distributions

    links: List[Dict[str, Any]] = [
        {
            "rel": "self",
            "type": "application/geo+json",
            "title": "This record",
            "href": f"{base_url}/{record_id}",
        },
        {
            "rel": "collection",
            "type": "application/json",
            "title": _COLLECTION_TITLE,
            "href": base_url,
        },
    ]
    existing = [lk for lk in props.get("links", []) if lk.get("rel") == "enclosure"]
    props["links"] = links + existing
    record["properties"] = props
    return record


def _get_group_sql() -> tuple[str, str, str, str]:
    """Return (from_sql, group_expr, group_id_expr, child_name_expr).

    Groups by layer_group_id — layers with a group are grouped together,
    layers without a group are each their own record.
    No dependency on datacatalog schema.
    """
    cs = settings.CUSTOMER_SCHEMA
    from_sql = f"FROM {cs}.layer cl"
    # Group by layer_group_id if set, otherwise by layer id (each is its own record)
    group_expr = "COALESCE(cl.layer_group_id::text, cl.id::text)"
    group_id_expr = "cl.layer_group_id::text"
    child_name_expr = "cl.name"
    return from_sql, group_expr, group_id_expr, child_name_expr


# ---------------------------------------------------------------------------
# OGC API Records — static endpoints
# ---------------------------------------------------------------------------

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
            {"rel": "self", "type": "application/json", "title": "This document", "href": base},
            {"rel": "conformance", "type": "application/json", "title": "Conformance", "href": f"{base}/conformance"},
            {"rel": "data", "type": "application/json", "title": "Collections", "href": f"{base}/collections"},
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
                    {"rel": "items", "type": "application/geo+json", "title": "Records", "href": f"{base}/items"},
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
            {"rel": "self", "type": "application/json", "title": "This collection", "href": base},
            {"rel": "items", "type": "application/geo+json", "title": "Records", "href": f"{base}/items"},
        ],
    }


# ---------------------------------------------------------------------------
# OGC API Records — items (grouped by dataset/package)
# ---------------------------------------------------------------------------

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
    year: Optional[int] = Query(None, description="Filter by data reference year", example=2023),
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

    Records are **grouped by CKAN package** — one record per dataset with a
    ``distributions`` array listing individual layers/files. Single-layer
    datasets have one entry in ``distributions``.

    Standard OGC parameters: ``q``, ``bbox``, ``datetime``, ``themes``, ``sortby``, ``limit``, ``offset``.
    Extension parameters: ``bbox_boost``, ``license``, ``publisher``, ``type``, ``geographical_code``.
    """
    where_sql, bind = _build_filters(
        q=q, bbox=bbox, themes=themes, language=language, year=year,
        source_format=source_format, license_=license, publisher=publisher, type_=type,
        geographical_code=geographical_code, datetime_=datetime,
    )
    from_sql, group_expr, group_id_expr, child_name_expr = _get_group_sql()

    # Count total grouped datasets
    count_res = await async_session.execute(
        text(f"SELECT COUNT(DISTINCT {group_expr}) {from_sql} WHERE {where_sql}"),
        bind,
    )
    number_matched: int = count_res.scalar() or 0

    if number_matched == 0:
        base_url = f"{settings.API_V2_STR}/catalog/records/collections/{_COLLECTION_ID}/items"
        return {
            "type": "FeatureCollection",
            "numberMatched": 0,
            "numberReturned": 0,
            "links": [{"rel": "self", "type": "application/geo+json", "href": base_url}],
            "features": [],
        }

    # Parse sortby into SQL ORDER BY clauses for the outer query.
    # Allowed properties map to SQL expressions on the CTE columns.
    _SORTBY_MAP = {
        "title": "COALESCE(record_jsonb->'properties'->>'title', name)",
        "updated": "updated_at",
        "created": "created_at",
        "type": "type",
    }
    outer_order_clauses: List[str] = []

    # bbox_boost adds a primary sort clause
    if bbox_boost:
        bbox_boost_params = _parse_bbox_param(bbox_boost)
        if bbox_boost_params:
            bind.update({f"boost_{k}": v for k, v in bbox_boost_params.items()})
            outer_order_clauses.append(
                "CASE WHEN extent IS NOT NULL"
                " AND extent && ST_MakeEnvelope("
                ":boost_bbox_west, :boost_bbox_south,"
                " :boost_bbox_east, :boost_bbox_north, 4326)"
                " THEN 0 ELSE 1 END"
            )

    # sortby adds user-requested sort clauses
    if sortby:
        for part in sortby.split(","):
            part = part.strip()
            if not part:
                continue
            if part.startswith("-"):
                direction = "DESC"
                prop = part[1:]
            elif part.startswith("+"):
                direction = "ASC"
                prop = part[1:]
            else:
                direction = "ASC"
                prop = part
            sql_expr = _SORTBY_MAP.get(prop)
            if sql_expr:
                outer_order_clauses.append(f"{sql_expr} {direction}")

    # Default fallback sort
    if not outer_order_clauses:
        outer_order_clauses.append("updated_at DESC")

    # When we need custom ordering (sortby or bbox_boost), use a CTE
    # because DISTINCT ON requires ORDER BY to start with the group expression.
    needs_cte = bool(bbox_boost or sortby)

    if needs_cte:
        outer_order_sql = ", ".join(outer_order_clauses)
        reps_query = f"""
            WITH reps AS (
                SELECT DISTINCT ON ({group_expr})
                    cl.id AS representative_id,
                    {group_expr} AS group_key,
                    {group_id_expr} AS group_id,
                    cl.extent,
                    cl.updated_at,
                    cl.created_at,
                    cl.name,
                    cl.type,
                    cl.record_jsonb
                {from_sql}
                WHERE {where_sql}
                ORDER BY {group_expr}, cl.updated_at DESC
            )
            SELECT representative_id, group_key, group_id
            FROM reps
            ORDER BY {outer_order_sql}
            LIMIT :limit OFFSET :offset
        """
    else:
        reps_query = f"""
            SELECT DISTINCT ON ({group_expr})
                cl.id AS representative_id,
                {group_expr} AS group_key,
                {group_id_expr} AS group_id
            {from_sql}
            WHERE {where_sql}
            ORDER BY {group_expr}, cl.updated_at DESC
            LIMIT :limit OFFSET :offset
        """

    reps_res = await async_session.execute(
        text(reps_query),
        {**bind, "limit": limit, "offset": offset},
    )
    page_groups = reps_res.fetchall()

    if not page_groups:
        base_url = f"{settings.API_V2_STR}/catalog/records/collections/{_COLLECTION_ID}/items"
        return {
            "type": "FeatureCollection",
            "numberMatched": number_matched,
            "numberReturned": 0,
            "links": [{"rel": "self", "type": "application/geo+json", "href": base_url}],
            "features": [],
        }

    rep_ids = [g.representative_id for g in page_groups]
    group_keys = [g.group_key for g in page_groups]

    # Fetch representative Layer objects
    rep_res = await async_session.execute(select(Layer).where(Layer.id.in_(rep_ids)))
    rep_layers: Dict[str, Layer] = {str(l.id): l for l in rep_res.scalars().all()}

    # Fetch sibling layers (distributions) for each group
    siblings_res = await async_session.execute(
        text(
            f"""
            SELECT cl.id,
                   {child_name_expr} AS name,
                   cl.type,
                   cl.feature_layer_geometry_type,
                   {group_expr} AS group_key
            {from_sql}
            WHERE cl.in_catalog = TRUE
              AND {group_expr} = ANY(:group_keys)
            ORDER BY name
            """
        ),
        {"group_keys": group_keys},
    )
    distributions_by_group: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in siblings_res.fetchall():
        distributions_by_group[row.group_key].append({
            "layer_id": str(row.id),
            "name": row.name,
            "type": row.type,
            "geometry_type": row.feature_layer_geometry_type,
        })

    # Fetch group-level metadata from customer.layer_group (authoritative source).
    cs = settings.CUSTOMER_SCHEMA
    group_ids_in_page = [g.group_id for g in page_groups if g.group_id]
    group_records: Dict[str, Dict[str, Any]] = {}
    if group_ids_in_page:
        try:
            grp_res = await async_session.execute(
                text(
                    f"SELECT id::text, record_jsonb FROM {cs}.layer_group"
                    f" WHERE id = ANY(CAST(:gids AS uuid[])) AND record_jsonb IS NOT NULL"
                ),
                {"gids": group_ids_in_page},
            )
            for row in grp_res.fetchall():
                if row.record_jsonb:
                    group_records[row.id] = row.record_jsonb
        except Exception:
            pass  # Table might not exist yet — fall back to layer metadata

    # Build OGC Record features
    base_url = f"{settings.API_V2_STR}/catalog/records/collections/{_COLLECTION_ID}/items"
    features: List[Dict[str, Any]] = []
    for group in page_groups:
        rep = rep_layers.get(str(group.representative_id))
        if not rep:
            continue
        record_id = group.group_id or str(group.representative_id)
        dists = distributions_by_group.get(group.group_key, [{
            "layer_id": str(rep.id),
            "name": rep.name,
            "type": rep.type,
            "geometry_type": rep.feature_layer_geometry_type,
        }])
        pkg_rj = group_records.get(group.group_id) if group.group_id else None
        features.append(_build_record(rep, dists, base_url, record_id, package_record_jsonb=pkg_rj))

    # Pagination links
    links: List[Dict[str, Any]] = [
        {"rel": "self", "type": "application/geo+json", "href": base_url},
    ]
    if offset + limit < number_matched:
        links.append({
            "rel": "next",
            "type": "application/geo+json",
            "href": f"{base_url}?limit={limit}&offset={offset + limit}",
        })
    if offset > 0:
        links.append({
            "rel": "prev",
            "type": "application/geo+json",
            "href": f"{base_url}?limit={limit}&offset={max(0, offset - limit)}",
        })

    return {
        "type": "FeatureCollection",
        "numberMatched": number_matched,
        "numberReturned": len(features),
        "links": links,
        "features": features,
    }


# ---------------------------------------------------------------------------
# OGC API Records — aggregates (extension for faceted filtering)
# Must be registered BEFORE the {item_id} route to avoid path conflict.
# ---------------------------------------------------------------------------

@router.get(
    f"/collections/{_COLLECTION_ID}/items/aggregates",
    summary="OGC API Records — filter value counts for faceted search",
    response_model=Dict[str, Any],
    status_code=200,
)
async def ogc_records_aggregates(
    async_session: AsyncSession = Depends(get_db),
    bbox: Optional[str] = Query(None, description="Bounding box filter as 'west,south,east,north'"),
    q: Optional[str] = Query(None, description="Free-text search"),
    themes: Optional[str] = Query(None, description="Filter by data category (comma-separated)"),
    language: Optional[str] = Query(None, description="Filter by language code"),
    year: Optional[int] = Query(None, description="Filter by year"),
    source_format: Optional[str] = Query(None, description="Filter by source format"),
    license: Optional[str] = Query(None, description="Filter by license (comma-separated)"),
    publisher: Optional[str] = Query(None, description="Filter by publisher (comma-separated)"),
    type: Optional[str] = Query(None, description="Filter by layer type (comma-separated)"),
    geographical_code: Optional[str] = Query(None, description="Filter by geographical code (comma-separated)"),
    datetime: Optional[str] = Query(None, description="OGC temporal filter: '2023-01-01/2024-12-31'"),
) -> Dict[str, Any]:
    """Return aggregated counts of filterable metadata values.

    Accepts the same filter parameters as the items endpoint. Counts reflect
    the currently active filters, enabling progressive faceted search.
    """
    where_sql, bind = _build_filters(
        q=q, bbox=bbox, themes=themes, language=language, year=year,
        source_format=source_format, license_=license, publisher=publisher, type_=type,
        geographical_code=geographical_code, datetime_=datetime,
    )
    cs = settings.CUSTOMER_SCHEMA

    # Keys match the frontend DatasetMetadataAggregated schema.
    # All catalog metadata reads come from record_jsonb only (OGC-compliant).
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
            f"SELECT cl.record_jsonb->'properties'->>'geographical_code' AS value,"
            f" COUNT(*) AS count"
            f" FROM {cs}.layer cl WHERE {where_sql}"
            f" AND cl.record_jsonb->'properties'->>'geographical_code' IS NOT NULL"
            f" GROUP BY value ORDER BY count DESC"
        ),
        "language_code": f"SELECT cl.record_jsonb->'properties'->>'language' AS value, COUNT(*) AS count FROM {cs}.layer cl WHERE {where_sql} AND cl.record_jsonb->'properties'->>'language' IS NOT NULL GROUP BY value ORDER BY count DESC",
        "distributor_name": f"SELECT cl.record_jsonb->'properties'->'publisher'->>'name' AS value, COUNT(*) AS count FROM {cs}.layer cl WHERE {where_sql} AND cl.record_jsonb->'properties'->'publisher'->>'name' IS NOT NULL GROUP BY value ORDER BY count DESC",
        "license": f"SELECT cl.record_jsonb->'properties'->>'license' AS value, COUNT(*) AS count FROM {cs}.layer cl WHERE {where_sql} AND cl.record_jsonb->'properties'->>'license' IS NOT NULL GROUP BY value ORDER BY count DESC",
    }

    result: Dict[str, List[Dict[str, Any]]] = {}
    for key, query in agg_queries.items():
        res = await async_session.execute(text(query), bind)
        result[key] = [{"value": row.value, "count": row.count} for row in res.fetchall()]

    return result


# ---------------------------------------------------------------------------
# OGC API Records — single item
# ---------------------------------------------------------------------------

@router.get(
    f"/collections/{_COLLECTION_ID}/items/{{item_id}}",
    summary="OGC API Records — single catalog dataset record",
    response_model=Dict[str, Any],
    status_code=200,
)
async def ogc_records_item(
    item_id: str,
    async_session: AsyncSession = Depends(get_db),
) -> Dict[str, Any]:
    """Return a single dataset record by package_id or layer UUID.

    For multi-file datasets, returns the grouped record with all distributions.
    """
    from_sql, group_expr, group_id_expr, child_name_expr = _get_group_sql()
    cs = settings.CUSTOMER_SCHEMA

    # Try to find by layer_group_id or layer UUID
    rep_res = await async_session.execute(
        text(
            f"""
            SELECT DISTINCT ON ({group_expr})
                cl.id AS representative_id,
                {group_expr} AS group_key,
                {group_id_expr} AS group_id
            {from_sql}
            WHERE cl.in_catalog = TRUE
              AND ({group_id_expr} = :item_id OR cl.id::text = :item_id)
            ORDER BY {group_expr}, cl.updated_at DESC
            LIMIT 1
            """
        ),
        {"item_id": item_id},
    )
    group = rep_res.fetchone()
    if group is None:
        raise HTTPException(status_code=404, detail="Record not found")

    # Fetch representative layer
    layer_res = await async_session.execute(
        select(Layer).where(Layer.id == group.representative_id)
    )
    rep = layer_res.scalars().first()
    if rep is None:
        raise HTTPException(status_code=404, detail="Record not found")

    # Fetch distributions
    siblings_res = await async_session.execute(
        text(
            f"""
            SELECT cl.id,
                   {child_name_expr} AS name,
                   cl.type,
                   cl.feature_layer_geometry_type
            {from_sql}
            WHERE cl.in_catalog = TRUE
              AND {group_expr} = :group_key
            ORDER BY name
            """
        ),
        {"group_key": group.group_key},
    )
    distributions = [
        {
            "layer_id": str(row.id),
            "name": row.name,
            "type": row.type,
            "geometry_type": row.feature_layer_geometry_type,
        }
        for row in siblings_res.fetchall()
    ]

    # Fetch group-level metadata from customer.layer_group
    pkg_rj = None
    if group.group_id:
        try:
            pkg_res = await async_session.execute(
                text(
                    f"SELECT record_jsonb FROM {cs}.layer_group"
                    f" WHERE id = CAST(:gid AS uuid) AND record_jsonb IS NOT NULL"
                ),
                {"gid": group.group_id},
            )
            pkg_row = pkg_res.fetchone()
            if pkg_row and pkg_row.record_jsonb:
                pkg_rj = pkg_row.record_jsonb
        except Exception:
            pass

    base_url = f"{settings.API_V2_STR}/catalog/records/collections/{_COLLECTION_ID}/items"
    record_id = group.group_id or str(group.representative_id)
    return _build_record(rep, distributions, base_url, record_id, package_record_jsonb=pkg_rj)


# ---------------------------------------------------------------------------
# NUTS region search
# ---------------------------------------------------------------------------

@router.get(
    "/nuts",
    summary="Search NUTS regions for spatial filtering",
    response_model=List[Dict[str, Any]],
)
async def nuts_search(
    q: Optional[str] = Query(None, description="Search by name or NUTS code"),
    level: Optional[int] = Query(None, ge=0, le=3, description="NUTS level (0-3)"),
    limit: int = Query(20, ge=1, le=100),
    async_session: AsyncSession = Depends(get_db),
) -> List[Dict[str, Any]]:
    """Search NUTS regions from basic.nuts table. Returns matching regions with bbox."""
    filters = []
    bind: Dict[str, Any] = {"limit": limit}

    if q and q.strip():
        filters.append(
            "(nuts_name ILIKE :q OR nuts_id ILIKE :q)"
        )
        bind["q"] = f"{q.strip()}%"

    if level is not None:
        filters.append("levl_code = :level")
        bind["level"] = level

    where = ("WHERE " + " AND ".join(filters)) if filters else ""

    result = await async_session.execute(
        text(
            f"""
            SELECT nuts_id, nuts_name, levl_code AS level, cntr_code AS country,
                   ST_XMin(geom) AS west, ST_YMin(geom) AS south,
                   ST_XMax(geom) AS east, ST_YMax(geom) AS north
            FROM basic.nuts
            {where}
            ORDER BY levl_code, nuts_name
            LIMIT :limit
            """
        ),
        bind,
    )

    return [
        {
            "nuts_id": row.nuts_id,
            "nuts_name": row.nuts_name,
            "level": row.level,
            "country": row.country,
            "bbox": [
                round(row.west, 6),
                round(row.south, 6),
                round(row.east, 6),
                round(row.north, 6),
            ],
        }
        for row in result.fetchall()
    ]


@router.get(
    "/nuts/{nuts_id}/geometry",
    summary="Get NUTS region boundary as GeoJSON",
    response_model=Dict[str, Any],
)
async def nuts_geometry(
    nuts_id: str,
    async_session: AsyncSession = Depends(get_db),
) -> Dict[str, Any]:
    """Return the boundary of a NUTS region as a GeoJSON Feature."""
    result = await async_session.execute(
        text(
            """
            SELECT nuts_id, nuts_name, levl_code AS level,
                   ST_AsGeoJSON(ST_Transform(geom, 4326))::json AS geometry
            FROM basic.nuts
            WHERE nuts_id = :nuts_id
            LIMIT 1
            """
        ),
        {"nuts_id": nuts_id},
    )
    row = result.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail=f"NUTS region {nuts_id} not found")

    return {
        "type": "Feature",
        "properties": {
            "nuts_id": row.nuts_id,
            "nuts_name": row.nuts_name,
            "level": row.level,
        },
        "geometry": row.geometry,
    }
