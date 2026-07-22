"""Reusable catalog query functions shared by the OGC Records router and the MCP server.

Read-only PostgreSQL metadata queries (customer.layer, customer.layer_group,
basic.nuts) over geoapi's asyncpg pool. No FastAPI/Request dependency so both the
HTTP endpoints and the MCP tools can call these directly.
"""

from __future__ import annotations

import copy
import json
import re
from collections import defaultdict
from datetime import date
from datetime import datetime as dt_mod
from typing import Any, Dict, List, Optional

import asyncpg
from goatlib.services.s3 import S3Service

_s3 = S3Service()

_COLLECTION_ID = "datasets"
_COLLECTION_TITLE = "GOAT Catalog Datasets"
_COLLECTION_DESCRIPTION = (
    "Open geospatial datasets available in the GOAT catalog, "
    "harvested from CKAN and other sources. "
    "Each record is a dataset; a grouped dataset links its member layers via "
    'rel="item".'
)

# Representative-layer columns shared by the items + single-item queries.
_REP_COLS = (
    "id, name, type, feature_layer_geometry_type, thumbnail_url, record_jsonb, "
    "ST_AsGeoJSON(ST_Envelope(extent::geometry)) AS extent_geojson"
)


def _loads(v: Any) -> Any:
    """Decode a JSONB column value (asyncpg returns JSONB as str)."""
    if v is None:
        return None
    if isinstance(v, (dict, list)):
        return v
    try:
        return json.loads(v)
    except (TypeError, ValueError):
        return None


def _parse_bbox_floats(bbox_str: str) -> list[float] | None:
    """Parse 'west,south,east,north' into [w,s,e,n] floats, or None if malformed."""
    parts = bbox_str.split(",")
    if len(parts) != 4:
        return None
    try:
        return [float(parts[0]), float(parts[1]), float(parts[2]), float(parts[3])]
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
) -> tuple[str, List[Any]]:
    """Build a WHERE clause (asyncpg ``$N`` placeholders) + ordered params list.

    Operates on table alias ``cl`` (customer.layer).
    """
    filters = ["cl.in_catalog = TRUE"]
    params: List[Any] = []

    def add(value: Any) -> str:
        params.append(value)
        return f"${len(params)}"

    # Prefix tsquery; \W+ split mirrors the 'simple' tokenizer and blocks
    # tsquery-syntax injection (punctuation-only input → no text filter).
    q_terms = [t for t in re.split(r"\W+", q) if t] if q else []
    if q_terms:
        q_ph = add(" & ".join(f"{t}:*" for t in q_terms))
        filters.append(
            f"""
            to_tsvector('simple',
                coalesce(cl.record_jsonb->'properties'->>'title', '') || ' ' ||
                coalesce(cl.record_jsonb->'properties'->>'description', '') || ' ' ||
                coalesce(
                    (SELECT string_agg(kw, ' ')
                     FROM jsonb_array_elements_text(
                         coalesce(cl.record_jsonb->'properties'->'keywords', '[]'::jsonb)
                     ) AS kw),
                    ''
                )
            ) @@ to_tsquery('simple', {q_ph})
            """
        )

    if themes:
        theme_list = [t.strip() for t in themes.split(",") if t.strip()]
        theme_clauses = []
        for theme_val in theme_list:
            ph = add(json.dumps([{"concepts": [{"id": theme_val}]}]))
            theme_clauses.append(
                f"cl.record_jsonb->'properties'->'themes' @> CAST({ph} AS jsonb)"
            )
        if theme_clauses:
            filters.append("(" + " OR ".join(theme_clauses) + ")")

    if language:
        lang_list = [v.strip() for v in language.split(",") if v.strip()]
        filters.append(
            f"cl.record_jsonb->'properties'->'language'->>'code' = ANY({add(lang_list)})"
        )

    if year:
        filters.append(
            "substring(cl.record_jsonb->'time'->'interval'->0->>0 from 1 for 4)::int = "
            f"{add(year)}"
        )

    if source_format:
        # Provenance lives in the flat column (e.g. 'harvesting_opencatalog');
        # legacy value 'dcat' maps to the harvester.
        sf = "harvesting_opencatalog" if source_format == "dcat" else source_format
        filters.append(f"cl.upload_file_type = {add(sf)}")

    if license_:
        lic_list = [v.strip() for v in license_.split(",") if v.strip()]
        filters.append(
            f"cl.record_jsonb->'properties'->>'license' = ANY({add(lic_list)})"
        )

    if publisher:
        pub_list = [v.strip() for v in publisher.split(",") if v.strip()]
        filters.append(
            f"cl.record_jsonb->'properties'->'contacts'->0->>'name' = ANY({add(pub_list)})"
        )

    if type_:
        type_list = [v.strip() for v in type_.split(",") if v.strip()]
        filters.append(f"cl.type = ANY({add(type_list)})")

    if geographical_code:
        geo_list = [v.strip() for v in geographical_code.split(",") if v.strip()]
        filters.append(
            f"cl.record_jsonb->'properties'->>'goat:geographical_code' = ANY({add(geo_list)})"
        )

    if datetime_:
        # OGC datetime filter on SOURCE dates (record_jsonb.properties.created/updated),
        # falling back to cl.updated_at for layers without record_jsonb.
        _dt_col = (
            "COALESCE("
            "  (cl.record_jsonb->'properties'->>'updated')::timestamptz,"
            "  (cl.record_jsonb->'properties'->>'created')::timestamptz,"
            "  cl.updated_at"
            ")"
        )

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
                    filters.append(f"{_dt_col} >= {add(parsed)}")
            if dt_end_s and dt_end_s != "..":
                parsed = _parse_dt(dt_end_s)
                if parsed:
                    filters.append(f"{_dt_col} <= {add(parsed)}")
        elif len(parts) == 1 and datetime_.strip():
            parsed = _parse_dt(datetime_.strip())
            if parsed:
                filters.append(f"CAST({_dt_col} AS date) = CAST({add(parsed)} AS date)")

    if bbox:
        bbox_floats = _parse_bbox_floats(bbox)
        if bbox_floats:
            w = add(bbox_floats[0])
            s = add(bbox_floats[1])
            e = add(bbox_floats[2])
            n = add(bbox_floats[3])
            # `&&` is a cheap bbox-overlap pre-filter; refine with a real area test so a
            # mere edge sliver is dropped but a region-covering dataset still matches.
            env = f"ST_MakeEnvelope({w}, {s}, {e}, {n}, 4326)"
            filters.append(
                f"cl.extent && {env} "
                f"AND ST_Area(ST_Intersection(cl.extent::geometry, {env})) "
                f">= 0.3 * LEAST(ST_Area(cl.extent::geometry), ST_Area({env}))"
            )

    return " AND ".join(filters), params


def _thumbnail_url(key: Optional[str]) -> Optional[str]:
    """Presign an S3 thumbnail key; pass through full URLs; None otherwise."""
    if not key:
        return None
    if key.startswith(("http://", "https://")):
        return key
    try:
        url = _s3.generate_presigned_get(key, use_public_url=True)
        return str(url) if url else None
    except Exception:
        return None


def _build_record(
    rep: Any,
    distributions: List[Dict[str, Any]],
    base_url: str,
    record_id: str,
    package_record_jsonb: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """Build a grouped OGC Record Feature from an asyncpg representative row.

    Prefers package-level record_jsonb (from customer.layer_group) as the
    dataset metadata source. Falls back to the representative layer's
    record_jsonb, then to flat columns.
    """
    # Defensive stub: only for rows written outside the app (record-first).
    source_jsonb = (
        package_record_jsonb
        or _loads(rep["record_jsonb"])
        or {
            "type": "Feature",
            "geometry": None,
            "properties": {"type": "dataset", "title": rep["name"]},
        }
    )
    record = copy.deepcopy(source_jsonb)
    record["id"] = record_id
    # geometry + goat:* are stored (trigger/harvest); serve only fills gaps.
    props = record.get("properties") or {}

    if rep["thumbnail_url"]:
        props["thumbnail_url"] = _thumbnail_url(rep["thumbnail_url"])
    props.setdefault("goat:layerType", rep["type"])
    props.setdefault("goat:geometryType", rep["feature_layer_geometry_type"])
    props.pop("distributions", None)  # superseded by rel="item" links

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
    # Member layers → rel="item" links; a standalone layer gets no item link.
    for d in distributions:
        if str(d.get("layer_id")) == str(record_id):
            continue
        links.append(
            {
                "rel": "item",
                "type": "application/geo+json",
                "title": d.get("name"),
                "href": f"{base_url}/{d.get('layer_id')}",
                "goat:layerType": d.get("type"),
                "goat:geometryType": d.get("geometry_type"),
            }
        )
    # Keep stored via/enclosure links (the actual DCAT distributions);
    # tolerate legacy records that still carry properties.links.
    stored = record.get("links") or props.pop("links", None) or []
    existing = [lk for lk in stored if lk.get("rel") in ("enclosure", "via")]
    record["links"] = links + existing
    record["properties"] = props
    return record


def _get_group_sql() -> tuple[str, str, str, str]:
    """Return (from_sql, group_expr, group_id_expr, child_name_expr).

    Groups by layer_group_id — layers with a group are grouped together,
    layers without a group are each their own record.
    """
    cs = "customer"
    from_sql = f"FROM {cs}.layer cl"
    group_expr = "COALESCE(cl.layer_group_id::text, cl.id::text)"
    group_id_expr = "cl.layer_group_id::text"
    child_name_expr = "cl.name"
    return from_sql, group_expr, group_id_expr, child_name_expr


async def search_records(
    pool: asyncpg.Pool,
    *,
    base_url: str,
    q: Optional[str] = None,
    bbox: Optional[str] = None,
    bbox_boost: Optional[str] = None,
    themes: Optional[str] = None,
    language: Optional[str] = None,
    year: Optional[int] = None,
    source_format: Optional[str] = None,
    license: Optional[str] = None,
    publisher: Optional[str] = None,
    type: Optional[str] = None,
    geographical_code: Optional[str] = None,
    datetime: Optional[str] = None,
    sortby: Optional[str] = None,
    limit: int = 10,
    offset: int = 0,
) -> Dict[str, Any]:
    """Search catalog datasets, grouped by dataset, as an OGC FeatureCollection dict.

    ``base_url`` is the items endpoint URL used to build pagination/self links.
    """
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
    from_sql, group_expr, group_id_expr, child_name_expr = _get_group_sql()

    async with pool.acquire() as conn:
        # Count total grouped datasets (where-params only)
        number_matched: int = (
            await conn.fetchval(
                f"SELECT COUNT(DISTINCT {group_expr}) {from_sql} WHERE {where_sql}",
                *params,
            )
            or 0
        )

        if number_matched == 0:
            return {
                "type": "FeatureCollection",
                "numberMatched": 0,
                "numberReturned": 0,
                "links": [
                    {"rel": "self", "type": "application/geo+json", "href": base_url}
                ],
                "features": [],
            }

        _SORTBY_MAP = {
            "title": "COALESCE(record_jsonb->'properties'->>'title', name)",
            "updated": "updated_at",
            "created": "created_at",
            "type": "type",
        }
        outer_order_clauses: List[str] = []

        # bbox_boost adds a primary sort clause (params appended after where-params)
        if bbox_boost:
            boost_floats = _parse_bbox_floats(bbox_boost)
            if boost_floats:
                i = len(params)
                params.extend(boost_floats)
                outer_order_clauses.append(
                    "CASE WHEN extent IS NOT NULL"
                    f" AND extent && ST_MakeEnvelope(${i + 1}, ${i + 2}, ${i + 3}, ${i + 4}, 4326)"
                    " THEN 0 ELSE 1 END"
                )

        if sortby:
            for part in sortby.split(","):
                part = part.strip()
                if not part:
                    continue
                if part.startswith("-"):
                    direction, prop = "DESC", part[1:]
                elif part.startswith("+"):
                    direction, prop = "ASC", part[1:]
                else:
                    direction, prop = "ASC", part
                sql_expr = _SORTBY_MAP.get(prop)
                if sql_expr:
                    outer_order_clauses.append(f"{sql_expr} {direction}")

        if not outer_order_clauses:
            outer_order_clauses.append("updated_at DESC")

        needs_cte = bool(bbox_boost or sortby)

        limit_ph = f"${len(params) + 1}"
        params.append(limit)
        offset_ph = f"${len(params) + 1}"
        params.append(offset)

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
                LIMIT {limit_ph} OFFSET {offset_ph}
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
                LIMIT {limit_ph} OFFSET {offset_ph}
            """

        page_groups = await conn.fetch(reps_query, *params)

        if not page_groups:
            return {
                "type": "FeatureCollection",
                "numberMatched": number_matched,
                "numberReturned": 0,
                "links": [
                    {"rel": "self", "type": "application/geo+json", "href": base_url}
                ],
                "features": [],
            }

        rep_ids = [g["representative_id"] for g in page_groups]
        group_keys = [g["group_key"] for g in page_groups]

        rep_rows = await conn.fetch(
            f"SELECT {_REP_COLS} FROM customer.layer WHERE id = ANY($1::uuid[])",
            rep_ids,
        )
        rep_layers = {str(r["id"]): r for r in rep_rows}

        siblings_rows = await conn.fetch(
            f"""
            SELECT cl.id,
                   {child_name_expr} AS name,
                   cl.type,
                   cl.feature_layer_geometry_type,
                   {group_expr} AS group_key
            {from_sql}
            WHERE cl.in_catalog = TRUE
              AND {group_expr} = ANY($1)
            ORDER BY name
            """,
            group_keys,
        )
        distributions_by_group: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for row in siblings_rows:
            distributions_by_group[row["group_key"]].append(
                {
                    "layer_id": str(row["id"]),
                    "name": row["name"],
                    "type": row["type"],
                    "geometry_type": row["feature_layer_geometry_type"],
                }
            )

        # Group-level metadata from customer.layer_group (authoritative)
        group_ids_in_page = [g["group_id"] for g in page_groups if g["group_id"]]
        group_records: Dict[str, Dict[str, Any]] = {}
        if group_ids_in_page:
            try:
                grp_rows = await conn.fetch(
                    "SELECT id::text AS id, record_jsonb FROM customer.layer_group"
                    " WHERE id = ANY($1::uuid[]) AND record_jsonb IS NOT NULL",
                    group_ids_in_page,
                )
                for row in grp_rows:
                    rj = _loads(row["record_jsonb"])
                    if rj:
                        group_records[row["id"]] = rj
            except Exception:
                pass  # fall back to layer metadata

    features: List[Dict[str, Any]] = []
    for group in page_groups:
        rep = rep_layers.get(str(group["representative_id"]))
        if not rep:
            continue
        record_id = group["group_id"] or str(group["representative_id"])
        dists = distributions_by_group.get(
            group["group_key"],
            [
                {
                    "layer_id": str(rep["id"]),
                    "name": rep["name"],
                    "type": rep["type"],
                    "geometry_type": rep["feature_layer_geometry_type"],
                }
            ],
        )
        pkg_rj = group_records.get(group["group_id"]) if group["group_id"] else None
        features.append(
            _build_record(
                rep,
                dists,
                base_url,
                record_id,
                package_record_jsonb=pkg_rj,
            )
        )

    links: List[Dict[str, Any]] = [
        {"rel": "self", "type": "application/geo+json", "href": base_url},
    ]
    if offset + limit < number_matched:
        links.append(
            {
                "rel": "next",
                "type": "application/geo+json",
                "href": f"{base_url}?limit={limit}&offset={offset + limit}",
            }
        )
    if offset > 0:
        links.append(
            {
                "rel": "prev",
                "type": "application/geo+json",
                "href": f"{base_url}?limit={limit}&offset={max(0, offset - limit)}",
            }
        )

    return {
        "type": "FeatureCollection",
        "numberMatched": number_matched,
        "numberReturned": len(features),
        "links": links,
        "features": features,
    }


async def get_record(
    pool: asyncpg.Pool, item_id: str, *, base_url: str
) -> Optional[Dict[str, Any]]:
    """Return a single record. A group id resolves to the group (members as
    rel="item" links); a layer id resolves to that layer's own record."""
    cs = "customer"

    async with pool.acquire() as conn:
        is_group = await conn.fetchval(
            f"SELECT 1 FROM {cs}.layer_group WHERE id::text = $1", item_id
        )
        if is_group:
            rep = await conn.fetchrow(
                f"SELECT {_REP_COLS} FROM {cs}.layer"
                f" WHERE layer_group_id::text = $1 AND in_catalog = TRUE"
                f" ORDER BY updated_at DESC LIMIT 1",
                item_id,
            )
            if rep is None:
                return None
            siblings_rows = await conn.fetch(
                f"SELECT id, name, type, feature_layer_geometry_type FROM {cs}.layer"
                f" WHERE in_catalog = TRUE AND layer_group_id::text = $1 ORDER BY name",
                item_id,
            )
            distributions = [
                {
                    "layer_id": str(row["id"]),
                    "name": row["name"],
                    "type": row["type"],
                    "geometry_type": row["feature_layer_geometry_type"],
                }
                for row in siblings_rows
            ]
            pkg_rj = None
            try:
                pkg_row = await conn.fetchrow(
                    f"SELECT record_jsonb FROM {cs}.layer_group"
                    f" WHERE id = $1::uuid AND record_jsonb IS NOT NULL",
                    item_id,
                )
                if pkg_row and pkg_row["record_jsonb"]:
                    pkg_rj = _loads(pkg_row["record_jsonb"])
            except Exception:
                pass
            return _build_record(
                rep,
                distributions,
                base_url,
                item_id,
                package_record_jsonb=pkg_rj,
            )

        # A layer id (standalone or a group member) → its own record, no members.
        rep = await conn.fetchrow(
            f"SELECT {_REP_COLS} FROM {cs}.layer WHERE id::text = $1 AND in_catalog = TRUE",
            item_id,
        )
        if rep is None:
            return None
        return _build_record(rep, [], base_url, str(rep["id"]))


async def search_nuts(
    pool: asyncpg.Pool, q: Optional[str], level: Optional[int], limit: int
) -> List[Dict[str, Any]]:
    """Search NUTS regions from basic.nuts. Returns matching regions with bbox."""
    filters = []
    params: List[Any] = []

    def add(value: Any) -> str:
        params.append(value)
        return f"${len(params)}"

    if q and q.strip():
        ph = add(f"{q.strip()}%")
        filters.append(f"(nuts_name ILIKE {ph} OR nuts_id ILIKE {ph})")

    if level is not None:
        filters.append(f"levl_code = {add(level)}")

    where = ("WHERE " + " AND ".join(filters)) if filters else ""
    limit_ph = add(limit)

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT nuts_id, nuts_name, levl_code AS level, cntr_code AS country,
                   ST_XMin(geom) AS west, ST_YMin(geom) AS south,
                   ST_XMax(geom) AS east, ST_YMax(geom) AS north
            FROM basic.nuts
            {where}
            ORDER BY levl_code, nuts_name
            LIMIT {limit_ph}
            """,
            *params,
        )

    return [
        {
            "nuts_id": row["nuts_id"],
            "nuts_name": row["nuts_name"],
            "level": row["level"],
            "country": row["country"],
            "bbox": [
                round(row["west"], 6),
                round(row["south"], 6),
                round(row["east"], 6),
                round(row["north"], 6),
            ],
        }
        for row in rows
    ]


async def get_nuts_geometry(
    pool: asyncpg.Pool, nuts_id: str
) -> Optional[Dict[str, Any]]:
    """Return the boundary of a NUTS region as a GeoJSON Feature, or None if absent."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT nuts_id, nuts_name, levl_code AS level,
                   ST_AsGeoJSON(ST_Transform(geom, 4326)) AS geometry
            FROM basic.nuts
            WHERE nuts_id = $1
            LIMIT 1
            """,
            nuts_id,
        )
    if not row:
        return None

    return {
        "type": "Feature",
        "properties": {
            "nuts_id": row["nuts_id"],
            "nuts_name": row["nuts_name"],
            "level": row["level"],
        },
        "geometry": _loads(row["geometry"]),
    }
