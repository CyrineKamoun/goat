from __future__ import annotations

import math
import os
from collections import Counter
from datetime import datetime
from decimal import Decimal
from typing import Any

import duckdb
from fastapi import HTTPException, status
from psycopg import connect
from psycopg.rows import dict_row

from core.core.config import settings

_CATALOG_FILTER_KEYS = (
    "type",
    "data_category",
    "distributor_name",
    "geographical_code",
    "language_code",
    "license",
)
_CATALOG_ORDERABLE_COLUMNS = {
    "updated_at",
    "created_at",
    "name",
    "distributor_name",
}
_DEFAULT_WKT_EXTENT = "POLYGON((-180 -90, 180 -90, 180 90, -180 90, -180 -90))"
_DUCKLAKE_CATALOG_SCHEMA = os.getenv("DUCKLAKE_CATALOG_SCHEMA", "ducklake")
_DUCKLAKE_DATA_DIR = os.getenv("DUCKLAKE_DATA_DIR", f"{settings.DATA_DIR}/ducklake")


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str) or not value.strip():
        return None
    candidate = value.strip().replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(candidate)
    except ValueError:
        return None


def _normalize_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _extract_distributor_name(csw_record: dict[str, Any]) -> str | None:
    contacts = csw_record.get("contacts")
    if not isinstance(contacts, list):
        return None

    for contact in contacts:
        if not isinstance(contact, dict):
            continue
        organization = _normalize_text(contact.get("organization"))
        if organization:
            return organization

    return None


def _extract_resource_type(csw_record: dict[str, Any]) -> str | None:
    return _normalize_text(csw_record.get("resource_type")) or _normalize_text(
        csw_record.get("type")
    )


def _extract_crs(csw_record: dict[str, Any]) -> str | None:
    direct = _normalize_text(csw_record.get("crs"))
    if direct:
        return direct

    spatial_reference = csw_record.get("spatial_reference")
    if isinstance(spatial_reference, dict):
        code = _normalize_text(spatial_reference.get("code"))
        if code:
            return code

    bbox = csw_record.get("bbox")
    if isinstance(bbox, dict):
        return _normalize_text(bbox.get("crs"))

    return None


def _default_feature_properties(geometry_type: str | None) -> dict[str, Any]:
    base: dict[str, Any] = {
        "visibility": True,
        "filled": True,
        "stroked": True,
        "opacity": 0.8,
        "stroke_width": 1,
        "color": [90, 132, 232],
        "stroke_color": [45, 75, 160],
    }

    if geometry_type == "point":
        base["radius"] = 5

    return base


def _item_matches_filters(
    item: dict[str, Any], params: Any, *, skip_key: str | None = None
) -> bool:
    for key in _CATALOG_FILTER_KEYS:
        if key == skip_key:
            continue
        values = getattr(params, key, None)
        if not values:
            continue

        item_value = _normalize_text(item.get(key))
        if not item_value or item_value not in set(values):
            return False

    if getattr(params, "search", None) and skip_key != "search":
        search = str(params.search).lower().strip()
        keywords = item.get("other_properties", {}).get("csw", {}).get("keywords", [])
        keyword_text = " ".join(
            [str(keyword).strip() for keyword in keywords if str(keyword).strip()]
        )
        haystack = " ".join(
            [
                _normalize_text(item.get("name")) or "",
                _normalize_text(item.get("description")) or "",
                _normalize_text(item.get("distributor_name")) or "",
                keyword_text,
            ]
        ).lower()
        if search not in haystack:
            return False

    return True


def _sort_catalog_items(
    items: list[dict[str, Any]], order_by: str, order: str
) -> list[dict[str, Any]]:
    reverse = order != "ascendent"

    def _sort_key(item: dict[str, Any]) -> Any:
        value = item.get(order_by)
        if order_by in {"updated_at", "created_at"}:
            return _parse_datetime(value) or datetime.min
        if isinstance(value, str):
            return value.lower()
        return value or ""

    return sorted(items, key=_sort_key, reverse=reverse)


def _table_exists(cur: Any, table_name: str) -> bool:
    cur.execute("SELECT to_regclass(%s) IS NOT NULL AS exists", (table_name,))
    row = cur.fetchone()
    return bool(row["exists"]) if row else False


def _column_exists(cur: Any, schema: str, table_name: str, column_name: str) -> bool:
    cur.execute(
        """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = %s
              AND column_name = %s
        ) AS exists
        """,
        (schema, table_name, column_name),
    )
    row = cur.fetchone()
    return bool(row["exists"]) if row else False


def _build_catalog_items(params: Any) -> list[dict[str, Any]]:
    layer_table = f"{settings.CATALOG_SCHEMA}.layer"
    version_table = f"{settings.CATALOG_SCHEMA}.processor_dataset_version"
    fallback_ts = "1970-01-01T00:00:00+00:00"

    with connect(
        host=settings.POSTGRES_SERVER,
        port=settings.POSTGRES_PORT,
        dbname=settings.POSTGRES_DB,
        user=settings.POSTGRES_USER,
        password=settings.POSTGRES_PASSWORD,
        row_factory=dict_row,
    ) as conn:
        with conn.cursor() as cur:
            if not _table_exists(cur, layer_table):
                return []

            version_join_sql = ""
            if _table_exists(cur, version_table):
                version_join_sql = f"""
                LEFT JOIN LATERAL (
                    SELECT
                        id,
                        package_id,
                        signature,
                        version_num,
                        status,
                        run_id,
                        processed_at
                    FROM {version_table} v
                    WHERE v.resource_id = l.resource_id
                      AND v.status <> 'failed'
                    ORDER BY v.version_num DESC
                    LIMIT 1
                ) AS v ON TRUE
                """

            cur.execute(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_schema = %s
                      AND table_name = 'layer'
                      AND column_name = 'csw_record_jsonb'
                ) AS has_csw
                """,
                (settings.CATALOG_SCHEMA,),
            )
            has_csw_row = cur.fetchone() or {}
            csw_select_sql = (
                "l.csw_record_jsonb AS csw_record_jsonb"
                if bool(has_csw_row.get("has_csw"))
                else "NULL::jsonb AS csw_record_jsonb"
            )
            has_is_latest = _column_exists(cur, settings.CATALOG_SCHEMA, "layer", "is_latest")
            has_layer_version_num = _column_exists(
                cur, settings.CATALOG_SCHEMA, "layer", "version_num"
            )
            has_base_resource_id = _column_exists(
                cur, settings.CATALOG_SCHEMA, "layer", "base_resource_id"
            )
            latest_only_sql = "WHERE COALESCE(l.is_latest, TRUE)" if has_is_latest else ""
            layer_version_select_sql = (
                "l.version_num AS layer_version_num"
                if has_layer_version_num
                else "NULL::integer AS layer_version_num"
            )
            layer_is_latest_select_sql = (
                "l.is_latest AS layer_is_latest" if has_is_latest else "TRUE AS layer_is_latest"
            )
            base_resource_select_sql = (
                "l.base_resource_id AS base_resource_id"
                if has_base_resource_id
                else "l.resource_id AS base_resource_id"
            )

            cur.execute(
                f"""
                SELECT
                    l.id::text AS layer_id,
                    l.resource_id,
                    {base_resource_select_sql},
                    {layer_version_select_sql},
                    {layer_is_latest_select_sql},
                    l.name,
                    l.feature_layer_geometry_type,
                    CASE WHEN l.extent IS NOT NULL THEN ST_AsText(l.extent) END AS extent,
                    {csw_select_sql},
                    l.schema_name,
                    l.table_name,
                    v.package_id,
                    v.id AS version_id,
                    v.signature,
                    v.version_num,
                    v.status,
                    v.run_id,
                    v.processed_at
                FROM {layer_table} l
                {version_join_sql}
                {latest_only_sql}
                ORDER BY l.name ASC
                """
            )
            rows = cur.fetchall()

    items: list[dict[str, Any]] = []
    for row in rows:
        layer_id = _normalize_text(row.get("layer_id")) or ""
        resource_id = _normalize_text(row.get("resource_id")) or ""
        csw_record = (
            row.get("csw_record_jsonb") if isinstance(row.get("csw_record_jsonb"), dict) else {}
        )
        geometry_type = _normalize_text(row.get("feature_layer_geometry_type"))
        processed_at = _parse_datetime(row.get("processed_at"))
        updated_at = processed_at.isoformat() if processed_at else fallback_ts
        created_at = updated_at

        has_materialized_output = bool(
            _normalize_text(row.get("schema_name")) and _normalize_text(row.get("table_name"))
        )
        raw_processor_status = _normalize_text(row.get("status"))
        goat_status = "ready" if has_materialized_output else "pending"
        goat_reason = None if goat_status == "ready" else (raw_processor_status or "not_materialized")

        title = _normalize_text(csw_record.get("title")) or _normalize_text(row.get("name"))
        abstract = _normalize_text(csw_record.get("abstract"))
        language_code = _normalize_text(csw_record.get("language"))
        geographical_code = _normalize_text(csw_record.get("geographical_code"))
        license_value = _normalize_text(csw_record.get("license"))
        attribution = _normalize_text(csw_record.get("attribution"))
        keywords = csw_record.get("keywords") if isinstance(csw_record.get("keywords"), list) else []
        distributor_name = _extract_distributor_name(csw_record)
        resource_type = _extract_resource_type(csw_record)
        csw_updated_at = _normalize_text(csw_record.get("updated_at"))
        csw_bbox = csw_record.get("bbox") if isinstance(csw_record.get("bbox"), dict) else None
        csw_crs = _extract_crs(csw_record)

        item = {
            "id": layer_id,
            "name": title or resource_id or layer_id or "Unnamed catalog layer",
            "description": abstract,
            "thumbnail_url": None,
            "type": resource_type or "feature",
            "data_category": _normalize_text(csw_record.get("topic_category")),
            "distributor_name": distributor_name,
            "geographical_code": geographical_code,
            "language_code": language_code,
            "license": license_value,
            "attribution": attribution,
            "folder_id": "00000000-0000-0000-0000-000000000000",
            "user_id": "00000000-0000-0000-0000-000000000000",
            "properties": _default_feature_properties(geometry_type),
            "extent": _normalize_text(row.get("extent")) or _DEFAULT_WKT_EXTENT,
            "other_properties": {
                "source": "goat_catalog",
                "resource_id": resource_id,
                "csw": {
                    "identifier": _normalize_text(csw_record.get("identifier")) or resource_id,
                    "title": title,
                    "abstract": abstract,
                    "keywords": keywords,
                    "resource_type": resource_type,
                    "language": language_code,
                    "geographical_code": geographical_code,
                    "license": license_value,
                    "topic_category": _normalize_text(csw_record.get("topic_category")),
                    "attribution": attribution,
                    "bbox": csw_bbox,
                    "crs": csw_crs,
                    "contacts": csw_record.get("contacts")
                    if isinstance(csw_record.get("contacts"), list)
                    else [],
                    "updated_at": csw_updated_at,
                },
            },
            "url": None,
            "feature_layer_type": "standard",
            "feature_layer_geometry_type": geometry_type,
            "data_type": None,
            "goat_profile": {
                "source_package_id": _normalize_text(row.get("package_id")),
                "source_resource_id": resource_id,
                "title": title,
                "description": abstract,
                "type": resource_type or "feature",
                "data_category": _normalize_text(csw_record.get("topic_category")),
                "license": license_value,
                "language_code": language_code,
                "geographical_code": geographical_code,
                "distributor_name": distributor_name,
                "goat_status": goat_status,
                "goat_reason": goat_reason,
                "source_kind": "duckdb_table" if has_materialized_output else "unknown",
                "source_dataset_id": layer_id,
                "source_location": {
                    "duckdb_schema": _normalize_text(row.get("schema_name")),
                    "duckdb_table": _normalize_text(row.get("table_name")),
                },
                "source_version": _normalize_text(row.get("signature")),
                "source_version_id": _normalize_text(row.get("version_id")),
                "source_version_num": (
                    int(row.get("layer_version_num"))
                    if row.get("layer_version_num") is not None
                    else (
                        int(row.get("version_num"))
                        if row.get("version_num") is not None
                        else None
                    )
                ),
                "processor_run_id": _normalize_text(row.get("run_id")),
                "derived_from": {
                    "resource_id": _normalize_text(row.get("base_resource_id")) or resource_id,
                },
                "is_latest": bool(row.get("layer_is_latest")),
                "csw_record": csw_record,
            },
            "in_catalog": True,
            "created_at": created_at,
            "updated_at": updated_at,
        }
        if _item_matches_filters(item, params):
            items.append(item)

    return items


def _get_catalog_item_by_id(dataset_id: str) -> dict[str, Any]:
    items = _build_catalog_items(type("CatalogFilter", (), {})())
    for item in items:
        if str(item.get("id") or "") == dataset_id:
            return item
        goat_profile = item.get("goat_profile") or {}
        if str(goat_profile.get("source_resource_id") or "") == dataset_id:
            return item
    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail="Catalog dataset not found",
    )


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _json_safe(val) for key, val in value.items()}
    return str(value)


def _quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


_DUCKLAKE_KEEPALIVE_PARAMS = {
    "keepalives": "1",
    "keepalives_idle": "30",
    "keepalives_interval": "5",
    "keepalives_count": "5",
}


def _ducklake_connect() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect()
    for ext in ("spatial", "postgres", "ducklake"):
        try:
            conn.execute(f"INSTALL {ext};")
        except Exception:
            pass
        conn.execute(f"LOAD {ext};")

    params = {
        "host": settings.POSTGRES_SERVER,
        "port": str(settings.POSTGRES_PORT),
        "dbname": settings.POSTGRES_DB,
        "user": settings.POSTGRES_USER,
        "password": settings.POSTGRES_PASSWORD,
    }
    params.update(_DUCKLAKE_KEEPALIVE_PARAMS)
    libpq_str = " ".join(f"{k}={v}" for k, v in params.items())

    conn.execute(
        f"ATTACH 'ducklake:postgres:{libpq_str}' AS lake ("
        f"DATA_PATH '{_DUCKLAKE_DATA_DIR}', "
        f"METADATA_SCHEMA '{_DUCKLAKE_CATALOG_SCHEMA}', "
        f"OVERRIDE_DATA_PATH TRUE, "
        f"READ_ONLY"
        f")"
    )
    return conn


def _read_duckdb_sample(*, duckdb_schema: str, duckdb_table: str, limit: int) -> dict[str, Any]:
    conn = _ducklake_connect()
    full_table = f'lake."{duckdb_schema}"."{duckdb_table}"'
    try:
        describe_rows = conn.execute(f"DESCRIBE SELECT * FROM {full_table}").fetchall()
        columns = [
            {
                "name": str(row[0]),
                "type": str(row[1]),
                "nullable": True if len(row) < 3 else str(row[2]).upper() != "NO",
            }
            for row in describe_rows
        ]

        select_exprs = []
        for column in columns:
            col_name = str(column["name"])
            quoted_col = _quote_identifier(col_name)
            col_type = str(column["type"]).upper()
            if "GEOMETRY" in col_type:
                select_exprs.append(f"ST_AsText({quoted_col}) AS {quoted_col}")
            else:
                select_exprs.append(quoted_col)

        select_sql = f"SELECT {', '.join(select_exprs)} FROM {full_table} LIMIT ?"
        data_rows = conn.execute(select_sql, [limit]).fetchall()
        col_names = [col["name"] for col in columns]
        rows = [
            {col_names[index]: _json_safe(value) for index, value in enumerate(row)}
            for row in data_rows
        ]

        total = int(conn.execute(f"SELECT COUNT(*) FROM {full_table}").fetchone()[0])
        return {
            "columns": columns,
            "rows": rows,
            "row_count": total,
            "returned": len(rows),
        }
    finally:
        conn.close()


def _infer_duckdb_geometry_type(*, duckdb_schema: str, duckdb_table: str) -> str | None:
    conn = _ducklake_connect()
    full_table = f'lake."{duckdb_schema}"."{duckdb_table}"'
    try:
        describe_rows = conn.execute(f"DESCRIBE SELECT * FROM {full_table}").fetchall()
        geometry_columns = [
            str(row[0])
            for row in describe_rows
            if len(row) > 1 and "GEOMETRY" in str(row[1]).upper()
        ]
        if not geometry_columns:
            return None

        geom_col = _quote_identifier(geometry_columns[0])
        row = conn.execute(
            f"SELECT ST_GeometryType({geom_col}) FROM {full_table} WHERE {geom_col} IS NOT NULL LIMIT 1"
        ).fetchone()
        if not row or not row[0]:
            return None

        geom_type = str(row[0]).upper().replace("ST_", "")
        if "POINT" in geom_type:
            return "point"
        if "LINE" in geom_type:
            return "line"
        if "POLYGON" in geom_type:
            return "polygon"
        return None
    finally:
        conn.close()


def _infer_duckdb_extent(*, duckdb_schema: str, duckdb_table: str) -> str | None:
    conn = _ducklake_connect()
    full_table = f'lake."{duckdb_schema}"."{duckdb_table}"'
    try:
        describe_rows = conn.execute(f"DESCRIBE SELECT * FROM {full_table}").fetchall()
        geometry_columns = [
            str(row[0])
            for row in describe_rows
            if len(row) > 1 and "GEOMETRY" in str(row[1]).upper()
        ]
        if not geometry_columns:
            return None

        geom_col = _quote_identifier(geometry_columns[0])
        row = conn.execute(
            (
                f"SELECT "
                f"ST_XMin(ST_Extent_Agg({geom_col})), "
                f"ST_YMin(ST_Extent_Agg({geom_col})), "
                f"ST_XMax(ST_Extent_Agg({geom_col})), "
                f"ST_YMax(ST_Extent_Agg({geom_col})) "
                f"FROM {full_table}"
            )
        ).fetchone()
        if not row or any(value is None for value in row):
            return None

        min_x, min_y, max_x, max_y = row
        return (
            f"POLYGON(({min_x} {min_y}, {max_x} {min_y}, "
            f"{max_x} {max_y}, {min_x} {max_y}, {min_x} {min_y}))"
        )
    finally:
        conn.close()


def _enrich_item_with_duckdb_geometry(item: dict[str, Any]) -> dict[str, Any]:
    if item.get("type") != "feature":
        return item

    goat_profile = item.get("goat_profile") or {}
    source_location = goat_profile.get("source_location") or {}
    duckdb_schema = source_location.get("duckdb_schema")
    duckdb_table = source_location.get("duckdb_table")

    if not duckdb_schema or not duckdb_table:
        return item

    try:
        inferred = _infer_duckdb_geometry_type(
            duckdb_schema=str(duckdb_schema),
            duckdb_table=str(duckdb_table),
        )
    except Exception:
        inferred = None

    try:
        inferred_extent = _infer_duckdb_extent(
            duckdb_schema=str(duckdb_schema),
            duckdb_table=str(duckdb_table),
        )
    except Exception:
        inferred_extent = None

    if not inferred and not inferred_extent:
        return item

    enriched = dict(item)
    if inferred:
        enriched["feature_layer_geometry_type"] = inferred
    if inferred_extent:
        enriched["extent"] = inferred_extent
    return enriched


def _build_use_data_payload(item: dict[str, Any]) -> dict[str, Any]:
    goat_profile = item.get("goat_profile") or {}
    source_location = goat_profile.get("source_location") or {}

    duckdb_schema = _normalize_text(source_location.get("duckdb_schema"))
    duckdb_table = _normalize_text(source_location.get("duckdb_table"))
    has_canonical_pointer = bool(duckdb_schema and duckdb_table)

    status = "ready" if has_canonical_pointer else "import_required"
    reason = (
        None
        if has_canonical_pointer
        else (_normalize_text(goat_profile.get("goat_reason")) or "not_materialized")
    )

    return {
        "mode": "pointer_only",
        "copy_policy": "forbidden",
        "status": status,
        "reason": reason,
        "requires_import": not has_canonical_pointer,
        "usable_layer_id": None,
        "canonical_dataset_id": _normalize_text(goat_profile.get("source_dataset_id"))
        or _normalize_text(item.get("id")),
        "canonical_pointer": {
            "kind": _normalize_text(goat_profile.get("source_kind")) or "duckdb_table",
            "duckdb_schema": duckdb_schema,
            "duckdb_table": duckdb_table,
        },
        "processor_version": {
            "version_id": _normalize_text(goat_profile.get("source_version_id")),
            "version_num": goat_profile.get("source_version_num"),
            "signature": _normalize_text(goat_profile.get("source_version")),
            "run_id": _normalize_text(goat_profile.get("processor_run_id")),
        },
    }


def _read_dataset_versions(dataset_id: str) -> dict[str, Any]:
    item = _get_catalog_item_by_id(dataset_id)
    goat_profile = item.get("goat_profile") or {}
    resource_id = _normalize_text(goat_profile.get("source_resource_id"))
    layer_id = _normalize_text(item.get("id"))

    if not resource_id:
        return {
            "dataset_id": dataset_id,
            "layer_id": layer_id,
            "resource_id": None,
            "latest_version": None,
            "versions": [],
        }

    version_table = f"{settings.CATALOG_SCHEMA}.processor_dataset_version"
    layer_table = f"{settings.CATALOG_SCHEMA}.layer"
    versions: list[dict[str, Any]] = []
    latest: dict[str, Any] | None = None

    with connect(
        host=settings.POSTGRES_SERVER,
        port=settings.POSTGRES_PORT,
        dbname=settings.POSTGRES_DB,
        user=settings.POSTGRES_USER,
        password=settings.POSTGRES_PASSWORD,
        row_factory=dict_row,
    ) as conn:
        with conn.cursor() as cur:
            if not _table_exists(cur, version_table):
                return {
                    "dataset_id": dataset_id,
                    "layer_id": layer_id,
                    "resource_id": resource_id,
                    "latest_version": None,
                    "versions": [],
                }

            has_layer_table = _table_exists(cur, layer_table)
            has_layer_version_num = has_layer_table and _column_exists(
                cur, settings.CATALOG_SCHEMA, "layer", "version_num"
            )
            has_layer_is_latest = has_layer_table and _column_exists(
                cur, settings.CATALOG_SCHEMA, "layer", "is_latest"
            )
            has_layer_base_resource = has_layer_table and _column_exists(
                cur, settings.CATALOG_SCHEMA, "layer", "base_resource_id"
            )

            if has_layer_version_num:
                is_latest_select_sql = (
                    "l.is_latest AS layer_is_latest"
                    if has_layer_is_latest
                    else "NULL::boolean AS layer_is_latest"
                )
                base_resource_select_sql = (
                    "l.base_resource_id AS layer_base_resource_id"
                    if has_layer_base_resource
                    else "NULL::text AS layer_base_resource_id"
                )
                cur.execute(
                    f"""
                    SELECT
                        v.id,
                        v.package_id,
                        v.resource_id,
                        v.signature,
                        v.version_num,
                        v.row_count,
                        v.status,
                        v.error,
                        v.run_id,
                        v.processed_at,
                        l.id::text AS layer_id,
                        {is_latest_select_sql},
                        {base_resource_select_sql}
                    FROM {version_table} v
                    LEFT JOIN {layer_table} l
                      ON l.resource_id = v.resource_id
                     AND l.version_num = v.version_num
                    WHERE v.resource_id = %s
                    ORDER BY v.version_num DESC
                    """,
                    (resource_id,),
                )
            else:
                cur.execute(
                    f"""
                    SELECT
                        id,
                        package_id,
                        resource_id,
                        signature,
                        version_num,
                        row_count,
                        status,
                        error,
                        run_id,
                        processed_at,
                        NULL::text AS layer_id,
                        NULL::boolean AS layer_is_latest,
                        NULL::text AS layer_base_resource_id
                    FROM {version_table}
                    WHERE resource_id = %s
                    ORDER BY version_num DESC
                    """,
                    (resource_id,),
                )
            rows = cur.fetchall()

    for row in rows:
        entry = {
            "id": _normalize_text(row.get("id")),
            "package_id": _normalize_text(row.get("package_id")),
            "resource_id": _normalize_text(row.get("resource_id")),
            "signature": _normalize_text(row.get("signature")),
            "version_num": int(row.get("version_num")) if row.get("version_num") is not None else None,
            "row_count": int(row.get("row_count")) if row.get("row_count") is not None else None,
            "status": _normalize_text(row.get("status")),
            "error": _normalize_text(row.get("error")),
            "run_id": _normalize_text(row.get("run_id")),
            "processed_at": _json_safe(row.get("processed_at")),
            "catalog_layer_id": _normalize_text(row.get("layer_id")),
            "is_latest": bool(row.get("layer_is_latest")) if row.get("layer_is_latest") is not None else None,
            "base_resource_id": _normalize_text(row.get("layer_base_resource_id")),
        }
        versions.append(entry)

    latest = versions[0] if versions else None

    return {
        "dataset_id": dataset_id,
        "layer_id": layer_id,
        "resource_id": resource_id,
        "latest_version": latest,
        "versions": versions,
    }


def _get_catalog_layer_id(resource_id: str) -> str | None:
    try:
        with connect(
            host=settings.POSTGRES_SERVER,
            port=settings.POSTGRES_PORT,
            dbname=settings.POSTGRES_DB,
            user=settings.POSTGRES_USER,
            password=settings.POSTGRES_PASSWORD,
            row_factory=dict_row,
        ) as conn:
            with conn.cursor() as cur:
                has_is_latest = _column_exists(cur, settings.CATALOG_SCHEMA, "layer", "is_latest")
                has_version_num = _column_exists(cur, settings.CATALOG_SCHEMA, "layer", "version_num")
                order_parts = []
                if has_is_latest:
                    order_parts.append("COALESCE(is_latest, FALSE) DESC")
                if has_version_num:
                    order_parts.append("version_num DESC NULLS LAST")
                order_by_sql = f"ORDER BY {', '.join(order_parts)}" if order_parts else ""
                cur.execute(
                    f"""
                    SELECT id
                    FROM {settings.CATALOG_SCHEMA}.layer
                    WHERE resource_id = %s
                    {order_by_sql}
                    LIMIT 1
                    """,
                    (resource_id,),
                )
                row = cur.fetchone()
                if row:
                    return str(row["id"])
    except Exception:
        pass
    return None


def lookup_catalog_layer_pointer(layer_id: str) -> dict[str, str] | None:
    try:
        with connect(
            host=settings.POSTGRES_SERVER,
            port=settings.POSTGRES_PORT,
            dbname=settings.POSTGRES_DB,
            user=settings.POSTGRES_USER,
            password=settings.POSTGRES_PASSWORD,
            row_factory=dict_row,
        ) as conn:
            with conn.cursor() as cur:
                has_is_latest = _column_exists(cur, settings.CATALOG_SCHEMA, "layer", "is_latest")
                has_version_num = _column_exists(cur, settings.CATALOG_SCHEMA, "layer", "version_num")
                order_parts = ["CASE WHEN id::text = %s THEN 0 ELSE 1 END"]
                if has_is_latest:
                    order_parts.append("COALESCE(is_latest, FALSE) DESC")
                if has_version_num:
                    order_parts.append("version_num DESC NULLS LAST")
                cur.execute(
                    f"""
                    SELECT id::text AS id, resource_id, schema_name, table_name
                    FROM {settings.CATALOG_SCHEMA}.layer
                    WHERE id::text = %s OR resource_id = %s
                    ORDER BY {', '.join(order_parts)}
                    LIMIT 1
                    """,
                    (layer_id, layer_id, layer_id),
                )
                row = cur.fetchone()
                if not row:
                    return None
                return {
                    "id": str(row["id"]),
                    "resource_id": str(row["resource_id"]),
                    "schema_name": str(row["schema_name"]),
                    "table_name": str(row["table_name"]),
                }
    except Exception:
        return None


def lookup_catalog_layer_metadata(layer_id: str) -> dict[str, Any] | None:
    try:
        with connect(
            host=settings.POSTGRES_SERVER,
            port=settings.POSTGRES_PORT,
            dbname=settings.POSTGRES_DB,
            user=settings.POSTGRES_USER,
            password=settings.POSTGRES_PASSWORD,
            row_factory=dict_row,
        ) as conn:
            with conn.cursor() as cur:
                has_is_latest = _column_exists(cur, settings.CATALOG_SCHEMA, "layer", "is_latest")
                has_version_num = _column_exists(cur, settings.CATALOG_SCHEMA, "layer", "version_num")
                order_parts = ["CASE WHEN l.id::text = %s THEN 0 ELSE 1 END"]
                if has_is_latest:
                    order_parts.append("COALESCE(l.is_latest, FALSE) DESC")
                if has_version_num:
                    order_parts.append("l.version_num DESC NULLS LAST")
                cur.execute(
                    f"""
                    SELECT
                        l.id::text AS id,
                        l.resource_id,
                        l.name,
                        l.feature_layer_geometry_type,
                        l.schema_name,
                        l.table_name,
                        ST_XMin(e.e) AS xmin,
                        ST_YMin(e.e) AS ymin,
                        ST_XMax(e.e) AS xmax,
                        ST_YMax(e.e) AS ymax
                    FROM {settings.CATALOG_SCHEMA}.layer l
                    LEFT JOIN LATERAL ST_Envelope(l.extent) e ON TRUE
                    WHERE l.id::text = %s OR l.resource_id = %s
                    ORDER BY {', '.join(order_parts)}
                    LIMIT 1
                    """,
                    (layer_id, layer_id, layer_id),
                )
                row = cur.fetchone()
                if not row:
                    return None
                return {
                    "id": str(row["id"]),
                    "resource_id": str(row["resource_id"]),
                    "name": str(row["name"]),
                    "feature_layer_geometry_type": row["feature_layer_geometry_type"],
                    "schema_name": str(row["schema_name"]),
                    "table_name": str(row["table_name"]),
                    "bounds": [
                        float(row["xmin"]) if row["xmin"] is not None else -180.0,
                        float(row["ymin"]) if row["ymin"] is not None else -90.0,
                        float(row["xmax"]) if row["xmax"] is not None else 180.0,
                        float(row["ymax"]) if row["ymax"] is not None else 90.0,
                    ],
                }
    except Exception:
        return None


def get_catalog_layers_page(
    *,
    page: int,
    size: int,
    order_by: str | None,
    order: str,
    params: Any,
) -> dict[str, Any]:
    effective_order_by = order_by if order_by in _CATALOG_ORDERABLE_COLUMNS else "updated_at"
    all_items = _sort_catalog_items(_build_catalog_items(params), effective_order_by, order)
    total = len(all_items)
    start_index = (page - 1) * size
    end_index = start_index + size
    mapped_rows = all_items[start_index:end_index]
    pages = math.ceil(total / size) if size else 0
    return {"items": mapped_rows, "total": total, "page": page, "size": size, "pages": pages}


def get_catalog_metadata_aggregate(params: Any) -> dict[str, list[dict[str, Any]]]:
    all_items = _build_catalog_items(type("CatalogFilter", (), {})())
    output: dict[str, list[dict[str, Any]]] = {}

    for key in _CATALOG_FILTER_KEYS:
        values = [
            _normalize_text(item.get(key))
            for item in all_items
            if _item_matches_filters(item, params, skip_key=key)
        ]
        counts = Counter([value for value in values if value])
        output[key] = [
            {"value": value, "count": count}
            for value, count in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))
        ]

    return output


def get_catalog_dataset_detail(dataset_id: str) -> dict[str, Any]:
    item = _enrich_item_with_duckdb_geometry(_get_catalog_item_by_id(dataset_id))
    csw_record = ((item.get("goat_profile") or {}).get("csw_record") or {})
    use_data = _build_use_data_payload(item)
    return {
        "dataset": item,
        "use_data": use_data,
        "metadata": {
            "summary": {
                "title": item.get("name"),
                "description": item.get("description"),
                "license": item.get("license"),
                "distributor_name": item.get("distributor_name"),
                "language_code": item.get("language_code"),
                "geographical_code": item.get("geographical_code"),
                "updated_at": item.get("updated_at"),
            },
            "technical": {
                "type": item.get("type"),
                "data_type": item.get("data_type"),
                "feature_layer_type": item.get("feature_layer_type"),
                "feature_layer_geometry_type": item.get("feature_layer_geometry_type"),
                "url": item.get("url"),
            },
            "csw": {
                "identifier": csw_record.get("identifier"),
                "title": csw_record.get("title"),
                "abstract": csw_record.get("abstract"),
                "keywords": csw_record.get("keywords")
                if isinstance(csw_record.get("keywords"), list)
                else [],
                "resource_type": _extract_resource_type(csw_record),
                "contacts": csw_record.get("contacts")
                if isinstance(csw_record.get("contacts"), list)
                else [],
                "language": csw_record.get("language"),
                "geographical_code": csw_record.get("geographical_code"),
                "license": csw_record.get("license"),
                "attribution": csw_record.get("attribution"),
                "topic_category": csw_record.get("topic_category"),
                "distribution_url": csw_record.get("distribution_url"),
                "bbox": csw_record.get("bbox") if isinstance(csw_record.get("bbox"), dict) else None,
                "crs": _extract_crs(csw_record),
                "updated_at": csw_record.get("updated_at"),
                "source": csw_record.get("source") if isinstance(csw_record.get("source"), dict) else {},
            },
            "use_data": use_data,
        },
    }


def get_catalog_dataset_versions(dataset_id: str) -> dict[str, Any]:
    return _read_dataset_versions(dataset_id)


def get_catalog_dataset_sample(dataset_id: str, *, limit: int) -> dict[str, Any]:
    item = _get_catalog_item_by_id(dataset_id)
    goat_profile = item.get("goat_profile") or {}
    source_location = goat_profile.get("source_location") or {}
    duckdb_schema = source_location.get("duckdb_schema")
    duckdb_table = source_location.get("duckdb_table")

    if not duckdb_schema or not duckdb_table:
        return {
            "dataset_id": dataset_id,
            "status": goat_profile.get("goat_status") or "pending",
            "columns": [],
            "rows": [],
            "row_count": 0,
            "returned": 0,
            "message": "Sample unavailable until processing is ready",
        }

    sample = _read_duckdb_sample(
        duckdb_schema=str(duckdb_schema),
        duckdb_table=str(duckdb_table),
        limit=limit,
    )

    non_geometry_columns = [
        column
        for column in sample.get("columns", [])
        if "GEOMETRY" not in str(column.get("type") or "").upper()
        and "BBOX" not in str(column.get("name") or "").upper()
    ]
    non_geometry_column_names = {str(column.get("name")) for column in non_geometry_columns}
    sample["columns"] = non_geometry_columns
    sample["rows"] = [
        {key: value for key, value in row.items() if key in non_geometry_column_names}
        for row in sample.get("rows", [])
        if isinstance(row, dict)
    ]

    return {
        "dataset_id": dataset_id,
        "status": goat_profile.get("goat_status") or "ready",
        **sample,
    }


def get_catalog_dataset_map_preview(dataset_id: str) -> dict[str, Any]:
    item = _enrich_item_with_duckdb_geometry(_get_catalog_item_by_id(dataset_id))
    goat_profile = item.get("goat_profile") or {}
    source_resource_id = goat_profile.get("source_resource_id") or item.get("id")

    catalog_layer_id = _get_catalog_layer_id(str(source_resource_id))
    collection_id = catalog_layer_id or source_resource_id

    return {
        "dataset_id": dataset_id,
        "status": goat_profile.get("goat_status") or "pending",
        "collection_id": collection_id,
        "title": item.get("name"),
        "source": {
            "type": "geoapi_collection",
            "collection_items_url": f"/collections/{collection_id}/items",
        },
        "style_hint": {
            "feature_layer_type": item.get("feature_layer_type"),
            "feature_layer_geometry_type": item.get("feature_layer_geometry_type"),
        },
    }
