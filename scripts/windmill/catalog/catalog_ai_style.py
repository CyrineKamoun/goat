"""Windmill step 3: apply AI style suggestions to ingested catalog layers.

The AI picks both the most relevant attribute field and an appropriate color
range for that field, guided by real column statistics fetched from DuckLake.

Field-stat rules:
  numeric  → min/max/mean/null_pct passed to AI → quantile coloring
  string ≤ 10 distinct → unique values passed → ordinal coloring
  string > 10 distinct → excluded (too many categories)
  attribute_mapping NULL → no field selection, flat colour kept
"""

from __future__ import annotations

import importlib
import json
import logging
import math
from datetime import datetime, timezone
from typing import Any

log = logging.getLogger(__name__)

import psycopg


# ---------------------------------------------------------------------------
# Attribute-mapping helpers
# ---------------------------------------------------------------------------

def _fields_from_attribute_mapping(
    attribute_mapping: dict[str, Any] | None,
) -> list[dict[str, str]]:
    """Convert attribute_mapping to [{name, type, _internal_col}] dicts.

    Column key prefixes encode the stored type:
      text_attr*    → string
      float_attr*   → number
      integer_attr* → number
      bigint_attr*  → number

    `_internal_col` is kept for DuckLake stat queries and stripped before
    sending to the AI.
    """
    if not attribute_mapping:
        return []
    fields: list[dict[str, str]] = []
    for col_key, original_name in attribute_mapping.items():
        if not original_name or not col_key:
            continue
        if col_key.startswith(("float_attr", "integer_attr", "bigint_attr")):
            fields.append({"name": str(original_name), "type": "number", "_internal_col": col_key})
        elif col_key.startswith("text_attr"):
            fields.append({"name": str(original_name), "type": "string", "_internal_col": col_key})
    return fields


# ---------------------------------------------------------------------------
# DuckLake schema introspection (fallback when attribute_mapping is NULL)
# ---------------------------------------------------------------------------

_GEOMETRY_COLS = frozenset({
    "geom", "geometry", "wkb_geometry", "the_geom", "shape", "wkt",
    "bbox", "xmin", "xmax", "ymin", "ymax", "lon", "lat", "longitude", "latitude",
})
_SKIP_COLS     = frozenset({"layer_id", "fid", "ogc_fid", "gid", "objectid", "id"})

_DUCKDB_NUMBER_TYPES = (
    "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "HUGEINT",
    "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC", "REAL",
    "INT", "INT4", "INT8", "INT2", "INT1",
)


def _fields_from_pg_ducklake_catalog(
    pg_conn: Any,
    schema_name: str,
    table_name: str,
    ducklake_catalog_schema: str,
) -> list[dict[str, str]]:
    """Return [{name, type, _internal_col}] by querying the DuckLake PostgreSQL catalog.

    DuckLake stores column metadata in PostgreSQL (ducklake_column table).
    We use the existing pg_conn — no DuckDB extension required.
    Geometry and system columns are excluded.
    """
    # DuckLake column_type values use DuckDB type names (VARCHAR, INTEGER, DOUBLE, etc.)
    _PG_NUMBER_PREFIXES = (
        "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "HUGEINT",
        "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC", "REAL",
        "INT", "UBIGINT", "UINTEGER",
    )
    cs = dp._validate_schema(ducklake_catalog_schema)
    try:
        with pg_conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                f"""
                SELECT c.column_name, c.column_type
                FROM {cs}.ducklake_column c
                JOIN {cs}.ducklake_table  t ON c.table_id  = t.table_id
                JOIN {cs}.ducklake_schema s ON t.schema_id = s.schema_id
                WHERE s.schema_name = %s
                  AND t.table_name  = %s
                ORDER BY c.column_name
                """,
                (schema_name, table_name),
            )
            rows = cur.fetchall()
    except Exception as exc:
        log.warning(
            "DuckLake catalog query failed for %s.%s: %s", schema_name, table_name, exc
        )
        pg_conn.rollback()
        return []

    fields: list[dict[str, str]] = []
    for row in rows:
        col_name = row["column_name"]
        col_type = (row["column_type"] or "").upper().split("(")[0].strip()
        col_lower = col_name.lower()
        if col_lower in _GEOMETRY_COLS or col_lower in _SKIP_COLS:
            continue
        if any(col_type.startswith(p) for p in _PG_NUMBER_PREFIXES):
            ftype = "number"
        elif "VARCHAR" in col_type or "TEXT" in col_type or "CHAR" in col_type:
            ftype = "string"
        else:
            continue  # GEOMETRY, LIST, STRUCT, BLOB, etc.
        fields.append({"name": col_name, "type": ftype, "_internal_col": col_name})

    log.info(
        "DuckLake PG catalog: %s.%s → %d fields: %s",
        schema_name, table_name, len(fields), [f["name"] for f in fields],
    )
    return fields


# ---------------------------------------------------------------------------
# DuckLake field-statistics fetcher
# ---------------------------------------------------------------------------

def _fetch_field_stats(
    duckdb_con: Any,
    schema_name: str | None,
    table_name: str | None,
    fields: list[dict[str, str]],
) -> list[dict[str, Any]]:
    """Enrich each field dict with real column statistics from DuckLake.

    Numeric fields get: min, max, mean, null_pct.
    String fields with ≤ 10 distinct values get: distinct_count, null_pct, unique_values.
    String fields with > 10 distinct values are excluded entirely.

    Falls back to the original name/type-only list if stats cannot be fetched.
    """
    if not schema_name or not table_name or not fields:
        return fields

    full_table = f"lake.\"{schema_name}\".\"{table_name}\""
    enriched: list[dict[str, Any]] = []

    for field in fields:
        internal_col = field["_internal_col"]
        col_ref = f'"{internal_col}"'
        ftype = field["type"]

        try:
            if ftype == "number":
                row = duckdb_con.execute(
                    f"""
                    SELECT
                        MIN({col_ref})::DOUBLE,
                        MAX({col_ref})::DOUBLE,
                        AVG({col_ref})::DOUBLE,
                        COUNT(*),
                        COUNT({col_ref})
                    FROM {full_table}
                    """
                ).fetchone()
                if row is None:
                    enriched.append({"name": field["name"], "type": "number"})
                    continue
                col_min, col_max, col_mean, total, non_null = row
                null_pct = round((1 - non_null / total) * 100) if total else 0

                def _fmt(v: Any) -> Any:
                    if v is None or (isinstance(v, float) and math.isnan(v)):
                        return None
                    return round(float(v), 4)

                # Compute 6 quantile breaks for 7-color sequential palette.
                # These become color_scale_breaks in the frontend step expression.
                n_colors = 7
                fracs = [round(i / n_colors, 6) for i in range(1, n_colors)]
                try:
                    breaks_row = duckdb_con.execute(
                        f"SELECT quantile_cont({col_ref}, {fracs!r}::DOUBLE[]) FROM {full_table}"
                    ).fetchone()
                    raw_breaks = list(breaks_row[0]) if breaks_row and breaks_row[0] else []
                    q_breaks = [_fmt(b) for b in raw_breaks if b is not None]
                except Exception:
                    q_breaks = []

                entry: dict[str, Any] = {
                    "name": field["name"],
                    "type": "number",
                    "min": _fmt(col_min),
                    "max": _fmt(col_max),
                    "mean": _fmt(col_mean),
                    "null_pct": null_pct,
                }
                if q_breaks:
                    entry["breaks"] = q_breaks
                enriched.append(entry)

            else:  # string
                row = duckdb_con.execute(
                    f"""
                    SELECT COUNT(DISTINCT {col_ref}), COUNT(*), COUNT({col_ref})
                    FROM {full_table}
                    """
                ).fetchone()
                if row is None:
                    # skip — no data
                    continue
                distinct_count, total, non_null = row
                if distinct_count > 10:
                    # Too many categories — not useful for ordinal styling; exclude.
                    log.debug("field %r has %d distinct values (>10); excluded", field["name"], distinct_count)
                    continue

                null_pct = round((1 - non_null / total) * 100) if total else 0
                unique_rows = duckdb_con.execute(
                    f"""
                    SELECT DISTINCT {col_ref}
                    FROM {full_table}
                    WHERE {col_ref} IS NOT NULL
                    LIMIT 10
                    """
                ).fetchall()
                unique_values = [str(r[0]) for r in unique_rows]

                enriched.append({
                    "name": field["name"],
                    "type": "string",
                    "distinct_count": int(distinct_count),
                    "null_pct": null_pct,
                    "unique_values": unique_values,
                })

        except Exception as exc:
            log.warning("stat query failed for field %r: %s; using name/type only", field["name"], exc)
            enriched.append({"name": field["name"], "type": ftype})

    return enriched if enriched else fields


# ---------------------------------------------------------------------------
# Pipeline module loader
# ---------------------------------------------------------------------------

def _load_pipeline_module() -> Any:
    import os
    import sys
    import types

    for module_path in (
        "f.goat.tasks.datacatalog_pipeline",
        "scripts.windmill.catalog.datacatalog_pipeline",
    ):
        try:
            return importlib.import_module(module_path)
        except ModuleNotFoundError:
            continue

    try:
        import httpx

        base_url = (os.environ.get("BASE_INTERNAL_URL") or os.environ.get("WM_BASE_URL") or "http://windmill-server:8000").rstrip("/")
        workspace = os.environ.get("WM_WORKSPACE", "goat")
        token = os.environ.get("WM_TOKEN", "")
        script_path = os.environ.get(
            "DATACATALOG_WM_PATH", "f/goat/tasks/datacatalog_pipeline"
        )
        resp = httpx.get(
            f"{base_url}/api/w/{workspace}/scripts/get/p/{script_path}",
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        resp.raise_for_status()
        content = resp.json().get("content", "")
        module = types.ModuleType("datacatalog_pipeline")
        module.__file__ = "datacatalog_pipeline.py"
        exec(compile(content, "datacatalog_pipeline.py", "exec"), module.__dict__)  # noqa: S102
        sys.modules["datacatalog_pipeline"] = module
        return module
    except Exception as exc:
        raise ModuleNotFoundError(
            f"Could not import datacatalog_pipeline module: {exc}"
        ) from exc


dp = _load_pipeline_module()


# ---------------------------------------------------------------------------
# Lightweight DuckLake connection (no Windmill variable fetching)
# ---------------------------------------------------------------------------

def _open_ducklake_con() -> Any | None:
    """Open a DuckDB connection with DuckLake attached using env vars directly.

    Avoids going through SimpleToolRunner / ToolSettings.from_env(), which
    calls the Windmill secrets API for every setting and fails in environments
    where those variables haven't been created.

    Returns None if the connection cannot be established (missing deps, bad
    config); the caller falls back to name/type-only field lists.
    """
    import os
    try:
        import duckdb
    except ImportError:
        log.warning("duckdb not installed; field stats unavailable")
        return None

    pg_host     = dp._env("META_PG_HOST",     dp._env("POSTGRES_SERVER", "db"))
    pg_port     = dp._env("META_PG_PORT",     dp._env("POSTGRES_PORT", "5432"))
    pg_user     = dp._env("META_PG_USER",     dp._env("POSTGRES_USER", "postgres"))
    pg_password = dp._env("META_PG_PASSWORD", dp._env("POSTGRES_PASSWORD", ""))
    pg_db       = dp._env("META_PG_DB",       dp._env("POSTGRES_DB", "goat"))

    pg_uri = (
        os.environ.get("POSTGRES_DATABASE_URI")
        or f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}"
    )
    data_dir      = dp._env("DUCKLAKE_DATA_DIR", "/app/data/ducklake")
    catalog_schema = dp._env("DUCKLAKE_CATALOG_SCHEMA", "ducklake")

    try:
        con = duckdb.connect()
        for ext in ("httpfs", "postgres", "ducklake"):
            con.execute(f"INSTALL {ext}; LOAD {ext};")

        # Optional S3/MinIO – only configure when endpoint is set.
        s3_endpoint = dp._env("S3_ENDPOINT_URL", "")
        if s3_endpoint:
            con.execute(f"""
                SET s3_endpoint = '{s3_endpoint}';
                SET s3_access_key_id = '{dp._env("S3_ACCESS_KEY_ID", "")}';
                SET s3_secret_access_key = '{dp._env("S3_SECRET_ACCESS_KEY", "")}';
                SET s3_url_style = 'path';
                SET s3_use_ssl = false;
            """)

        con.execute(f"""
            ATTACH 'ducklake:postgres:{pg_uri}' AS lake (
                DATA_PATH '{data_dir}',
                METADATA_SCHEMA '{catalog_schema}',
                OVERRIDE_DATA_PATH true
            )
        """)
        con.execute("SELECT 1")  # health-check
        return con
    except Exception as exc:
        log.warning("Could not open DuckLake connection; field stats unavailable: %s", exc)
        return None


# ---------------------------------------------------------------------------
# AI field + color-range selection
# ---------------------------------------------------------------------------

def _ai_suggest_field_style(
    *,
    fields: list[dict[str, Any]],
    metadata: dict[str, Any],
    geometry_type: str | None,
    planning_theme: str | None,
    log: logging.Logger,
) -> dict[str, Any] | None:
    """Ask the AI to pick the best field and a matching color range.

    `fields` is the stat-enriched list from `_fetch_field_stats`. Numeric fields
    carry min/max/mean/null_pct; string fields carry distinct_count/unique_values.
    String fields with > 10 distinct values have already been excluded.

    Returns a dict with:
      color_field  – {"name": str, "type": "number|string"}
      color_scale  – "quantile" | "ordinal"
      color_range  – ColorBrewer-style dict (name, type, category, colors[])

    Returns None when the AI is unavailable, no fields are present, or nothing
    is clearly relevant.
    """
    if not fields:
        return None

    ai_config = dp._resolve_ai_runtime_config()
    ai_url = ai_config.get("url", "").strip()
    if not ai_url:
        log.debug("AI disabled – skipping field style suggestion")
        return None

    model = ai_config.get("model", "mimo-v2-pro")
    api_key = ai_config.get("api_key", "").strip()

    # Strip the internal _internal_col key before sending to the AI.
    ai_fields = [{k: v for k, v in f.items() if k != "_internal_col"} for f in fields]

    is_point = geometry_type == "point"

    system_prompt = (
        "You are a geospatial data stylist for a urban-planning WebGIS platform. "
        "Given a map layer's topic and its attribute fields with real statistics, "
        "choose the best field for colour-coding, a semantically appropriate "
        "ColorBrewer palette, and (for point layers) a suitable map icon. "
        "Return only a JSON object. No markdown."
    )

    palette_guidance = (
        "Semantic palette rules (choose the most fitting):\n"
        "- Population density / counts / intensity → 'YlOrRd' or 'Reds' (light-yellow → dark-red)\n"
        "- Environmental quality / greenness / vegetation → 'YlGn' or 'Greens'\n"
        "- Water / precipitation / flood risk → 'Blues' or 'GnBu'\n"
        "- Low–high risk / danger / noise → 'YlOrRd' or 'OrRd'\n"
        "- Accessibility / travel time (lower = better) → 'RdYlGn' reversed (green=near, red=far)\n"
        "- Income / socioeconomic → 'PuRd' or 'BuPu'\n"
        "- Land use categories / typology → qualitative 'Set2' or 'Paired'\n"
        "- Diverging (positive vs negative change) → 'RdBu' or 'PiYG'\n"
        "For sequential/quantile: provide exactly 7 hex colours light-to-dark.\n"
        "For ordinal/qualitative: provide up to 8 visually distinct hex colours."
    )

    icon_guidance = ""
    available_icons: list[str] = []
    if is_point:
        # Flat icon list — mirrors MAKI_ICON_TYPES in apps/web/lib/constants/icons.ts exactly.
        available_icons = [
            # Urban Infrastructure
            "bridge","building-alt1","building","communications-tower","construction",
            "elevator","entrance-alt1","entrance","fire-station-JP","fire-station",
            "highway-rest-area","historic","home","hospital-JP","hospital","lift-gate",
            "police-JP","police","prison","roadblock","school-JP","school","shelter",
            "square-stroked","square","telephone","toll","town-hall","town","tunnel",
            "village","bridge-solid","building-solid","city-solid","film-solid",
            "helicopter-symbol-solid","hospital-regular","house-solid",
            "person-digging-solid","recycle-solid","road-circle-exclamation-solid",
            "road-solid","school-solid","child-solid","solar-panel-solid","tent-solid",
            "tower-cell-solid","tower-observation-solid","traffic-light-solid",
            "water-solid","wheelchair-solid","wind-turbine-solid-pro","waste-basket",
            # Cultural and Landmark
            "art-gallery","castle-JP","castle","landmark-JP","landmark","landmark-solid",
            "monument-JP","monument","museum","place-of-worship","church-solid",
            "mosque-solid","theatre","book-solid","masks-theater-solid","monument-solid",
            "marker","marker-stroked","foundation-marker","bookmark-regular",
            "bookmark-solid","circle-info-solid","flag-regular","globe-solid",
            "location-crosshairs-solid","location-dot-solid","map-pin-solid",
            # Shopping & Services
            "shop","shop-solid","post-JP","post","car-rental","car-repair","industry",
            "industry-solid","teahouse","gas-pump-solid","warehouse","bread-slice-solid",
            "store-solid","cart-shopping-solid","basket-shopping-solid","tags-solid",
            "bread-loaf-solid","butcher_meat","farm_shop_basket","library-icon",
            "grocery-or-supermarket","pharmacy",
            # Recreation and Entertainment
            "amusement-park","aquarium","baseball","basketball","bbq","casino","cinema",
            "gaming","golf","karaoke","music","park-alt1","park","picnic-site",
            "playground","racetrack-boat","racetrack-cycling","racetrack-horse",
            "racetrack","restaurant-bbq","restaurant-noodle","restaurant-pizza",
            "restaurant-seafood","restaurant-sushi","restaurant","skiing","soccer",
            "swimming","table-tennis","basketball-solid","bowling-ball-solid",
            "burger-solid","candy-cane-solid","dumbbell-solid","futbol-regular",
            "martini-glass-solid","mug-hot-solid","spa-solid","volleyball-solid",
            "stadium",
            # Nature and Environment
            "beach","farm","garden-centre","garden","harbor","hot-spring","mountain",
            "natural","viewpoint","waterfall","wetland","windmill",
            # Health and Safety
            "defibrillator","drinking-water","emergency-phone","veterinary",
            "radiation-solid","stethoscope-solid","tooth-solid",
            # Transportation and Vehicles
            "aerialway","bicycle-share","bike_parking_big","bicycle","electric-bike-icon",
            "bus","ferry-JP","ferry","heliport","rail-light","rail-metro","rail",
            "scooter","slipway","snowmobile","charging-station","airfield","airport",
            "car-solid","charging-station-solid","sailboat-solid","taxi-solid",
            "truck-medical-solid","plane-departure-solid","terminal","road-accident",
            "parking-garage","parking-paid","parking",
        ]
        icon_guidance = (
            "\n5. Since this is a POINT layer, also suggest a map icon from the available list "
            "that semantically matches the layer topic. Return it as 'marker_name'. "
            "If nothing fits well, return null for marker_name."
        )

    task = (
        "1. Pick the single most meaningful field from 'fields' for colouring this layer.\n"
        "   - Numeric fields: prefer those with a wide min–max range relative to the mean "
        "     and a low null_pct. Wide range → quantile coloring.\n"
        "   - String fields: unique_values are provided. Pick ordinal if the categories "
        "     are semantically distinct and meaningful for the layer topic.\n"
        "   - String fields with >10 distinct values have already been removed from the list.\n"
        "   - Return null for color_field if no field is clearly relevant.\n"
        "2. Set color_scale: 'quantile' for numeric, 'ordinal' for string.\n"
        "3. Choose a ColorBrewer palette following the semantic rules below. "
        "   Consider the layer title, abstract and the data direction implied by the field name "
        "   (e.g. 'niedrig'/'hoch', 'risk', 'density', 'green', 'access').\n"
        f"4. {palette_guidance}"
        f"{icon_guidance}"
    )

    output_schema: dict[str, Any] = {
        "color_field": {"name": "field name from the list, or null", "type": "number | string"},
        "color_scale": "quantile | ordinal",
        "color_range": {
            "name": "palette name e.g. YlOrRd",
            "type": "sequential | qualitative",
            "category": "ColorBrewer",
            "colors": ["#hex1", "...", "#hex7"],
        },
        "rationale": "short string",
    }
    if is_point:
        output_schema["marker_name"] = "icon name from available_icons list, or null"

    user_payload: dict[str, Any] = {
        "task": task,
        "output_schema": output_schema,
        "layer": {
            "title": metadata.get("title"),
            "abstract": metadata.get("abstract"),
            "keywords": metadata.get("keywords") or [],
            "geometry_type": geometry_type,
            "planning_theme": planning_theme,
        },
        "fields": ai_fields,
    }
    if is_point and available_icons:
        user_payload["available_icons"] = available_icons

    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    request_body = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": json.dumps(user_payload, ensure_ascii=True)},
        ],
        "temperature": 0,
        "response_format": {"type": "json_object"},
    }

    ai_timeout = dp._clamp_float(
        dp._env("CATALOG_AI_TIMEOUT_SECONDS", "20"),
        20.0,
        1.0,
        120.0,
    )

    try:
        import httpx
        with httpx.Client(timeout=ai_timeout) as client:
            resp = client.post(ai_url, headers=headers, json=request_body)
            resp.raise_for_status()
            payload = resp.json()

        content = (
            (((payload.get("choices") or [{}])[0]).get("message") or {}).get("content")
            if isinstance(payload, dict)
            else None
        )
        if not content:
            raise ValueError("AI response missing choices[0].message.content")

        parsed = json.loads(content)
        log.info("AI raw response for field style: %s", parsed)

        cf = parsed.get("color_field")
        if not isinstance(cf, dict):
            return None

        name = str(cf.get("name") or "").strip()
        ftype = str(cf.get("type") or "").strip().lower()
        if not name or ftype not in ("number", "string"):
            return None

        # Reject field names not in the known list.
        valid_names = {f["name"] for f in fields}
        if name not in valid_names:
            log.warning("AI suggested unknown field %r; ignoring", name)
            return None

        color_scale = str(parsed.get("color_scale") or "quantile").strip().lower()
        if color_scale not in ("quantile", "ordinal"):
            color_scale = "quantile" if ftype == "number" else "ordinal"

        color_range = parsed.get("color_range")
        if not isinstance(color_range, dict) or not isinstance(color_range.get("colors"), list):
            color_range = None

        # Extract optional icon suggestion for point layers.
        marker_name: str | None = None
        if is_point:
            raw_marker = parsed.get("marker_name")
            if isinstance(raw_marker, str) and raw_marker.strip():
                candidate = raw_marker.strip()
                if candidate in available_icons:
                    marker_name = candidate
                else:
                    log.warning("AI suggested unknown icon %r; ignoring", candidate)

        log.debug(
            "field style selected: field=%r type=%r scale=%r palette=%r marker=%r rationale=%r",
            name, ftype, color_scale,
            (color_range or {}).get("name"),
            marker_name,
            parsed.get("rationale"),
        )
        result: dict[str, Any] = {
            "color_field": {"name": name, "type": ftype},
            "color_scale": color_scale,
            "color_range": color_range,
        }
        if marker_name:
            result["marker_name"] = marker_name
        return result

    except Exception as exc:
        log.warning("field style AI call failed, using flat colour: %s", exc)
        return None


# ---------------------------------------------------------------------------
# Style assembly
# ---------------------------------------------------------------------------

def _build_style_with_field(
    base_style: dict[str, Any],
    field_style: dict[str, Any],
    geometry_type: str | None,
    fields: list[dict[str, Any]],
) -> dict[str, Any]:
    """Merge AI field_style into base_style.

    Also injects:
    - color_scale_breaks  for numeric/quantile fields (needed by the frontend
      MapLibre ["step"] expression builder)
    - color_range.color_map  for ordinal/string fields (needed by ["match"]
      expression builder)
    """
    style = dict(base_style)
    color_field = field_style["color_field"]
    color_scale  = field_style["color_scale"]
    color_range  = field_style.get("color_range")

    # Find stats for the chosen field.
    field_name = color_field["name"]
    field_stats = next((f for f in fields if f["name"] == field_name), None)

    style["color_field"] = color_field
    style["color_scale"]  = color_scale

    # Build color_range, enriching with color_map for ordinal.
    final_color_range = dict(color_range) if color_range else {}
    if color_scale == "ordinal" and field_stats and field_stats.get("unique_values"):
        colors = final_color_range.get("colors") or []
        color_map = [
            [[v], colors[i % len(colors)]]
            for i, v in enumerate(field_stats["unique_values"])
        ]
        final_color_range["color_map"] = color_map
    if final_color_range:
        style["color_range"] = final_color_range

    # Add quantile break points for numeric scales.
    if color_scale in ("quantile", "equal_interval", "standard_deviation", "heads_and_tails"):
        if field_stats and field_stats.get("breaks"):
            style["color_scale_breaks"] = {
                "min": field_stats.get("min"),
                "max": field_stats.get("max"),
                "mean": field_stats.get("mean"),
                "breaks": field_stats["breaks"],
            }

    if geometry_type == "line":
        style["stroke_color_field"] = color_field
        style["stroke_color_scale"] = color_scale
        if final_color_range:
            style["stroke_color_range"] = final_color_range

    # Apply icon for point layers when AI suggested one.
    marker_name = field_style.get("marker_name")
    if marker_name and geometry_type == "point":
        style["custom_marker"] = True
        style["marker"] = {
            "name": marker_name,
            "url": f"https://assets.plan4better.de/icons/maki/{marker_name}.svg",
            "source": "library",
        }

    return style


# ---------------------------------------------------------------------------
# Table bootstrapping
# ---------------------------------------------------------------------------

def _ensure_style_table(conn: psycopg.Connection, schema: str) -> None:
    s = dp._validate_schema(schema)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {s}.ai_style_queue (
                id               UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                run_id           TEXT,
                package_id       TEXT,
                resource_id      TEXT,
                layer_id         UUID NOT NULL,
                applied          BOOLEAN NOT NULL,
                rationale        TEXT,
                fields_jsonb     JSONB,
                style_jsonb      JSONB,
                created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        # Add the column to existing tables created before this migration.
        cur.execute(
            f"""
            ALTER TABLE {s}.ai_style_queue
            ADD COLUMN IF NOT EXISTS fields_jsonb JSONB
            """
        )
    conn.commit()


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def main(relevance_run_id: str | None = None) -> dict[str, Any]:
    pg_conn = psycopg.connect(
        host=dp._require("META_PG_HOST", "POSTGRES_SERVER"),
        port=int(dp._env("META_PG_PORT", dp._env("POSTGRES_PORT", "5432"))),
        dbname=dp._require("META_PG_DB", "POSTGRES_DB"),
        user=dp._require("META_PG_USER", "POSTGRES_USER"),
        password=dp._env("META_PG_PASSWORD", dp._env("POSTGRES_PASSWORD", "")) or None,
        autocommit=False,
    )
    catalog_schema = dp._env("META_PG_SCHEMA", "datacatalog")
    customer_schema = dp._env("CUSTOMER_SCHEMA", "customer")
    ducklake_catalog_schema = dp._env("DUCKLAKE_CATALOG_SCHEMA", "ducklake")
    _ensure_style_table(pg_conn, catalog_schema)

    cs = dp._validate_schema(customer_schema)
    ds = dp._validate_schema(catalog_schema)

    run_filter_sql = ""
    params: tuple[Any, ...] = ()
    if relevance_run_id:
        run_filter_sql = (
            f"""
            AND EXISTS (
                SELECT 1 FROM {ds}.ai_relevance_queue q
                WHERE q.run_id = %s
                  AND q.package_id = dl.package_id
                  AND q.resource_id = dl.resource_id
                  AND q.selected = TRUE
            )
            """
        )
        params = (relevance_run_id,)

    with pg_conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            f"""
            SELECT
                dl.layer_id,
                dl.package_id,
                dl.resource_id,
                dl.metadata_jsonb,
                dl.schema_name,
                dl.table_name,
                cl.feature_layer_geometry_type,
                cl.attribute_mapping
            FROM {ds}.layer dl
            JOIN {cs}.layer cl ON cl.id = dl.layer_id
            WHERE dl.is_latest = TRUE
              AND cl.in_catalog = TRUE
              {run_filter_sql}
            """,
            params,
        )
        rows = cur.fetchall()

    # One DuckLake connection shared across all rows for efficiency.
    duckdb_con = _open_ducklake_con()

    applied = 0
    for row in rows:
        metadata = dict(row.get("metadata_jsonb") or {})
        package = {
            "id": row.get("package_id"),
            "title": metadata.get("title"),
            "notes": metadata.get("abstract"),
        }
        resource = {
            "id": row.get("resource_id"),
            "name": metadata.get("title"),
            "format": metadata.get("source", {}).get("format"),
        }

        # Step 1 – relevance + theme classification + flat colour (existing).
        decision = dp._ai_evaluate_dataset(
            metadata=metadata,
            package=package,
            resource=resource,
            log=dp.logging.getLogger(__name__),
        )
        styled_metadata = {**metadata, "ai": decision}
        base_style = dp._merge_style_with_ai(
            row.get("feature_layer_geometry_type"), styled_metadata
        )

        # Step 2 – collect fields, enrich with stats, ask AI for field + palette.
        planning_theme: str | None = decision.get("planning_theme")
        layer_id = row["layer_id"]
        schema_name = row.get("schema_name")
        table_name  = row.get("table_name")
        geom_type   = row.get("feature_layer_geometry_type")

        fields = _fields_from_attribute_mapping(row.get("attribute_mapping"))
        log.info(
            "layer=%s theme=%r geom=%r attribute_mapping_fields=%d",
            layer_id, planning_theme, geom_type, len(fields),
        )

        # Fallback: derive fields from the DuckLake PostgreSQL catalog when
        # attribute_mapping is NULL (always the case for catalog layers).
        if not fields and schema_name and table_name:
            fields = _fields_from_pg_ducklake_catalog(
                pg_conn, schema_name, table_name, ducklake_catalog_schema
            )
            log.info("layer=%s schema_fallback_fields=%d", layer_id, len(fields))

        if fields and duckdb_con is not None and schema_name and table_name:
            fields = _fetch_field_stats(duckdb_con, schema_name, table_name, fields)
            log.info(
                "layer=%s stats_enriched_fields=%s",
                layer_id,
                [{k: v for k, v in f.items() if k != "_internal_col"} for f in fields],
            )
        elif not fields:
            log.info("layer=%s no fields available – keeping flat colour", layer_id)

        field_style = _ai_suggest_field_style(
            fields=fields,
            metadata=metadata,
            geometry_type=geom_type,
            planning_theme=planning_theme,
            log=log,
        )
        log.info("layer=%s field_style=%s", layer_id, field_style)

        # Step 3 – apply field-based style on top, or keep flat colour.
        style = (
            _build_style_with_field(base_style, field_style, geom_type, fields)
            if field_style
            else base_style
        )
        log.info(
            "layer=%s final style keys=%s  color_field=%s",
            layer_id,
            list(style.keys()),
            style.get("color_field"),
        )

        # UPDATE first — committed on its own so a queue-insert failure can
        # never roll back the style that was already applied.
        with pg_conn.cursor() as cur:
            cur.execute(
                f"UPDATE {cs}.layer SET properties = %s WHERE id = %s",
                (dp.Jsonb(style), row["layer_id"]),
            )
            if cur.rowcount == 0:
                log.warning("layer=%s UPDATE matched 0 rows – properties NOT saved", layer_id)
        pg_conn.commit()

        # INSERT into audit queue separately — failures here are non-fatal.
        fields_for_db = [
            {k: v for k, v in f.items() if k != "_internal_col"}
            for f in fields
        ]
        try:
            with pg_conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {ds}.ai_style_queue (
                        run_id, package_id, resource_id, layer_id,
                        applied, rationale, fields_jsonb, style_jsonb, created_at
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (
                        relevance_run_id,
                        row.get("package_id"),
                        row.get("resource_id"),
                        row["layer_id"],
                        True,
                        str(decision.get("rationale") or ""),
                        dp.Jsonb(fields_for_db),
                        dp.Jsonb(style),
                        datetime.now(timezone.utc),
                    ),
                )
            pg_conn.commit()
        except Exception as exc:
            log.warning("layer=%s ai_style_queue insert failed: %s", layer_id, exc)
            pg_conn.rollback()
        applied += 1
        if applied % 10 == 0:
            log.info("ai_style progress: styled=%d", applied)

    pg_conn.close()
    return {
        "styled_layers": applied,
        "status": "success",
    }
