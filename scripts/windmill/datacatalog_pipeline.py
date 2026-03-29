# py311

"""Windmill script: sync CKAN catalog to GOAT PostgreSQL.

Runs on the ``worker-tools`` queue where goatlib and its dependencies
(DuckDB, httpx, etc.) are pre-installed.  Only ``psycopg[binary]`` is
added via goatlib's ``[datacatalog]`` optional dependency.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import tempfile
import uuid
from datetime import datetime, timezone
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse
from xml.etree import ElementTree as ET

import httpx
import psycopg
import psycopg.rows
from psycopg.types.json import Jsonb

from goatlib.tools.style import get_default_style


# ---------------------------------------------------------------------------
# Config helpers — all settings come from environment variables only
# ---------------------------------------------------------------------------

def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default) or default


def _require(*names: str) -> str:
    """Return the first non-empty env var value; raise if all are missing."""
    for name in names:
        v = _env(name)
        if v:
            return v
    raise ValueError(
        f"Missing required environment variable. Tried: {', '.join(names)}"
    )


def _is_truthy(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _canonical_ducklake_metadata_dir(path: str) -> str:
    """Resolve DuckLake data dir for persisted metadata paths.

    Keep parity with regular layer upload behavior by honoring the configured
    `DUCKLAKE_DATA_DIR`. When host-local paths leak into container runtime,
    remap to the container mount path.
    """
    configured = (path or "").strip() or "/app/data/ducklake"
    configured = configured.rstrip("/")

    # In containers, host-local dev paths are not readable.
    if os.path.exists("/.dockerenv") and configured.startswith("/home/"):
        if "/data/ducklake" in configured:
            return "/app/data/ducklake"

    return configured

# ---------------------------------------------------------------------------
# CKAN API helpers
# ---------------------------------------------------------------------------

def _resolve_ckan_api_key() -> str | None:
    """Return the CKAN API key, auto-resolving from the CKAN DB if not set.

    Resolution order:
    1. ``CKAN_API_KEY`` env var (explicit, preferred)
    2. Sysadmin legacy ``apikey`` fetched from the CKAN PostgreSQL DB
    """
    if explicit := _env("CKAN_API_KEY"):
        return explicit

    host = _env("CKAN_DB_HOST", "goat-ckan-db")
    port = int(_env("CKAN_DB_PORT", "5432"))
    dbname = _env("CKAN_DB_NAME", "ckan")
    user = _env("CKAN_DB_USER", "ckan")
    password = _env("CKAN_DB_PASSWORD", _env("CKAN_PG_PASSWORD", "ckan"))
    try:
        with psycopg.connect(
            host=host, port=port, dbname=dbname, user=user, password=password,
            connect_timeout=5,
        ) as conn:
            row = conn.execute(
                'SELECT apikey FROM "user" WHERE sysadmin = true'
                " AND apikey IS NOT NULL AND apikey != '' LIMIT 1"
            ).fetchone()
        if row:
            logging.getLogger(__name__).info("CKAN API key auto-resolved from CKAN DB (sysadmin legacy key)")
            return str(row[0])
    except Exception as exc:
        logging.getLogger(__name__).warning("Could not auto-resolve CKAN API key from DB: %s", exc)

    return None


def _fetch_latest_harvest_xml(package_id: str) -> str | None:
    """Fetch latest non-empty XML metadata from CKAN harvest_object.content."""
    if not package_id:
        return None

    host = _env("CKAN_DB_HOST", "goat-ckan-db")
    port = int(_env("CKAN_DB_PORT", "5432"))
    dbname = _env("CKAN_DB_NAME", "ckan")
    user = _env("CKAN_DB_USER", "ckan")
    password = _env("CKAN_DB_PASSWORD", _env("CKAN_PG_PASSWORD", "ckan"))

    query_latest_current = """
        SELECT content
        FROM harvest_object
        WHERE package_id = %s
          AND current IS TRUE
          AND content IS NOT NULL
          AND btrim(content) <> ''
        ORDER BY
            COALESCE(import_finished, fetch_finished, gathered, metadata_modified_date) DESC NULLS LAST,
            id DESC
        LIMIT 1
    """
    query_latest_any = """
        SELECT content
        FROM harvest_object
        WHERE package_id = %s
          AND content IS NOT NULL
          AND btrim(content) <> ''
        ORDER BY
            COALESCE(import_finished, fetch_finished, gathered, metadata_modified_date) DESC NULLS LAST,
            id DESC
        LIMIT 1
    """

    try:
        with psycopg.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password,
            connect_timeout=5,
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(query_latest_current, (package_id,))
                row = cur.fetchone()
                if row and row[0]:
                    return str(row[0])

                cur.execute(query_latest_any, (package_id,))
                row = cur.fetchone()
                if row and row[0]:
                    return str(row[0])
    except Exception as exc:
        logging.getLogger(__name__).warning(
            "Could not fetch harvest metadata from CKAN DB for package_id=%s: %s",
            package_id,
            exc,
        )

    return None


def _ckan_headers(api_key: str | None) -> dict[str, str]:
    headers = {"Accept": "application/json"}
    if api_key:
        headers["Authorization"] = api_key
    return headers


def check_ckan_health(health_url: str, api_key: str | None) -> dict[str, Any]:
    """Return readiness dict; ``ready`` is True when CKAN responds successfully."""
    try:
        r = httpx.get(
            health_url,
            headers=_ckan_headers(api_key),
            timeout=20,
            follow_redirects=True,
        )
        ok = 200 <= r.status_code < 300
        payload: dict[str, Any] = {}
        try:
            payload = r.json()
        except Exception:
            pass
        logical_ok = bool(payload.get("success", True)) if ok else False
        ready = ok and logical_ok
        return {
            "ready": ready,
            "status_code": r.status_code,
            "url": health_url,
            "reason": None if ready else f"http_{r.status_code}",
        }
    except Exception as exc:
        return {"ready": False, "url": health_url, "reason": str(exc)}


def _candidate_ckan_bases() -> list[str]:
    """Build ordered CKAN base URL candidates from env and common container names."""
    raw_candidates: list[str] = []

    explicit = _env("CKAN_API_URL") or _env("CKAN_URL") or _env("CKAN_SITE_URL")
    if explicit:
        raw_candidates.append(explicit)

    # Optional comma-separated fallback list for operators.
    extra = _env("CKAN_API_URL_CANDIDATES", "")
    if extra:
        raw_candidates.extend([item.strip() for item in extra.split(",") if item.strip()])

    # Common local/container defaults.
    raw_candidates.extend(
        [
            "http://ckan:5000",
            "http://goat-ckan:5000",
            "http://host.docker.internal:5050",
        ]
    )

    normalized: list[str] = []
    seen: set[str] = set()
    for candidate in raw_candidates:
        if not candidate:
            continue
        value = candidate.strip().rstrip("/")
        if not value:
            continue
        if not value.startswith(("http://", "https://")):
            value = f"http://{value}"
        if value not in seen:
            seen.add(value)
            normalized.append(value)
    return normalized


def _resolve_ckan_urls(api_key: str | None) -> tuple[str, str, str]:
    """Resolve working CKAN base + package_search + health URLs.

    If explicit endpoint envs are provided they are used directly; otherwise we probe
    candidate CKAN base URLs and return the first healthy endpoint.
    """
    explicit_package = _env("CKAN_PACKAGE_SEARCH_URL", "").strip()
    explicit_health = _env("CKAN_API_HEALTH_URL", "").strip()

    if explicit_package and explicit_health:
        inferred_base = _env("CKAN_API_URL", _env("CKAN_URL", _env("CKAN_SITE_URL", ""))).strip().rstrip("/")
        if not inferred_base:
            marker = "/api/3/action/"
            if marker in explicit_package:
                inferred_base = explicit_package.split(marker, 1)[0].rstrip("/")
        return inferred_base, explicit_package, explicit_health

    failures: list[dict[str, str]] = []
    for base in _candidate_ckan_bases():
        health_url = explicit_health or f"{base}/api/3/action/status_show"
        readiness = check_ckan_health(health_url, api_key)
        if readiness.get("ready"):
            package_url = explicit_package or f"{base}/api/3/action/package_search"
            return base, package_url, health_url
        failures.append(
            {
                "base": base,
                "health_url": health_url,
                "reason": str(readiness.get("reason") or "unhealthy"),
            }
        )

    raise RuntimeError(
        "Unable to reach CKAN health endpoint from worker. "
        f"Tried candidates: {json.dumps(failures, sort_keys=True)}"
    )


def fetch_ckan_packages(
    package_search_url: str,
    api_key: str | None,
    *,
    page_size: int = 200,
    max_pages: int = 100,
) -> list[dict[str, Any]]:
    """Paginate through CKAN package_search and return all active packages."""
    packages: list[dict[str, Any]] = []
    with httpx.Client(
        timeout=60,
        follow_redirects=True,
        headers=_ckan_headers(api_key),
    ) as client:
        for page in range(max_pages):
            r = client.get(
                package_search_url,
                params={
                    "rows": page_size,
                    "start": page * page_size,
                    "include_private": "true",
                },
            )
            r.raise_for_status()
            payload = r.json()
            if not isinstance(payload, dict) or not payload.get("success"):
                raise RuntimeError(
                    f"CKAN package_search returned success=false: {payload}"
                )
            results: list[dict[str, Any]] = (
                payload.get("result") or {}
            ).get("results") or []
            if not results:
                break
            packages.extend(results)
            if len(results) < page_size:
                break
    return packages


# ---------------------------------------------------------------------------
# Spatial resource detection
# ---------------------------------------------------------------------------

_SPATIAL_FORMATS = frozenset(
    {
        "geojson", "shapefile", "shp", "gpkg", "geopackage",
        "kml", "kmz", "wfs", "parquet", "geoparquet",
        "topojson", "zip",
    }
)
_SPATIAL_URL_SUFFIXES = (
    ".geojson", ".shp", ".gpkg", ".kml", ".kmz",
    ".parquet", ".geoparquet", ".topojson",
)


def is_spatial_resource(resource: dict[str, Any]) -> bool:
    fmt = str(resource.get("format") or "").strip().lower()
    url = str(resource.get("url") or "").strip().lower()
    mimetype = str(resource.get("mimetype") or "").strip().lower()
    return (
        fmt in _SPATIAL_FORMATS
        or "wfs" in fmt
        or "wfs" in mimetype
        or "service=wfs" in url
        or "request=getfeature" in url
        or url.split("?")[0].endswith(_SPATIAL_URL_SUFFIXES)
    )


# ---------------------------------------------------------------------------
# ISO 19139 XML metadata extraction
# ---------------------------------------------------------------------------

def _xml_text(root: ET.Element, *paths: str) -> str | None:
    for path in paths:
        node = root.find(path)
        if node is not None:
            text = "".join(node.itertext()).strip()
            if text:
                return text
    return None


def _xml_all_text(root: ET.Element, path: str) -> list[str]:
    return [
        t
        for node in root.findall(path)
        for t in ["".join(node.itertext()).strip()]
        if t
    ]


def extract_iso_metadata(xml: str) -> dict[str, Any]:
    """Extract key fields from ISO 19139 XML. Returns empty dict on parse error."""
    out: dict[str, Any] = {}
    try:
        root = ET.fromstring(xml)
    except ET.ParseError:
        return out

    out["title"] = _xml_text(
        root,
        ".//{*}identificationInfo//{*}citation//{*}title//{*}CharacterString",
        ".//{*}identificationInfo//{*}citation//{*}title//{*}Anchor",
    )
    out["abstract"] = _xml_text(
        root,
        ".//{*}identificationInfo//{*}abstract//{*}CharacterString",
        ".//{*}identificationInfo//{*}abstract//{*}Anchor",
    )
    out["keywords"] = _xml_all_text(
        root,
        ".//{*}descriptiveKeywords//{*}keyword//{*}CharacterString",
    )

    for contact in root.findall(".//{*}CI_ResponsibleParty"):
        org = _xml_text(contact, ".//{*}organisationName//{*}CharacterString")
        if org:
            out["distributor_name"] = org
            email = _xml_text(contact, ".//{*}electronicMailAddress//{*}CharacterString")
            if email:
                out["distributor_email"] = email
            break

    for box in root.findall(".//{*}EX_GeographicBoundingBox"):
        w = _xml_text(box, ".//{*}westBoundLongitude//{*}Decimal", ".//{*}westBoundLongitude//{*}Real")
        s = _xml_text(box, ".//{*}southBoundLatitude//{*}Decimal", ".//{*}southBoundLatitude//{*}Real")
        e = _xml_text(box, ".//{*}eastBoundLongitude//{*}Decimal", ".//{*}eastBoundLongitude//{*}Real")
        n = _xml_text(box, ".//{*}northBoundLatitude//{*}Decimal", ".//{*}northBoundLatitude//{*}Real")
        if w and s and e and n:
            try:
                out["bbox"] = {
                    "west": float(w),
                    "south": float(s),
                    "east": float(e),
                    "north": float(n),
                }
                break
            except ValueError:
                pass

    return out


def build_ckan_metadata(
    package: dict[str, Any],
    resource: dict[str, Any],
    xml: str | None,
) -> dict[str, Any]:
    resource_id = str(resource.get("id") or "")
    package_id = str(package.get("id") or "")

    record: dict[str, Any] = {
        "identifier": resource_id,
        "title": resource_id,
        "abstract": None,
        "keywords": [],
        "source": {"package_id": package_id, "resource_id": resource_id},
    }

    if xml:
        meta = extract_iso_metadata(xml)
        if meta.get("title"):
            record["title"] = meta["title"]
        if meta.get("abstract"):
            record["abstract"] = meta["abstract"]
        for field in ("keywords", "distributor_name", "distributor_email", "bbox"):
            if meta.get(field):
                record[field] = meta[field]

    return record


def _build_customer_xml_metadata(
    raw_xml: str | None,
    metadata: dict[str, Any],
) -> str | None:
    """Return metadata payload for customer.layer.xml_metadata.

    Use only CKAN CSW XML metadata. Do not persist JSON fallbacks.
    """
    _ = metadata
    if raw_xml and raw_xml.strip():
        return raw_xml
    return None


# ---------------------------------------------------------------------------
# PostgreSQL schema + upsert helpers
# ---------------------------------------------------------------------------

def _validate_schema(name: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise ValueError(f"Invalid schema name: {name!r}")
    return name


def _parse_uuid(value: str, env_name: str) -> uuid.UUID:
    try:
        return uuid.UUID(value)
    except ValueError as exc:
        raise ValueError(f"Invalid UUID in {env_name}: {value!r}") from exc


def _catalog_layer_uuid(resource_id: str) -> uuid.UUID:
    """Deterministic layer UUID for a catalog resource."""
    return uuid.uuid5(uuid.NAMESPACE_URL, f"goat:catalog:{resource_id}")


def _bbox_to_extent_wkt(metadata: dict[str, Any]) -> str | None:
    """Convert CSW bbox from metadata dict to WKT POLYGON."""
    bbox = metadata.get("bbox")
    if not bbox:
        return None
    try:
        w, s, e, n = float(bbox["west"]), float(bbox["south"]), float(bbox["east"]), float(bbox["north"])
        if w == e and s == n:
            return None
        return f"POLYGON(({w} {s}, {e} {s}, {e} {n}, {w} {n}, {w} {s}))"
    except (KeyError, ValueError, TypeError):
        return None


def _is_degenerate_extent(wkt_text: str | None) -> bool:
    """Return True if the extent is a degenerate single-point box."""
    if not wkt_text:
        return True
    normalized = _normalize_extent_wkt(wkt_text)
    if not normalized:
        return True
    # A degenerate polygon has all identical coordinates.
    coords = re.findall(r"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?", normalized)
    if len(coords) >= 4:
        xs = coords[0::2]
        ys = coords[1::2]
        if len(set(xs)) <= 1 and len(set(ys)) <= 1:
            return True
    return False


def upsert_customer_catalog_layer(
    conn: psycopg.Connection,
    *,
    customer_schema: str,
    owner_user_id: uuid.UUID,
    folder_id: uuid.UUID,
    layer_id: uuid.UUID,
    layer_name: str,
    geometry_type: str | None,
    extent_wkt: str | None,
    resource_id: str,
    xml_metadata: str | None,
    metadata: dict[str, Any] | None = None,
) -> str:
    """Upsert a catalog row in customer.layer and keep it pinned to Catalog folder."""
    s = _validate_schema(customer_schema)
    _ = resource_id

    # Use data extent if valid, otherwise fall back to CSW bbox.
    if _is_degenerate_extent(extent_wkt) and metadata:
        csw_extent = _bbox_to_extent_wkt(metadata)
        if csw_extent:
            extent_wkt = csw_extent
    normalized_extent_wkt = _normalize_extent_wkt(extent_wkt)

    # Generate default styling properties for the geometry type.
    properties = Jsonb(get_default_style(geometry_type))

    with conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE {s}.layer
            SET
                user_id = %s,
                folder_id = %s,
                name = %s,
                type = 'feature',
                feature_layer_type = 'standard',
                feature_layer_geometry_type = COALESCE(%s, feature_layer_geometry_type, 'polygon'),
                extent = CASE WHEN %s::text IS NOT NULL
                              THEN ST_Multi(ST_GeomFromText(%s, 4326)) ELSE NULL END,
                properties = COALESCE(%s, properties),
                in_catalog = TRUE,
                xml_metadata = %s,
                updated_at = NOW()
            WHERE id = %s
            RETURNING id
            """,
            (
                owner_user_id,
                folder_id,
                layer_name,
                geometry_type,
                normalized_extent_wkt,
                normalized_extent_wkt,
                properties,
                xml_metadata,
                layer_id,
            ),
        )
        row = cur.fetchone()
        if row:
            return str(row[0])

        cur.execute(
            f"""
            INSERT INTO {s}.layer (
                id, user_id, folder_id, name, type,
                feature_layer_type, feature_layer_geometry_type,
                extent, properties, in_catalog, xml_metadata,
                thumbnail_url, created_at, updated_at
            ) VALUES (
                %s, %s, %s, %s, 'feature',
                'standard', COALESCE(%s, 'polygon'),
                CASE WHEN %s::text IS NOT NULL
                     THEN ST_Multi(ST_GeomFromText(%s, 4326)) ELSE NULL END,
                %s, TRUE, %s,
                'https://assets.plan4better.de/img/goat_new_dataset_thumbnail.png',
                NOW(), NOW()
            )
            RETURNING id
            """,
            (
                layer_id,
                owner_user_id,
                folder_id,
                layer_name,
                geometry_type,
                normalized_extent_wkt,
                normalized_extent_wkt,
                properties,
                xml_metadata,
            ),
        )
        inserted = cur.fetchone()
        return str(inserted[0]) if inserted else ""


def ensure_catalog_schema(conn: psycopg.Connection, schema: str) -> None:
    """Create the datacatalog schema and tables if they don't exist."""
    s = _validate_schema(schema)
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {s}")
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {s}.processor_dataset_version (
                id           TEXT PRIMARY KEY,
                run_id       TEXT NOT NULL,
                package_id   TEXT NOT NULL,
                resource_id  TEXT NOT NULL,
                signature    TEXT NOT NULL,
                version_num  INTEGER NOT NULL,
                duckdb_path  TEXT NOT NULL,
                duckdb_table TEXT NOT NULL,
                row_count    BIGINT,
                status       TEXT NOT NULL,
                error        TEXT,
                processed_at TIMESTAMPTZ NOT NULL
            )
        """)
        cur.execute(f"""
            CREATE UNIQUE INDEX IF NOT EXISTS ux_processor_dataset_signature
            ON {s}.processor_dataset_version(resource_id, signature)
        """)
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {s}.layer (
                id                         UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                layer_id                   UUID,
                user_id                    UUID NOT NULL,
                name                       TEXT NOT NULL,
                resource_id                TEXT NOT NULL,
                base_resource_id           TEXT,
                version_num                INTEGER DEFAULT 1,
                is_latest                  BOOLEAN DEFAULT TRUE,
                superseded_at              TIMESTAMPTZ,
                metadata_jsonb             JSONB,
                xml_raw                    TEXT,
                schema_name                TEXT NOT NULL,
                table_name                 TEXT NOT NULL
            )
        """)
        # Migration-safe: add columns that might be missing in older deployments.
        for col, col_type in [
            ("layer_id", "UUID"),
            ("user_id", "UUID"),
            ("name", "TEXT"),
            ("base_resource_id", "TEXT"),
            ("version_num", "INTEGER"),
            ("is_latest", "BOOLEAN"),
            ("superseded_at", "TIMESTAMPTZ"),
            ("metadata_jsonb", "JSONB"),
            ("xml_raw", "TEXT"),
        ]:
            cur.execute(
                f"ALTER TABLE {s}.layer ADD COLUMN IF NOT EXISTS {col} {col_type}"
            )
        # Rename legacy columns if they exist.
        for old_col, new_col in [
            ("csw_record_jsonb", "metadata_jsonb"),
            ("csw_raw_xml", "xml_raw"),
        ]:
            cur.execute(f"""
                DO $$
                BEGIN
                    IF EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_schema = '{s}' AND table_name = 'layer'
                          AND column_name = '{old_col}'
                    ) AND NOT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_schema = '{s}' AND table_name = 'layer'
                          AND column_name = '{new_col}'
                    ) THEN
                        ALTER TABLE {s}.layer RENAME COLUMN {old_col} TO {new_col};
                    END IF;
                END $$;
            """)
        # Drop legacy columns.
        cur.execute(f"ALTER TABLE {s}.layer DROP COLUMN IF EXISTS csw_record_jsonb")
        cur.execute(f"ALTER TABLE {s}.layer DROP COLUMN IF EXISTS csw_raw_xml")
        cur.execute(
            f"ALTER TABLE {s}.layer DROP COLUMN IF EXISTS feature_layer_geometry_type"
        )
        cur.execute(f"ALTER TABLE {s}.layer DROP COLUMN IF EXISTS extent")
        cur.execute(f"""
            CREATE UNIQUE INDEX IF NOT EXISTS ux_layer_resource_latest
            ON {s}.layer(resource_id)
            WHERE is_latest
        """)
        cur.execute(f"""
            CREATE INDEX IF NOT EXISTS ix_layer_resource_id
            ON {s}.layer(resource_id)
        """)
        cur.execute(f"""
            CREATE INDEX IF NOT EXISTS ix_layer_metadata_gin
            ON {s}.layer USING GIN (metadata_jsonb)
        """)
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {s}.ckan_package_dependency (
                id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                subject_package_id  TEXT NOT NULL,
                object_package_id   TEXT NOT NULL,
                relationship_type   TEXT NOT NULL,
                comment             TEXT,
                synced_at           TIMESTAMPTZ NOT NULL,
                CONSTRAINT uq_ckan_dep UNIQUE (subject_package_id, object_package_id, relationship_type)
            )
        """)
        # Migration-safe: add columns that might be missing in older deployments.
        for col, col_type in [
            ("comment", "TEXT"),
        ]:
            cur.execute(
                f"ALTER TABLE {s}.ckan_package_dependency ADD COLUMN IF NOT EXISTS {col} {col_type}"
            )
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {s}.resource_run_history (
                id           UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                run_id       TEXT NOT NULL,
                package_id   TEXT NOT NULL,
                resource_id  TEXT NOT NULL,
                status       TEXT NOT NULL,
                version_num  INTEGER,
                row_count    BIGINT,
                error        TEXT,
                processed_at TIMESTAMPTZ NOT NULL
            )
        """)
        cur.execute(f"""
            CREATE INDEX IF NOT EXISTS ix_resource_run_history_resource_id
            ON {s}.resource_run_history (resource_id, processed_at DESC)
        """)
        # Migration-safe: add columns that might be missing in older deployments.
        for col, col_type in [
            ("version_num", "INTEGER"),
            ("row_count", "BIGINT"),
            ("error", "TEXT"),
        ]:
            cur.execute(
                f"ALTER TABLE {s}.resource_run_history ADD COLUMN IF NOT EXISTS {col} {col_type}"
            )
    conn.commit()


def sync_ckan_dependencies(
    conn: psycopg.Connection,
    *,
    packages: list[dict[str, Any]],
    schema: str,
    synced_at: datetime,
) -> int:
    """Upsert all CKAN package relationships into ckan_package_dependency.

    Processes both relationships_as_subject and relationships_as_object from
    every package. Deduplication is handled by the unique constraint on
    (subject_package_id, object_package_id, relationship_type).

    Returns the number of rows upserted.
    """
    s = _validate_schema(schema)
    seen: set[tuple[str, str, str]] = set()
    rows: list[tuple[str, str, str, str | None, datetime]] = []

    for pkg in packages:
        for rel_list in (
            pkg.get("relationships_as_subject") or [],
            pkg.get("relationships_as_object") or [],
        ):
            for rel in rel_list:
                if not isinstance(rel, dict):
                    continue
                subj = str(rel.get("subject") or "").strip()
                obj = str(rel.get("object") or "").strip()
                rel_type = str(rel.get("type") or "").strip()
                comment = str(rel.get("comment") or "").strip() or None
                if not (subj and obj and rel_type):
                    continue
                key = (subj, obj, rel_type)
                if key in seen:
                    continue
                seen.add(key)
                rows.append((subj, obj, rel_type, comment, synced_at))

    if not rows:
        return 0

    with conn.cursor() as cur:
        cur.executemany(
            f"""
            INSERT INTO {s}.ckan_package_dependency
                (subject_package_id, object_package_id, relationship_type, comment, synced_at)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT ON CONSTRAINT uq_ckan_dep DO UPDATE SET
                comment   = EXCLUDED.comment,
                synced_at = EXCLUDED.synced_at
            """,
            rows,
        )
    conn.commit()
    return len(rows)


def insert_resource_run_history(
    conn: psycopg.Connection,
    *,
    schema: str,
    run_id: str,
    package_id: str,
    resource_id: str,
    status: str,
    version_num: int | None,
    row_count: int | None,
    error: str | None,
    processed_at: datetime,
) -> None:
    """Append one row to resource_run_history. Never raises — failures are logged only."""
    s = _validate_schema(schema)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {s}.resource_run_history
                    (run_id, package_id, resource_id, status, version_num, row_count, error, processed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    run_id,
                    package_id,
                    resource_id,
                    status,
                    version_num,
                    row_count,
                    error[:2000] if error else None,
                    processed_at,
                ),
            )
        conn.commit()
    except Exception as exc:
        logging.getLogger(__name__).warning(
            "resource_run_history insert failed resource_id=%s: %s", resource_id, exc
        )
        try:
            conn.rollback()
        except Exception:
            pass


def _resource_signature(
    package_id: str,
    resource_id: str,
    resource_url: str,
    resource_format: str,
    resource_hash: str,
) -> str:
    raw = "|".join(
        [package_id, resource_id, resource_hash or resource_url, resource_format]
    )
    return hashlib.sha256(raw.encode()).hexdigest()


def _latest_signature(
    conn: psycopg.Connection,
    resource_id: str,
    schema: str,
) -> str | None:
    s = _validate_schema(schema)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT signature
            FROM {s}.processor_dataset_version
            WHERE resource_id = %s AND status = 'success'
            ORDER BY version_num DESC
            LIMIT 1
            """,
            (resource_id,),
        )
        row = cur.fetchone()
    return str(row[0]) if row else None


def _existing_signature(
    conn: psycopg.Connection,
    resource_id: str,
    signature: str,
    schema: str,
) -> dict[str, Any] | None:
    """Return existing processor_dataset_version row for exact signature, any status."""
    s = _validate_schema(schema)
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            f"""
            SELECT id, status, version_num, duckdb_table, row_count
            FROM {s}.processor_dataset_version
            WHERE resource_id = %s AND signature = %s
            LIMIT 1
            """,
            (resource_id, signature),
        )
        row = cur.fetchone()
    return dict(row) if row else None


def _next_version(
    conn: psycopg.Connection,
    resource_id: str,
    schema: str,
) -> int:
    s = _validate_schema(schema)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT COALESCE(MAX(version_num), 0) + 1
            FROM {s}.processor_dataset_version
            WHERE resource_id = %s
            """,
            (resource_id,),
        )
        row = cur.fetchone()
    return int(row[0]) if row else 1


def _get_existing_layer(
    conn: psycopg.Connection,
    resource_id: str,
    schema: str,
) -> dict[str, Any] | None:
    s = _validate_schema(schema)
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            f"""
            SELECT schema_name, table_name
            FROM {s}.layer
            WHERE resource_id = %s AND is_latest
            ORDER BY version_num DESC NULLS LAST
            LIMIT 1
            """,
            (resource_id,),
        )
        row = cur.fetchone()
    return dict(row) if row else None


def _get_existing_customer_layer_info(
    conn: psycopg.Connection,
    customer_schema: str,
    layer_id: uuid.UUID,
) -> dict[str, Any] | None:
    s = _validate_schema(customer_schema)
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            f"""
            SELECT
                feature_layer_geometry_type,
                CASE WHEN extent IS NOT NULL THEN ST_AsText(extent) END AS extent_wkt
            FROM {s}.layer
            WHERE id = %s
            LIMIT 1
            """,
            (layer_id,),
        )
        row = cur.fetchone()
    return dict(row) if row else None


def _normalize_extent_wkt(extent_wkt: str | None) -> str | None:
    """Convert non-WKT bbox forms (for example DuckDB BOX) to valid WKT polygon."""
    if not extent_wkt:
        return None
    text = extent_wkt.strip()
    upper = text.upper()
    if not upper.startswith("BOX("):
        return text

    m = re.match(
        r"^BOX\(\s*([-+]?\d*\.?\d+(?:[eE][-+]?\d+)?)\s+([-+]?\d*\.?\d+(?:[eE][-+]?\d+)?)\s*,\s*([-+]?\d*\.?\d+(?:[eE][-+]?\d+)?)\s+([-+]?\d*\.?\d+(?:[eE][-+]?\d+)?)\s*\)$",
        text,
    )
    if not m:
        return None

    x1, y1, x2, y2 = m.groups()
    return (
        f"POLYGON(({x1} {y1}, {x2} {y1}, {x2} {y2}, {x1} {y2}, {x1} {y1}))"
    )


def upsert_catalog_layer(
    conn: psycopg.Connection,
    *,
    schema: str,
    resource_id: str,
    layer_id: str | None,
    user_id: str,
    name: str,
    metadata: dict[str, Any],
    xml_raw: str | None,
    schema_name: str,
    table_name: str,
    version_num: int,
    create_new_version: bool,
) -> str:
    """Insert or update datacatalog.layer; returns the UUID of the row."""
    s = _validate_schema(schema)
    with conn.cursor() as cur:
        if create_new_version:
            cur.execute(
                f"UPDATE {s}.layer SET is_latest=FALSE, superseded_at=NOW() "
                f"WHERE resource_id=%s AND is_latest",
                (resource_id,),
            )
            cur.execute(
                f"""
                INSERT INTO {s}.layer (
                    layer_id, user_id, name,
                    resource_id, base_resource_id, version_num, is_latest, superseded_at,
                    metadata_jsonb, xml_raw,
                    schema_name, table_name
                ) VALUES (
                    %s, %s, %s,
                    %s, %s, %s, TRUE, NULL, %s, %s,
                    %s, %s
                )
                RETURNING id
                """,
                (
                    layer_id, user_id, name,
                    resource_id, resource_id, version_num,
                    Jsonb(metadata), xml_raw,
                    schema_name, table_name,
                ),
            )
        else:
            cur.execute(
                f"""
                UPDATE {s}.layer SET
                    layer_id=%s, user_id=%s, name=%s,
                    metadata_jsonb=%s, xml_raw=%s,
                    schema_name=%s, table_name=%s
                WHERE resource_id=%s AND is_latest
                RETURNING id
                """,
                (
                    layer_id, user_id, name,
                    Jsonb(metadata), xml_raw,
                    schema_name, table_name,
                    resource_id,
                ),
            )

        row = cur.fetchone()
        if row:
            return str(row[0])

        # First-time insert when no existing row matched.
        cur.execute(
            f"""
            INSERT INTO {s}.layer (
                layer_id, user_id, name,
                resource_id, base_resource_id, version_num, is_latest, superseded_at,
                metadata_jsonb, xml_raw,
                schema_name, table_name
            ) VALUES (
                %s, %s, %s,
                %s, %s, %s, TRUE, NULL, %s, %s,
                %s, %s
            )
            RETURNING id
            """,
            (
                layer_id, user_id, name,
                resource_id, resource_id, version_num or 1,
                Jsonb(metadata), xml_raw,
                schema_name, table_name,
            ),
        )
        inserted = cur.fetchone()
        return str(inserted[0]) if inserted else ""


def resolve_catalog_ownership(
    conn: psycopg.Connection,
    *,
    customer_schema: str,
    owner_user_id: uuid.UUID,
    folder_id: uuid.UUID,
) -> tuple[uuid.UUID, uuid.UUID, str]:
    """Resolve a valid catalog ownership pair for customer.layer writes.

    Priority:
    1. Use configured env IDs when both exist and belong together.
    2. Reuse ownership from existing in-catalog layers.
    3. Reuse a folder named "catalog".
    4. Fall back to configured IDs (will fail loudly if invalid).
    """
    s = _validate_schema(customer_schema)

    owner_exists = False
    folder_owner_id: uuid.UUID | None = None

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        try:
            cur.execute(
                f'SELECT id FROM {s}."user" WHERE id = %s LIMIT 1',
                (owner_user_id,),
            )
            owner_exists = cur.fetchone() is not None
        except Exception:
            owner_exists = False
            try:
                conn.rollback()
            except Exception:
                pass

        try:
            cur.execute(
                f"SELECT id, user_id FROM {s}.folder WHERE id = %s LIMIT 1",
                (folder_id,),
            )
            folder_row = cur.fetchone()
            if folder_row and folder_row.get("user_id"):
                folder_owner_id = uuid.UUID(str(folder_row["user_id"]))
        except Exception:
            folder_owner_id = None
            try:
                conn.rollback()
            except Exception:
                pass

        if owner_exists and folder_owner_id and folder_owner_id == owner_user_id:
            return owner_user_id, folder_id, "env"

        try:
            cur.execute(
                f"""
                SELECT user_id, folder_id
                FROM {s}.layer
                WHERE in_catalog IS TRUE
                  AND user_id IS NOT NULL
                  AND folder_id IS NOT NULL
                LIMIT 1
                """,
            )
            row = cur.fetchone()
            if row and row.get("user_id") and row.get("folder_id"):
                return (
                    uuid.UUID(str(row["user_id"])),
                    uuid.UUID(str(row["folder_id"])),
                    "existing_catalog_layer",
                )
        except Exception:
            pass
            try:
                conn.rollback()
            except Exception:
                pass

        try:
            cur.execute(
                f"""
                SELECT id, user_id
                FROM {s}.folder
                WHERE lower(name) = 'catalog'
                  AND user_id IS NOT NULL
                LIMIT 1
                """,
            )
            row = cur.fetchone()
            if row and row.get("id") and row.get("user_id"):
                return (
                    uuid.UUID(str(row["user_id"])),
                    uuid.UUID(str(row["id"])),
                    "catalog_folder",
                )
        except Exception:
            pass
            try:
                conn.rollback()
            except Exception:
                pass

    return owner_user_id, folder_id, "env_unvalidated"


# ---------------------------------------------------------------------------
# DuckLake data loading
# ---------------------------------------------------------------------------

def _map_geometry_type(raw: str | None) -> str | None:
    if not raw:
        return None
    u = raw.upper().replace("ST_", "").replace("MULTI", "")
    if "POINT" in u:
        return "point"
    if "LINE" in u or "STRING" in u:
        return "line"
    if "POLYGON" in u:
        return "polygon"
    return None


def _get_tool_runner():
    """Lazily initialise and cache a goatlib SimpleToolRunner.

    This gives us a properly configured DuckDB connection with DuckLake
    attached, S3 configured, and parquet options set — exactly matching
    the connection pattern used by all other GOAT analytics tools.
    """
    if not hasattr(_get_tool_runner, "_runner"):
        from goatlib.tools.base import SimpleToolRunner

        runner = SimpleToolRunner()
        runner.init_from_env()
        _get_tool_runner._runner = runner
    return _get_tool_runner._runner


def _get_duckdb_con():
    """Get a DuckDB connection from the cached SimpleToolRunner.

    If the cached connection is broken (e.g. previous error left it in a bad
    state), clear it so ``SimpleToolRunner.duckdb_con`` recreates it.
    """
    runner = _get_tool_runner()
    try:
        con = runner.duckdb_con
        # Quick health-check; raises if the connection is dead.
        con.execute("SELECT 1")
        return con
    except Exception:
        # Force reconnection on next access.
        runner._duckdb_con = None
        return runner.duckdb_con


def _get_ducklake_snapshot_id(
    *,
    table_schema: str,
    table_name: str,
) -> int | None:
    """Read current DuckLake snapshot id for a table, if available."""
    con = _get_duckdb_con()
    try:
        row = con.execute(
            f"""
            SELECT t.begin_snapshot
            FROM __ducklake_metadata_lake.ducklake.ducklake_table t
            JOIN __ducklake_metadata_lake.ducklake.ducklake_schema s
              ON t.schema_id = s.schema_id
            WHERE s.schema_name = '{_validate_schema(table_schema)}'
              AND t.table_name = '{table_name}'
              AND t.end_snapshot IS NULL
            """
        ).fetchone()
        if row:
            return int(row[0])
    except Exception:
        return None
    return None


def generate_pmtiles_for_table(
    *,
    table_schema: str,
    table_name: str,
    user_id: str,
    layer_id: str,
    geometry_column: str,
) -> str | None:
    """Generate PMTiles from a DuckLake table; returns output path if created."""
    runner = _get_tool_runner()
    settings = runner.settings
    if settings is None or not settings.pmtiles_enabled:
        return None

    from goatlib.io.pmtiles import PMTilesConfig, PMTilesGenerator

    config = PMTilesConfig(
        enabled=True,
        min_zoom=settings.pmtiles_min_zoom,
        max_zoom=settings.pmtiles_max_zoom,
    )
    generator = PMTilesGenerator(
        tiles_data_dir=settings.tiles_data_dir,
        config=config,
    )

    full_table_name = f"lake.{_validate_schema(table_schema)}.{table_name}"
    snapshot_id = _get_ducklake_snapshot_id(
        table_schema=table_schema,
        table_name=table_name,
    )

    pmtiles_path = generator.generate_from_table(
        duckdb_con=_get_duckdb_con(),
        table_name=full_table_name,
        user_id=user_id,
        layer_id=layer_id,
        geometry_column=geometry_column,
        snapshot_id=snapshot_id,
    )
    return str(pmtiles_path) if pmtiles_path else None


def load_into_ducklake(
    *,
    file_path: str,
    table_schema: str,
    table_name: str,
) -> tuple[int, str | None, str | None, str | None]:
    """Load a file into the DuckLake catalog.

    Returns ``(row_count, geometry_type, extent_wkt, geometry_column)``.

    Uses goatlib's ``SimpleToolRunner`` for DuckDB/DuckLake connection
    management so the ATTACH options, parquet settings, and S3 config
    are identical to those used by regular GOAT analytics tools.
    """
    con = _get_duckdb_con()

    ts = _validate_schema(table_schema)
    full_table = f"lake.{ts}.{table_name}"

    # Ensure the user schema exists in the catalog.
    con.execute(f"CREATE SCHEMA IF NOT EXISTS lake.{ts}")

    # Drop existing table for retry safety (idempotent re-runs).
    con.execute(f"DROP TABLE IF EXISTS {full_table}")

    # Detect columns: geometry for Hilbert ordering, VARCHAR for NULL cleaning.
    cols = con.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{file_path}')"
    ).fetchall()
    geom_col = None
    select_parts: list[str] = []
    for col_name, col_type, *_ in cols:
        if "GEOMETRY" in col_type.upper():
            geom_col = col_name
            select_parts.append(f'"{col_name}"')
        elif "VARCHAR" in col_type.upper():
            # Replace literal string 'NULL' with actual NULL (common in WFS data).
            select_parts.append(f'NULLIF("{col_name}", \'NULL\') AS "{col_name}"')
        else:
            select_parts.append(f'"{col_name}"')

    select_sql = ", ".join(select_parts)
    source_sql = f"SELECT {select_sql} FROM read_parquet('{file_path}')"
    order_clause = f'ORDER BY ST_Hilbert("{geom_col}")' if geom_col else ""

    # Compute stats from source query before writing into DuckLake.
    # This avoids extra metadata reads on the target table, which can fail
    # on forked environments with older DuckLake catalog schemas.
    row_count: int = con.execute(
        f"SELECT COUNT(*) FROM ({source_sql}) src"
    ).fetchone()[0]

    geometry_type: str | None = None
    extent_wkt: str | None = None
    if geom_col:
        try:
            raw_type_row = con.execute(
                f"SELECT ST_GeometryType(\"{geom_col}\") "
                f"FROM ({source_sql}) src "
                f"WHERE \"{geom_col}\" IS NOT NULL LIMIT 1"
            ).fetchone()
            if raw_type_row:
                geometry_type = _map_geometry_type(str(raw_type_row[0]))

            bbox_row = con.execute(
                f"SELECT ST_AsText(ST_Extent(\"{geom_col}\")) FROM ({source_sql}) src"
            ).fetchone()
            if bbox_row and bbox_row[0]:
                extent_wkt = str(bbox_row[0])
        except Exception:
            pass

    # Create registered DuckLake table from the converted parquet file.
    con.execute(f"""
        CREATE TABLE {full_table} AS
        {source_sql}
        {order_clause}
    """)

    return row_count, geometry_type, extent_wkt, geom_col


def _download_resource(url: str) -> str:
    """Download a URL to a temp file; returns its path."""
    parsed = urlparse(url)
    filename = parsed.path.rsplit("/", 1)[-1]
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    # Keep suffix conservative to avoid invalid temporary file paths.
    suffix = f".{ext}" if re.fullmatch(r"[a-z0-9]{1,10}", ext) else ".bin"
    with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
        tmp_path = tmp.name

    with httpx.Client(timeout=120, follow_redirects=True) as client:
        with client.stream("GET", url) as resp:
            resp.raise_for_status()
            # If URL has no useful extension, infer from content type.
            if suffix == ".bin":
                ctype = str(resp.headers.get("content-type") or "").lower()
                if "json" in ctype:
                    new_suffix = ".geojson"
                elif "xml" in ctype or "gml" in ctype:
                    new_suffix = ".xml"
                elif "zip" in ctype:
                    new_suffix = ".zip"
                else:
                    new_suffix = ""
                if new_suffix:
                    base_path, _ = os.path.splitext(tmp_path)
                    new_path = f"{base_path}{new_suffix}"
                    os.replace(tmp_path, new_path)
                    tmp_path = new_path
            with open(tmp_path, "wb") as fh:
                for chunk in resp.iter_bytes(1024 * 128):
                    fh.write(chunk)

    return tmp_path


def _build_wfs_getfeature_targets(url: str) -> list[tuple[str, str | None]]:
    """Return list of (download_url, typename) for WFS resources.

    - For GetCapabilities, returns all feature types as individual GetFeature URLs.
    - For other URLs, returns the original URL and optional typename if present.
    """
    parsed = urlparse(url)
    params = {k.lower(): v for k, v in parse_qsl(parsed.query, keep_blank_values=True)}
    if params.get("service", "").lower() != "wfs":
        return [(url, None)]

    request = params.get("request", "").lower()
    version = params.get("version") or "2.0.0"

    if request == "getcapabilities":
        with httpx.Client(timeout=60, follow_redirects=True) as client:
            r = client.get(url)
            r.raise_for_status()
            root = ET.fromstring(r.content)

        ns = {
            "wfs": "http://www.opengis.net/wfs",
            "wfs2": "http://www.opengis.net/wfs/2.0",
        }
        typenames: list[str] = []
        for xpath in (
            ".//wfs:FeatureType/wfs:Name",
            ".//wfs2:FeatureType/wfs2:Name",
        ):
            for node in root.findall(xpath, ns):
                name = (node.text or "").strip()
                if name and name not in typenames:
                    typenames.append(name)

        if not typenames:
            return [(url, None)]

        targets: list[tuple[str, str | None]] = []
        for typename in typenames:
            query = urlencode(
                {
                    "service": "WFS",
                    "version": version,
                    "request": "GetFeature",
                    "typeNames": typename,
                    "outputFormat": "application/json",
                    "srsName": "EPSG:4326",
                }
            )
            targets.append(
                (urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", query, "")), typename)
            )
        return targets

    typename_param = params.get("typenames") or params.get("typename")
    if typename_param:
        typenames = [t.strip() for t in typename_param.split(",") if t.strip()]
        if typenames:
            return [(url, typenames[0])]

    return [(url, None)]


def _to_parquet(src_path: str) -> tuple[str, str]:
    """Convert spatial file to GOAT-standard parquet via goatlib.

    Returns (parquet_path, temp_dir_to_cleanup).
    Falls back to src_path unchanged when conversion fails.
    """
    if src_path.endswith(".parquet"):
        return src_path, ""

    try:
        from goatlib.io.ingest import convert_any

        convert_dir = tempfile.mkdtemp(prefix="ckan_cvt_")
        outputs = convert_any(
            src_path=src_path, dest_dir=convert_dir, target_crs="EPSG:4326"
        )
        if outputs:
            out_path, _ = outputs[0]
            return str(out_path), convert_dir
    except Exception as exc:
        logging.getLogger(__name__).warning(
            "goatlib conversion failed (%s); sending raw file to DuckLake", exc
        )

    return src_path, ""


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def main() -> dict[str, Any]:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    log = logging.getLogger(__name__)

    # ---- Resolve config ----------------------------------------------------
    api_key = _resolve_ckan_api_key()
    ckan_base, package_search_url, health_url = _resolve_ckan_urls(api_key)
    page_size = int(_env("CKAN_API_PAGE_SIZE", "200"))
    max_pages = int(_env("CKAN_API_MAX_PAGES", "100"))

    pg_host = _require("META_PG_HOST", "POSTGRES_SERVER")
    pg_port = int(_env("META_PG_PORT", _env("POSTGRES_PORT", "5432")))
    pg_db = _require("META_PG_DB", "POSTGRES_DB")
    pg_user = _require("META_PG_USER", "POSTGRES_USER")
    pg_password = _env("META_PG_PASSWORD", _env("POSTGRES_PASSWORD", ""))
    catalog_schema = _env("META_PG_SCHEMA", "datacatalog")
    customer_schema = _env("CUSTOMER_SCHEMA", "customer")
    catalog_owner_user_id = _parse_uuid(
        _env("CATALOG_OWNER_USER_ID", "744e4fd1-685c-495c-8b02-efebce875359"),
        "CATALOG_OWNER_USER_ID",
    )
    catalog_folder_id = _parse_uuid(
        _env("CATALOG_FOLDER_ID", "8526b040-082a-471f-b8ea-f11321e5b33a"),
        "CATALOG_FOLDER_ID",
    )
    catalog_folder_name = "catalog"

    # DuckLake data dir for path bookkeeping in metadata tables.
    # Keep this canonical so downstream services don't attempt host-local paths.
    ducklake_data_dir = _canonical_ducklake_metadata_dir(
        _env("DUCKLAKE_DATA_DIR", "/app/data/ducklake")
    )

    # ---- CKAN health check -------------------------------------------------
    health = check_ckan_health(health_url, api_key)
    if not health["ready"]:
        return {
            "status": "error",
            "reason": "ckan_not_ready",
            "health": health,
        }
    log.info("CKAN ready at %s", health_url)
    log.info("CKAN API key configured: %s", "yes" if api_key else "no (unauthenticated — private datasets will be hidden)")

    # ---- Fetch packages from CKAN API -------------------------------------
    log.info("Fetching packages from %s", package_search_url)
    packages = fetch_ckan_packages(
        package_search_url, api_key, page_size=page_size, max_pages=max_pages
    )
    log.info("Fetched %d packages", len(packages))
    sync_run_at = datetime.now(timezone.utc)

    # ---- Connect to PostgreSQL and set up schema --------------------------
    pg_conn = psycopg.connect(
        host=pg_host,
        port=pg_port,
        dbname=pg_db,
        user=pg_user,
        password=pg_password or None,
        autocommit=False,
    )
    ensure_catalog_schema(pg_conn, catalog_schema)

    (
        catalog_owner_user_id,
        catalog_folder_id,
        ownership_source,
    ) = resolve_catalog_ownership(
        pg_conn,
        customer_schema=customer_schema,
        owner_user_id=catalog_owner_user_id,
        folder_id=catalog_folder_id,
    )

    log.info(
        "Catalog ownership resolved source=%s user_id=%s folder_id=%s",
        ownership_source,
        catalog_owner_user_id,
        catalog_folder_id,
    )

    dependencies_synced = sync_ckan_dependencies(
        pg_conn,
        packages=packages,
        schema=catalog_schema,
        synced_at=sync_run_at,
    )
    log.info("Synced %d CKAN package dependencies", dependencies_synced)

    run_id = str(uuid.uuid4())
    processed = skipped = failed = candidates = 0
    harvest_xml_cache: dict[str, str | None] = {}

    for package in packages:
        if str(package.get("state") or "active").lower() != "active":
            continue

        package_id = str(package.get("id") or "")
        if package_id not in harvest_xml_cache:
            harvest_xml_cache[package_id] = _fetch_latest_harvest_xml(package_id)
        package_xml_metadata = harvest_xml_cache.get(package_id)
        resources: list[dict[str, Any]] = package.get("resources") or []

        for resource in resources:
            if not isinstance(resource, dict):
                continue
            if str(resource.get("state") or "active").lower() != "active":
                continue
            if not is_spatial_resource(resource):
                continue

            resource_id = str(resource.get("id") or "")
            resource_url = str(resource.get("url") or "")
            resource_format = str(resource.get("format") or "")
            resource_hash = str(resource.get("hash") or "")

            try:
                wfs_targets = _build_wfs_getfeature_targets(resource_url)
            except Exception as exc:
                log.warning(
                    "resource_id=%s URL normalization failed (%s); using original URL",
                    resource_id,
                    exc,
                )
                wfs_targets = [(resource_url, None)]

            log.info("resource_id=%s resolved targets=%d", resource_id, len(wfs_targets))

            base_metadata = build_ckan_metadata(package, resource, package_xml_metadata)
            raw_xml_metadata = package_xml_metadata
            xml_metadata = _build_customer_xml_metadata(raw_xml_metadata, base_metadata)

            for resource_download_url, typename in wfs_targets:
                candidates += 1
                # Clear any stale failed transaction state from previous iterations.
                try:
                    pg_conn.rollback()
                except Exception:
                    pass
                effective_resource_id = (
                    f"{resource_id}::{typename}" if typename else resource_id
                )
                customer_layer_id = _catalog_layer_uuid(effective_resource_id)
                customer_layer_id_no_dash = str(customer_layer_id).replace("-", "")
                layer_suffix = f" [{typename}]" if typename else ""
                layer_name = str(base_metadata.get("title") or resource_id) + layer_suffix

                sig = _resource_signature(
                    package_id,
                    effective_resource_id,
                    resource_download_url,
                    resource_format,
                    resource_hash,
                )
                existing_sig = _existing_signature(
                    pg_conn,
                    effective_resource_id,
                    sig,
                    catalog_schema,
                )
                if existing_sig:
                    existing_status = str(existing_sig.get("status") or "").lower()
                    if existing_status != "success":
                        log.info(
                            "resource_id=%s retrying previous %s signature",
                            effective_resource_id,
                            existing_status or "unknown",
                        )
                    else:
                        existing = _get_existing_layer(
                            pg_conn, effective_resource_id, catalog_schema
                        )
                        if existing:
                            existing_schema_name = (
                                f"user_{str(catalog_owner_user_id).replace('-', '')}"
                            )
                            existing_table_name = f"t_{customer_layer_id_no_dash}"
                            existing_customer = _get_existing_customer_layer_info(
                                pg_conn,
                                customer_schema,
                                customer_layer_id,
                            )

                            upsert_customer_catalog_layer(
                                pg_conn,
                                customer_schema=customer_schema,
                                owner_user_id=catalog_owner_user_id,
                                folder_id=catalog_folder_id,
                                layer_id=customer_layer_id,
                                layer_name=layer_name,
                                geometry_type=(
                                    existing_customer or {}
                                ).get("feature_layer_geometry_type"),
                                extent_wkt=(existing_customer or {}).get("extent_wkt"),
                                resource_id=effective_resource_id,
                                xml_metadata=xml_metadata,
                                metadata=base_metadata,
                            )
                            upsert_catalog_layer(
                                pg_conn,
                                schema=catalog_schema,
                                resource_id=effective_resource_id,
                                layer_id=str(customer_layer_id),
                                user_id=str(catalog_owner_user_id),
                                name=layer_name,
                                metadata=base_metadata,
                                xml_raw=raw_xml_metadata,
                                schema_name=existing_schema_name,
                                table_name=existing_table_name,
                                version_num=int(existing_sig.get("version_num") or 1),
                                create_new_version=False,
                            )
                            with pg_conn.cursor() as cur:
                                cur.execute(
                                    f"""
                                    UPDATE {_validate_schema(catalog_schema)}.processor_dataset_version
                                    SET
                                        duckdb_path = %s,
                                        duckdb_table = %s,
                                        processed_at = %s
                                    WHERE resource_id = %s AND signature = %s
                                    """,
                                    (
                                        os.path.join(
                                            ducklake_data_dir,
                                            existing_schema_name,
                                            f"{existing_table_name}.parquet",
                                        ),
                                        existing_table_name,
                                        datetime.now(timezone.utc),
                                        effective_resource_id,
                                        sig,
                                    ),
                                )
                        pg_conn.commit()
                        skipped += 1
                        log.info(
                            "resource_id=%s skipped duplicate signature (status=%s)",
                            effective_resource_id,
                            existing_sig.get("status"),
                        )
                        insert_resource_run_history(
                            pg_conn,
                            schema=catalog_schema,
                            run_id=run_id,
                            package_id=package_id,
                            resource_id=effective_resource_id,
                            status="skipped",
                            version_num=int(existing_sig.get("version_num") or 0) or None,
                            row_count=int(existing_sig.get("row_count") or 0) or None,
                            error=None,
                            processed_at=datetime.now(timezone.utc),
                        )
                        continue

                reuse_failed = bool(existing_sig) and str(
                    existing_sig.get("status") or ""
                ).lower() != "success"
                if reuse_failed:
                    version_num = int(existing_sig.get("version_num") or 1)
                    version_id = str(existing_sig.get("id") or uuid.uuid4())
                else:
                    version_num = _next_version(
                        pg_conn, effective_resource_id, catalog_schema
                    )
                    version_id = str(uuid.uuid4())

                target_table_schema = f"user_{str(catalog_owner_user_id).replace('-', '')}"
                target_table_name = f"t_{customer_layer_id_no_dash}"
                target_table_path = os.path.join(
                    ducklake_data_dir,
                    target_table_schema,
                    f"{target_table_name}.parquet",
                )

                processed_at = datetime.now(timezone.utc)
                tmp_path: str | None = None
                convert_dir: str | None = None

                try:
                    log.info(
                        "resource_id=%s downloading from %s",
                        effective_resource_id,
                        resource_download_url,
                    )
                    tmp_path = _download_resource(resource_download_url)
                    parquet_path, convert_dir = _to_parquet(tmp_path)
                    if parquet_path == tmp_path:
                        skipped += 1
                        log.warning(
                            "resource_id=%s skipped: not a convertible spatial file",
                            effective_resource_id,
                        )
                        insert_resource_run_history(
                            pg_conn,
                            schema=catalog_schema,
                            run_id=run_id,
                            package_id=package_id,
                            resource_id=effective_resource_id,
                            status="skipped",
                            version_num=None,
                            row_count=None,
                            error="not a convertible spatial file",
                            processed_at=datetime.now(timezone.utc),
                        )
                        continue

                    row_count, geometry_type, extent_wkt, geometry_column = load_into_ducklake(
                        file_path=parquet_path,
                        table_schema=target_table_schema,
                        table_name=target_table_name,
                    )
                    if geometry_type is None:
                        skipped += 1
                        log.warning(
                            "resource_id=%s skipped: geometry type could not be detected",
                            effective_resource_id,
                        )
                        # Non-spatial/tabular datasets should not be published as feature layers.
                        _get_duckdb_con().execute(
                            f"DROP TABLE IF EXISTS lake.{_validate_schema(target_table_schema)}.{target_table_name}"
                        )
                        insert_resource_run_history(
                            pg_conn,
                            schema=catalog_schema,
                            run_id=run_id,
                            package_id=package_id,
                            resource_id=effective_resource_id,
                            status="skipped",
                            version_num=None,
                            row_count=row_count,
                            error="geometry type could not be detected",
                            processed_at=datetime.now(timezone.utc),
                        )
                        continue
                    log.info(
                        "resource_id=%s loaded table=%s rows=%d",
                        effective_resource_id,
                        f"{target_table_schema}.{target_table_name}",
                        row_count,
                    )

                    try:
                        pmtiles_path = generate_pmtiles_for_table(
                            table_schema=target_table_schema,
                            table_name=target_table_name,
                            user_id=str(catalog_owner_user_id),
                            layer_id=str(customer_layer_id),
                            geometry_column=geometry_column or "geometry",
                        )
                        if pmtiles_path:
                            log.info(
                                "resource_id=%s pmtiles generated at %s",
                                effective_resource_id,
                                pmtiles_path,
                            )
                    except Exception as pmtiles_exc:
                        # PMTiles are an optimization; failures should not block ingestion.
                        log.warning(
                            "resource_id=%s PMTiles generation failed (non-fatal): %s",
                            effective_resource_id,
                            pmtiles_exc,
                        )

                    with pg_conn.cursor() as cur:
                        cur.execute(
                            f"""
                            INSERT INTO {_validate_schema(catalog_schema)}.processor_dataset_version (
                                id, run_id, package_id, resource_id, signature, version_num,
                                duckdb_path, duckdb_table, row_count, status, error, processed_at
                            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'success',NULL,%s)
                            ON CONFLICT (resource_id, signature) DO UPDATE SET
                                run_id = EXCLUDED.run_id,
                                package_id = EXCLUDED.package_id,
                                version_num = EXCLUDED.version_num,
                                duckdb_path = EXCLUDED.duckdb_path,
                                duckdb_table = EXCLUDED.duckdb_table,
                                row_count = EXCLUDED.row_count,
                                status = 'success',
                                error = NULL,
                                processed_at = EXCLUDED.processed_at
                            RETURNING id
                            """,
                            (
                                version_id,
                                run_id,
                                package_id,
                                effective_resource_id,
                                sig,
                                version_num,
                                target_table_path,
                                target_table_name,
                                row_count,
                                processed_at,
                            ),
                        )
                        inserted = cur.fetchone()

                    if not inserted:
                        raise RuntimeError(
                            "processor_dataset_version upsert did not return row"
                        )

                    upsert_customer_catalog_layer(
                        pg_conn,
                        customer_schema=customer_schema,
                        owner_user_id=catalog_owner_user_id,
                        folder_id=catalog_folder_id,
                        layer_id=customer_layer_id,
                        layer_name=layer_name,
                        geometry_type=geometry_type,
                        extent_wkt=extent_wkt,
                        resource_id=effective_resource_id,
                        xml_metadata=xml_metadata,
                        metadata=base_metadata,
                    )
                    upsert_catalog_layer(
                        pg_conn,
                        schema=catalog_schema,
                        resource_id=effective_resource_id,
                        layer_id=str(customer_layer_id),
                        user_id=str(catalog_owner_user_id),
                        name=layer_name,
                        metadata=base_metadata,
                        xml_raw=raw_xml_metadata,
                        schema_name=target_table_schema,
                        table_name=target_table_name,
                        version_num=version_num,
                        create_new_version=True,
                    )
                    pg_conn.commit()
                    processed += 1
                    log.info(
                        "resource_id=%s processed version=%d",
                        effective_resource_id,
                        version_num,
                    )
                    insert_resource_run_history(
                        pg_conn,
                        schema=catalog_schema,
                        run_id=run_id,
                        package_id=package_id,
                        resource_id=effective_resource_id,
                        status="success",
                        version_num=version_num,
                        row_count=row_count,
                        error=None,
                        processed_at=processed_at,
                    )

                except Exception as exc:
                    failed += 1
                    log.exception(
                        "resource_id=%s failed version=%d: %s",
                        effective_resource_id,
                        version_num,
                        exc,
                    )
                    pg_conn.rollback()
                    try:
                        with pg_conn.cursor() as cur:
                            cur.execute(
                                f"""
                                INSERT INTO {_validate_schema(catalog_schema)}.processor_dataset_version (
                                    id, run_id, package_id, resource_id, signature, version_num,
                                    duckdb_path, duckdb_table, row_count, status, error, processed_at
                                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,NULL,'failed',%s,%s)
                                ON CONFLICT (resource_id, signature) DO UPDATE SET
                                    run_id = EXCLUDED.run_id,
                                    package_id = EXCLUDED.package_id,
                                    version_num = EXCLUDED.version_num,
                                    duckdb_path = EXCLUDED.duckdb_path,
                                    duckdb_table = EXCLUDED.duckdb_table,
                                    status = 'failed',
                                    error = EXCLUDED.error,
                                    processed_at = EXCLUDED.processed_at
                                """,
                                (
                                    version_id,
                                    run_id,
                                    package_id,
                                    effective_resource_id,
                                    sig,
                                    version_num,
                                    target_table_path,
                                    target_table_name,
                                    str(exc)[:2000],
                                    processed_at,
                                ),
                            )
                        pg_conn.commit()
                    except Exception:
                        pg_conn.rollback()
                    insert_resource_run_history(
                        pg_conn,
                        schema=catalog_schema,
                        run_id=run_id,
                        package_id=package_id,
                        resource_id=effective_resource_id,
                        status="failed",
                        version_num=version_num,
                        row_count=None,
                        error=str(exc)[:2000],
                        processed_at=processed_at,
                    )

                finally:
                    if tmp_path and os.path.exists(tmp_path):
                        os.unlink(tmp_path)
                    if convert_dir and os.path.isdir(convert_dir):
                        import shutil
                        shutil.rmtree(convert_dir, ignore_errors=True)

    pg_conn.close()

    result = {
        "status": "ok",
        "run_id": run_id,
        "candidates": candidates,
        "processed": processed,
        "skipped": skipped,
        "failed": failed,
        "dependencies_synced": dependencies_synced,
        "catalog_owner_user_id": str(catalog_owner_user_id),
        "catalog_folder_name": catalog_folder_name,
    }
    log.info("Pipeline complete: %s", result)
    return result


if __name__ == "__main__":
    print(json.dumps(main(), indent=2))
