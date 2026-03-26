"""Dependencies for Processes API."""

import logging
from dataclasses import dataclass
from uuid import UUID

from fastapi import HTTPException
from goatlib.utils.layer import (
    InvalidLayerIdError,
    LayerNotFoundError,
    layer_id_to_table_name,
)
from goatlib.utils.layer import (
    get_schema_for_layer as _goatlib_get_schema_for_layer,
)
from goatlib.utils.layer import (
    normalize_layer_id as _goatlib_normalize_layer_id,
)
from psycopg import connect
from psycopg.rows import dict_row

from processes.config import settings
from processes.ducklake import ducklake_manager

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LayerLocation:
    """Resolved DuckLake location for a layer."""

    layer_id: str
    schema_name: str
    table_name: str


def normalize_layer_id(layer_id: str) -> str:
    """Normalize layer ID to standard UUID format with hyphens.

    Accepts:
    - 32-char hex: abc123def456...
    - UUID format: abc123de-f456-...

    Returns:
        Standard UUID format (lowercase, with hyphens)

    Raises:
        HTTPException: If layer ID is invalid
    """
    try:
        return _goatlib_normalize_layer_id(layer_id)
    except InvalidLayerIdError as e:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid collection ID: {e.layer_id}. Expected UUID format.",
        )


def get_schema_for_layer(layer_id: str) -> str:
    """Get schema name for a layer ID, with caching.

    Queries DuckDB's information_schema for the attached DuckLake catalog.

    Args:
        layer_id: Normalized layer ID (UUID format with hyphens)

    Returns:
        Schema name (e.g., 'user_abc123...')

    Raises:
        HTTPException: If layer not found
    """
    location = get_layer_location(layer_id)
    return location.schema_name


def get_layer_location(layer_id: str):
    normalized_layer_id = normalize_layer_id(layer_id)

    customer_location = _lookup_customer_layer(normalized_layer_id)
    if customer_location:
        return customer_location

    catalog_location = _lookup_catalog_layer(normalized_layer_id)
    if catalog_location:
        return catalog_location

    # Legacy fallback: discover by standard table naming in DuckLake.
    try:
        schema_name = _goatlib_get_schema_for_layer(
            normalized_layer_id,
            ducklake_manager,
        )
        return LayerLocation(
            layer_id=normalized_layer_id,
            schema_name=schema_name,
            table_name=layer_id_to_table_name(normalized_layer_id),
        )
    except LayerNotFoundError:
        pass

    raise HTTPException(
        status_code=404,
        detail=f"Layer not found: {normalized_layer_id}",
    )


def _lookup_customer_layer(layer_id: str) -> LayerLocation | None:
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
                cur.execute(
                    f"""
                    SELECT
                        user_id,
                        in_catalog,
                        other_properties->'canonical_pointer'->>'duckdb_schema' AS pointer_schema,
                        other_properties->'canonical_pointer'->>'duckdb_table' AS pointer_table
                    FROM {settings.CUSTOMER_SCHEMA}.layer
                    WHERE id = %s
                    """,
                    (UUID(layer_id),),
                )
                row = cur.fetchone()
                if not row:
                    return None

                if bool(row.get("in_catalog")):
                    pointer_schema = row.get("pointer_schema")
                    pointer_table = row.get("pointer_table")
                    if not pointer_schema or not pointer_table:
                        return None
                    return LayerLocation(
                        layer_id=layer_id,
                        schema_name=str(pointer_schema),
                        table_name=str(pointer_table),
                    )

                user_id = row.get("user_id")
                if not user_id:
                    return None
                return LayerLocation(
                    layer_id=layer_id,
                    schema_name=f"user_{str(user_id).replace('-', '')}",
                    table_name=layer_id_to_table_name(layer_id),
                )
    except Exception as exc:
        logger.debug("customer.layer lookup failed: %s", exc)
        return None


def _lookup_catalog_layer(layer_id: str) -> LayerLocation | None:
    datacatalog_schema = getattr(settings, "DATACATALOG_SCHEMA", "datacatalog")
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
                cur.execute(
                    f"SELECT schema_name, table_name FROM {datacatalog_schema}.layer WHERE id = %s",
                    (UUID(layer_id),),
                )
                row = cur.fetchone()
                if not row:
                    return None
                return LayerLocation(
                    layer_id=layer_id,
                    schema_name=str(row["schema_name"]),
                    table_name=str(row["table_name"]),
                )
    except Exception as exc:
        logger.debug("datacatalog.layer lookup failed: %s", exc)
        return None
