"""Feature write service for DuckLake mutations.

Handles feature CRUD (create, update, delete) and column management
(add, rename, delete columns) via the write-capable DuckLake manager.

All writes are serialized by BaseDuckLakeManager's internal threading.Lock.
"""

import json
import logging
import uuid
from typing import Any, Optional

from geoapi.config import settings
from geoapi.dependencies import LayerInfo
from geoapi.ducklake_write import ducklake_write_manager
from geoapi.models.write import COLUMN_TYPE_MAP

logger = logging.getLogger(__name__)

# Columns that cannot be modified by users
PROTECTED_COLUMNS = {"id", "geometry", "geom", "rowid"}

# Integer types where we should NOT insert a UUID string
_INTEGER_TYPES = {"INTEGER", "INT", "INT4", "INT32", "BIGINT", "INT8", "INT64",
                  "SMALLINT", "INT2", "INT16", "TINYINT", "INT1", "HUGEINT"}


def _validate_column_name(name: str, existing_columns: list[str]) -> None:
    """Validate that a column name is safe and exists."""
    if name in PROTECTED_COLUMNS:
        raise ValueError(f"Column '{name}' is protected and cannot be modified")
    if name in settings.HIDDEN_FIELDS:
        raise ValueError(f"Column '{name}' is a system field")
    if name not in existing_columns:
        raise ValueError(f"Column '{name}' does not exist")


def _validate_new_column_name(name: str, existing_columns: list[str]) -> None:
    """Validate that a new column name is safe and doesn't conflict."""
    if name in PROTECTED_COLUMNS:
        raise ValueError(f"Column name '{name}' is reserved")
    if name in settings.HIDDEN_FIELDS:
        raise ValueError(f"Column name '{name}' conflicts with system fields")
    if name in existing_columns:
        raise ValueError(f"Column '{name}' already exists")


def _resolve_duckdb_type(type_name: str) -> str:
    """Resolve a user-friendly type name to a DuckDB type."""
    type_lower = type_name.lower().strip()
    if type_lower not in COLUMN_TYPE_MAP:
        raise ValueError(
            f"Invalid column type: '{type_name}'. "
            f"Valid types: {', '.join(COLUMN_TYPE_MAP.keys())}"
        )
    return COLUMN_TYPE_MAP[type_lower]


class FeatureWriteService:
    """Service for writing features and managing columns in DuckLake."""

    # --- Feature CRUD ---

    def create_feature(
        self,
        layer_info: LayerInfo,
        geometry: Optional[dict[str, Any]],
        properties: dict[str, Any],
        column_names: list[str],
        geometry_column: Optional[str] = None,
        column_types: Optional[dict[str, str]] = None,
    ) -> str:
        """Create a new feature.

        Args:
            layer_info: Layer info with table name
            geometry: GeoJSON geometry dict or None
            properties: Feature properties
            column_names: Known column names for validation
            geometry_column: Geometry column name
            column_types: Column name -> data_type mapping

        Returns:
            New feature ID
        """
        table = layer_info.full_table_name
        has_id_column = "id" in column_names
        id_is_integer = (
            column_types is not None
            and column_types.get("id", "").upper() in _INTEGER_TYPES
        )

        # Build column list and values
        columns: list[str] = []
        placeholders: list[str] = []
        values: list[Any] = []

        with ducklake_write_manager.connection() as con:
            if has_id_column:
                if id_is_integer:
                    # Generate next integer ID
                    max_result = con.execute(
                        f'SELECT COALESCE(MAX("id"), 0) + 1 FROM {table}'
                    ).fetchone()
                    feature_id = str(max_result[0]) if max_result else "1"
                    columns.append('"id"')
                    placeholders.append("?")
                    values.append(int(feature_id))
                else:
                    feature_id = str(uuid.uuid4())
                    columns.append('"id"')
                    placeholders.append("?")
                    values.append(feature_id)
            else:
                feature_id = str(uuid.uuid4())

            # Add geometry if present
            geom_json = None
            if geometry and geometry_column:
                geom_json = json.dumps(geometry)
                columns.append(f'"{geometry_column}"')
                placeholders.append("ST_GeomFromGeoJSON(?)")
                values.append(geom_json)

                # Compute bbox struct if table has a bbox column
                if "bbox" in column_names:
                    columns.append('"bbox"')
                    placeholders.append(
                        "struct_pack("
                        "xmin := ST_XMin(ST_GeomFromGeoJSON(?)), "
                        "ymin := ST_YMin(ST_GeomFromGeoJSON(?)), "
                        "xmax := ST_XMax(ST_GeomFromGeoJSON(?)), "
                        "ymax := ST_YMax(ST_GeomFromGeoJSON(?)))"
                    )
                    values.extend([geom_json, geom_json, geom_json, geom_json])

            # Add properties (only known columns)
            for col_name, col_value in properties.items():
                if col_name in column_names and col_name not in PROTECTED_COLUMNS:
                    columns.append(f'"{col_name}"')
                    placeholders.append("?")
                    values.append(col_value)

            columns_str = ", ".join(columns)
            placeholders_str = ", ".join(placeholders)

            query = f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders_str})"
            logger.debug("Create feature: %s", query)
            con.execute(query, values)

        return feature_id

    def create_features_bulk(
        self,
        layer_info: LayerInfo,
        features: list[dict[str, Any]],
        column_names: list[str],
        geometry_column: Optional[str] = None,
        column_types: Optional[dict[str, str]] = None,
    ) -> list[str]:
        """Create multiple features in a single transaction.

        Args:
            layer_info: Layer info with table name
            features: List of {geometry, properties} dicts
            column_names: Known column names for validation
            geometry_column: Geometry column name
            column_types: Column name -> data_type mapping

        Returns:
            List of new feature IDs
        """
        table = layer_info.full_table_name
        feature_ids: list[str] = []
        has_id_column = "id" in column_names
        id_is_integer = (
            column_types is not None
            and column_types.get("id", "").upper() in _INTEGER_TYPES
        )

        with ducklake_write_manager.connection() as con:
            # For integer id columns, get the current max to generate sequential IDs
            next_int_id = 1
            if has_id_column and id_is_integer:
                max_result = con.execute(
                    f'SELECT COALESCE(MAX("id"), 0) FROM {table}'
                ).fetchone()
                next_int_id = (max_result[0] if max_result else 0) + 1

            for feature_data in features:
                geometry = feature_data.get("geometry")
                properties = feature_data.get("properties", {})

                columns: list[str] = []
                placeholders: list[str] = []
                values: list[Any] = []

                if has_id_column:
                    if id_is_integer:
                        feature_id = str(next_int_id)
                        columns.append('"id"')
                        placeholders.append("?")
                        values.append(next_int_id)
                        next_int_id += 1
                    else:
                        feature_id = str(uuid.uuid4())
                        columns.append('"id"')
                        placeholders.append("?")
                        values.append(feature_id)
                else:
                    feature_id = str(uuid.uuid4())
                feature_ids.append(feature_id)

                geom_json = None
                if geometry and geometry_column:
                    geom_json = json.dumps(geometry)
                    columns.append(f'"{geometry_column}"')
                    placeholders.append("ST_GeomFromGeoJSON(?)")
                    values.append(geom_json)

                    if "bbox" in column_names:
                        columns.append('"bbox"')
                        placeholders.append(
                            "struct_pack("
                            "xmin := ST_XMin(ST_GeomFromGeoJSON(?)), "
                            "ymin := ST_YMin(ST_GeomFromGeoJSON(?)), "
                            "xmax := ST_XMax(ST_GeomFromGeoJSON(?)), "
                            "ymax := ST_YMax(ST_GeomFromGeoJSON(?)))"
                        )
                        values.extend([geom_json, geom_json, geom_json, geom_json])

                for col_name, col_value in properties.items():
                    if col_name in column_names and col_name not in PROTECTED_COLUMNS:
                        columns.append(f'"{col_name}"')
                        placeholders.append("?")
                        values.append(col_value)

                columns_str = ", ".join(columns)
                placeholders_str = ", ".join(placeholders)
                query = f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders_str})"
                con.execute(query, values)

        return feature_ids

    def update_feature_properties(
        self,
        layer_info: LayerInfo,
        feature_id: str,
        properties: dict[str, Any],
        column_names: list[str],
    ) -> bool:
        """Update feature properties (partial update).

        Args:
            layer_info: Layer info with table name
            feature_id: Feature ID
            properties: Properties to update
            column_names: Known column names for validation

        Returns:
            True if feature was found and updated
        """
        table = layer_info.full_table_name

        # Filter to only known, non-protected columns
        safe_props = {
            k: v for k, v in properties.items()
            if k in column_names and k not in PROTECTED_COLUMNS
        }
        if not safe_props:
            raise ValueError("No valid properties to update")

        set_clauses = []
        values: list[Any] = []
        for col_name, col_value in safe_props.items():
            set_clauses.append(f'"{col_name}" = ?')
            values.append(col_value)

        has_id_col = "id" in column_names
        id_filter = '"id" = ?' if has_id_col else 'rowid = ?'
        values.append(int(feature_id) if not has_id_col else feature_id)
        set_str = ", ".join(set_clauses)
        query = f'UPDATE {table} SET {set_str} WHERE {id_filter}'
        logger.debug("Update feature: %s", query)

        with ducklake_write_manager.connection() as con:
            con.execute(query, values)
            count_result = con.execute(
                f'SELECT COUNT(*) FROM {table} WHERE {id_filter}', [int(feature_id) if not has_id_col else feature_id]
            ).fetchone()
            return count_result is not None and count_result[0] > 0

    def replace_feature(
        self,
        layer_info: LayerInfo,
        feature_id: str,
        geometry: Optional[dict[str, Any]],
        properties: dict[str, Any],
        column_names: list[str],
        geometry_column: Optional[str] = None,
    ) -> bool:
        """Replace a feature entirely (geometry + properties).

        Args:
            layer_info: Layer info with table name
            feature_id: Feature ID
            geometry: New GeoJSON geometry or None
            properties: New properties (replaces all)
            column_names: Known column names for validation
            geometry_column: Geometry column name

        Returns:
            True if feature was found and replaced
        """
        table = layer_info.full_table_name

        set_clauses = []
        values: list[Any] = []

        # Update geometry if provided
        if geometry and geometry_column:
            set_clauses.append(f'"{geometry_column}" = ST_GeomFromGeoJSON(?)')
            values.append(json.dumps(geometry))

        # Update all properties
        for col_name, col_value in properties.items():
            if col_name in column_names and col_name not in PROTECTED_COLUMNS:
                set_clauses.append(f'"{col_name}" = ?')
                values.append(col_value)

        if not set_clauses:
            raise ValueError("No valid fields to update")

        # Also update bbox if geometry changed and table has bbox column
        if geometry and geometry_column and "bbox" in column_names:
            geom_json = json.dumps(geometry)
            set_clauses.append(
                '"bbox" = struct_pack('
                'xmin := ST_XMin(ST_GeomFromGeoJSON(?)), '
                'ymin := ST_YMin(ST_GeomFromGeoJSON(?)), '
                'xmax := ST_XMax(ST_GeomFromGeoJSON(?)), '
                'ymax := ST_YMax(ST_GeomFromGeoJSON(?)))'
            )
            values.extend([geom_json, geom_json, geom_json, geom_json])

        has_id_col = "id" in column_names
        id_filter = '"id" = ?' if has_id_col else 'rowid = ?'
        values.append(int(feature_id) if not has_id_col else feature_id)
        set_str = ", ".join(set_clauses)
        query = f'UPDATE {table} SET {set_str} WHERE {id_filter}'
        logger.debug("Replace feature: %s", query)

        with ducklake_write_manager.connection() as con:
            con.execute(query, values)
            count_result = con.execute(
                f'SELECT COUNT(*) FROM {table} WHERE {id_filter}', [int(feature_id) if not has_id_col else feature_id]
            ).fetchone()
            return count_result is not None and count_result[0] > 0

    def delete_feature(
        self,
        layer_info: LayerInfo,
        feature_id: str,
        column_names: Optional[list[str]] = None,
    ) -> bool:
        """Delete a single feature.

        Args:
            layer_info: Layer info with table name
            feature_id: Feature ID
            column_names: Known column names (to detect id vs rowid)

        Returns:
            True if feature was found and deleted
        """
        table = layer_info.full_table_name
        has_id_col = column_names is None or "id" in column_names
        id_filter = '"id" = ?' if has_id_col else 'rowid = ?'
        id_val = feature_id if has_id_col else int(feature_id)

        with ducklake_write_manager.connection() as con:
            count = con.execute(
                f'SELECT COUNT(*) FROM {table} WHERE {id_filter}', [id_val]
            ).fetchone()
            if not count or count[0] == 0:
                return False

            con.execute(f'DELETE FROM {table} WHERE {id_filter}', [id_val])
            return True

    def delete_features_bulk(
        self,
        layer_info: LayerInfo,
        feature_ids: list[str],
        column_names: Optional[list[str]] = None,
    ) -> int:
        """Delete multiple features.

        Args:
            layer_info: Layer info with table name
            feature_ids: List of feature IDs to delete
            column_names: Known column names (to detect id vs rowid)

        Returns:
            Number of features deleted
        """
        table = layer_info.full_table_name

        if not feature_ids:
            return 0

        has_id_col = column_names is None or "id" in column_names
        id_col = '"id"' if has_id_col else 'rowid'
        id_vals = feature_ids if has_id_col else [int(fid) for fid in feature_ids]

        placeholders = ", ".join(["?"] * len(id_vals))
        query = f'DELETE FROM {table} WHERE {id_col} IN ({placeholders})'
        logger.debug("Bulk delete: %s ids", len(id_vals))

        with ducklake_write_manager.connection() as con:
            count_before = con.execute(
                f"SELECT COUNT(*) FROM {table}"
            ).fetchone()[0]
            con.execute(query, id_vals)
            count_after = con.execute(
                f"SELECT COUNT(*) FROM {table}"
            ).fetchone()[0]
            return count_before - count_after

    # --- Column Management ---

    def get_column_names(self, layer_info: LayerInfo) -> list[str]:
        """Get current column names for a layer."""
        with ducklake_write_manager.connection() as con:
            result = con.execute(
                f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_catalog = 'lake'
                AND table_schema = '{layer_info.schema_name}'
                AND table_name = '{layer_info.table_name}'
                ORDER BY ordinal_position
                """
            ).fetchall()
            return [row[0] for row in result]

    def get_column_types(self, layer_info: LayerInfo) -> dict[str, str]:
        """Get column name -> data_type mapping for a layer."""
        with ducklake_write_manager.connection() as con:
            result = con.execute(
                f"""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_catalog = 'lake'
                AND table_schema = '{layer_info.schema_name}'
                AND table_name = '{layer_info.table_name}'
                ORDER BY ordinal_position
                """
            ).fetchall()
            return {row[0]: row[1] for row in result}

    def add_column(
        self,
        layer_info: LayerInfo,
        name: str,
        type_name: str,
        default_value: Any = None,
    ) -> None:
        """Add a new column to a layer.

        Args:
            layer_info: Layer info with table name
            name: Column name
            type_name: Column type (user-friendly name)
            default_value: Optional default value
        """
        table = layer_info.full_table_name
        existing_columns = self.get_column_names(layer_info)
        _validate_new_column_name(name, existing_columns)
        duckdb_type = _resolve_duckdb_type(type_name)

        query = f'ALTER TABLE {table} ADD COLUMN "{name}" {duckdb_type}'
        if default_value is not None:
            query += " DEFAULT ?"
            logger.debug("Add column: %s", query)
            with ducklake_write_manager.connection() as con:
                con.execute(query, [default_value])
        else:
            logger.debug("Add column: %s", query)
            with ducklake_write_manager.connection() as con:
                con.execute(query)

    def rename_column(
        self,
        layer_info: LayerInfo,
        old_name: str,
        new_name: str,
    ) -> None:
        """Rename a column.

        Args:
            layer_info: Layer info with table name
            old_name: Current column name
            new_name: New column name
        """
        table = layer_info.full_table_name
        existing_columns = self.get_column_names(layer_info)
        _validate_column_name(old_name, existing_columns)
        _validate_new_column_name(new_name, existing_columns)

        query = f'ALTER TABLE {table} RENAME COLUMN "{old_name}" TO "{new_name}"'
        logger.debug("Rename column: %s", query)

        with ducklake_write_manager.connection() as con:
            con.execute(query)

    def delete_column(
        self,
        layer_info: LayerInfo,
        name: str,
    ) -> None:
        """Delete a column from a layer.

        Args:
            layer_info: Layer info with table name
            name: Column name to delete
        """
        table = layer_info.full_table_name
        existing_columns = self.get_column_names(layer_info)
        _validate_column_name(name, existing_columns)

        query = f'ALTER TABLE {table} DROP COLUMN "{name}"'
        logger.debug("Delete column: %s", query)

        with ducklake_write_manager.connection() as con:
            con.execute(query)


# Singleton instance
feature_write_service = FeatureWriteService()
