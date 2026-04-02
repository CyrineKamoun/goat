"""Feature write service for DuckLake mutations.

Handles feature CRUD (create, update, delete) and column management
(add, rename, delete columns) via the write-capable DuckLake manager.

All writes use DuckDB's stable `rowid` as the canonical feature identifier.
rowid is immutable, unique, monotonically increasing, and never recycled.

All writes are serialized by BaseDuckLakeManager's internal threading.Lock.
"""

import json
import logging
from typing import Any, Optional

from geoapi.config import settings
from geoapi.dependencies import LayerInfo
from geoapi.ducklake_write import ducklake_write_manager
from geoapi.models.write import COLUMN_TYPE_MAP

logger = logging.getLogger(__name__)

# Columns that cannot be modified by users
PROTECTED_COLUMNS = {"id", "geometry", "geom", "rowid"}


def _feature_id_to_rowid(feature_id: str) -> int:
    """Convert a public feature ID to internal DuckDB rowid.

    Feature IDs are rowid + 1 because MVT feature ID 0 is treated
    as "unset" by MapLibre (known limitation).
    """
    return int(feature_id) - 1


def _rowid_to_feature_id(rowid: int) -> str:
    """Convert internal DuckDB rowid to public feature ID."""
    return str(rowid + 1)


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
    ) -> str:
        """Create a new feature. Returns the new rowid as string."""
        table = layer_info.full_table_name

        columns: list[str] = []
        placeholders: list[str] = []
        values: list[Any] = []

        # Add geometry if present
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

        # Add properties (only known columns, skip protected)
        for col_name, col_value in properties.items():
            if col_name in column_names and col_name not in PROTECTED_COLUMNS:
                columns.append(f'"{col_name}"')
                placeholders.append("?")
                values.append(col_value)

        columns_str = ", ".join(columns)
        placeholders_str = ", ".join(placeholders)
        query = f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders_str})"
        logger.debug("Create feature: %s", query)

        with ducklake_write_manager.connection() as con:
            con.execute(query, values)
            result = con.execute(f"SELECT max(rowid) FROM {table}").fetchone()
            rowid = result[0] if result and result[0] is not None else 0
            return _rowid_to_feature_id(rowid)

    def create_features_bulk(
        self,
        layer_info: LayerInfo,
        features: list[dict[str, Any]],
        column_names: list[str],
        geometry_column: Optional[str] = None,
    ) -> list[str]:
        """Create multiple features. Returns list of new rowids as strings."""
        table = layer_info.full_table_name
        feature_ids: list[str] = []

        with ducklake_write_manager.connection() as con:
            for feature_data in features:
                geometry = feature_data.get("geometry")
                properties = feature_data.get("properties", {})

                columns: list[str] = []
                placeholders: list[str] = []
                values: list[Any] = []

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

                result = con.execute(f"SELECT max(rowid) FROM {table}").fetchone()
                rowid = result[0] if result and result[0] is not None else 0
                feature_ids.append(_rowid_to_feature_id(rowid))

        return feature_ids

    def update_feature_properties(
        self,
        layer_info: LayerInfo,
        feature_id: str,
        properties: dict[str, Any],
        column_names: list[str],
    ) -> bool:
        """Update feature properties by rowid (partial update)."""
        table = layer_info.full_table_name

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

        rid = _feature_id_to_rowid(feature_id)
        values.append(rid)
        set_str = ", ".join(set_clauses)
        query = f'UPDATE {table} SET {set_str} WHERE rowid = ?'
        logger.debug("Update feature: %s", query)

        with ducklake_write_manager.connection() as con:
            con.execute(query, values)
            count_result = con.execute(
                f'SELECT COUNT(*) FROM {table} WHERE rowid = ?', [rid]
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
        """Replace a feature entirely by rowid (geometry + properties)."""
        table = layer_info.full_table_name

        set_clauses = []
        values: list[Any] = []

        if geometry and geometry_column:
            set_clauses.append(f'"{geometry_column}" = ST_GeomFromGeoJSON(?)')
            values.append(json.dumps(geometry))

        for col_name, col_value in properties.items():
            if col_name in column_names and col_name not in PROTECTED_COLUMNS:
                set_clauses.append(f'"{col_name}" = ?')
                values.append(col_value)

        if not set_clauses:
            raise ValueError("No valid fields to update")

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

        rid = _feature_id_to_rowid(feature_id)
        values.append(rid)
        set_str = ", ".join(set_clauses)
        query = f'UPDATE {table} SET {set_str} WHERE rowid = ?'
        logger.debug("Replace feature: %s", query)

        with ducklake_write_manager.connection() as con:
            con.execute(query, values)
            count_result = con.execute(
                f'SELECT COUNT(*) FROM {table} WHERE rowid = ?', [rid]
            ).fetchone()
            return count_result is not None and count_result[0] > 0

    def delete_feature(
        self,
        layer_info: LayerInfo,
        feature_id: str,
    ) -> bool:
        """Delete a single feature by feature ID."""
        table = layer_info.full_table_name
        rid = _feature_id_to_rowid(feature_id)

        with ducklake_write_manager.connection() as con:
            count = con.execute(
                f'SELECT COUNT(*) FROM {table} WHERE rowid = ?', [rid]
            ).fetchone()
            if not count or count[0] == 0:
                return False
            con.execute(f'DELETE FROM {table} WHERE rowid = ?', [rid])
            return True

    def delete_features_bulk(
        self,
        layer_info: LayerInfo,
        feature_ids: list[str],
    ) -> int:
        """Delete multiple features by feature ID."""
        table = layer_info.full_table_name
        if not feature_ids:
            return 0

        id_vals = [_feature_id_to_rowid(fid) for fid in feature_ids]
        placeholders = ", ".join(["?"] * len(id_vals))
        query = f'DELETE FROM {table} WHERE rowid IN ({placeholders})'
        logger.debug("Bulk delete: %s ids", len(id_vals))

        with ducklake_write_manager.connection() as con:
            count_before = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            con.execute(query, id_vals)
            count_after = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
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
