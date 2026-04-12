"""Pydantic models for write operations (feature CRUD, column management)."""

from typing import Any, Optional

from pydantic import BaseModel, Field

# --- Feature Write Models ---


class FeatureCreate(BaseModel):
    """GeoJSON Feature for creation."""

    type: str = "Feature"
    geometry: Optional[dict[str, Any]] = None
    properties: dict[str, Any] = Field(default_factory=dict)


class FeatureUpdate(BaseModel):
    """Partial properties update for a feature."""

    properties: dict[str, Any]


class FeatureReplace(BaseModel):
    """Full feature replacement (geometry + properties)."""

    type: str = "Feature"
    geometry: Optional[dict[str, Any]] = None
    properties: dict[str, Any] = Field(default_factory=dict)


class BulkFeatureCreate(BaseModel):
    """Batch feature creation."""

    type: str = "FeatureCollection"
    features: list[FeatureCreate]


class BulkDeleteRequest(BaseModel):
    """Batch feature deletion by IDs."""

    ids: list[str]


class FeatureWriteResponse(BaseModel):
    """Response after a feature write operation."""

    id: str
    message: str = "success"


class BulkWriteResponse(BaseModel):
    """Response after a bulk write operation."""

    ids: list[str]
    count: int
    message: str = "success"


class DeleteResponse(BaseModel):
    """Response after a delete operation."""

    id: str
    message: str = "deleted"


class BulkDeleteResponse(BaseModel):
    """Response after a bulk delete operation."""

    count: int
    message: str = "deleted"


# --- Column Management Models ---

# Mapping of user-friendly type names to DuckDB types
COLUMN_TYPE_MAP: dict[str, str] = {
    "string": "VARCHAR",
    "text": "VARCHAR",
    "integer": "INTEGER",
    "int": "INTEGER",
    "bigint": "BIGINT",
    "number": "DOUBLE",
    "float": "DOUBLE",
    "double": "DOUBLE",
    "decimal": "DECIMAL",
    "boolean": "BOOLEAN",
    "bool": "BOOLEAN",
    "date": "DATE",
    "timestamp": "TIMESTAMP",
    "json": "JSON",
}

VALID_COLUMN_TYPES = list(COLUMN_TYPE_MAP.keys())


class ColumnCreate(BaseModel):
    """Create a new column."""

    name: str = Field(..., min_length=1, max_length=255, pattern=r"^[a-zA-Z_][a-zA-Z0-9_]*$")
    type: str = Field(..., description=f"Column type. Valid values: {', '.join(VALID_COLUMN_TYPES)}")
    default_value: Optional[Any] = None


class ColumnUpdate(BaseModel):
    """Update column properties (rename)."""

    new_name: Optional[str] = Field(
        None, min_length=1, max_length=255, pattern=r"^[a-zA-Z_][a-zA-Z0-9_]*$"
    )


class ColumnResponse(BaseModel):
    """Response for column operations."""

    name: str
    type: str
    message: str = "success"
