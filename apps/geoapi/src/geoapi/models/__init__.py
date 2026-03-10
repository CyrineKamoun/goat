"""Models package for GeoAPI."""

# Core OGC models (shared types like Link, GeometryType)
from geoapi.models.ogc import (
    Collection,
    CollectionsResponse,
    Conformance,
    Extent,
    Feature,
    FeatureCollection,
    GeometryType,
    HealthCheck,
    LandingPage,
    Link,
    QueryableProperty,
    Queryables,
    SpatialExtent,
    StyleJSON,
    TileJSON,
    TileMatrixSetItem,
    TileMatrixSetsResponse,
    TileSet,
)

# Write models (feature CRUD, column management)
from geoapi.models.write import (
    BulkDeleteRequest,
    BulkDeleteResponse,
    BulkFeatureCreate,
    BulkWriteResponse,
    ColumnCreate,
    ColumnResponse,
    ColumnUpdate,
    DeleteResponse,
    FeatureCreate,
    FeatureReplace,
    FeatureUpdate,
    FeatureWriteResponse,
)

__all__ = [
    # OGC Core Models
    "Collection",
    "CollectionsResponse",
    "Conformance",
    "Extent",
    "Feature",
    "FeatureCollection",
    "GeometryType",
    "HealthCheck",
    "LandingPage",
    "Link",
    "Queryables",
    "QueryableProperty",
    "SpatialExtent",
    "StyleJSON",
    "TileJSON",
    "TileMatrixSetItem",
    "TileMatrixSetsResponse",
    "TileSet",
    # Write Models
    "BulkDeleteRequest",
    "BulkDeleteResponse",
    "BulkFeatureCreate",
    "BulkWriteResponse",
    "ColumnCreate",
    "ColumnResponse",
    "ColumnUpdate",
    "DeleteResponse",
    "FeatureCreate",
    "FeatureReplace",
    "FeatureUpdate",
    "FeatureWriteResponse",
]
