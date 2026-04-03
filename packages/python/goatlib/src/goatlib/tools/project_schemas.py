"""Pydantic models for the GOAT project export archive format.

These models define the JSON structure inside the exported ZIP file.
They are used by both ProjectExport (serialization) and ProjectImport (validation).
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

# Literal types mirroring core enums for validation
LayerTypeLiteral = Literal["feature", "raster", "table"]
FeatureTypeLiteral = Literal["standard", "tool", "street_network"]
FeatureGeometryTypeLiteral = Literal["point", "line", "polygon"]
DataTypeLiteral = Literal["mvt", "wfs", "wms", "xyz", "wmts", "cog"]
FileUploadTypeLiteral = Literal[
    "csv", "xlsx", "geojson", "gpkg", "kml", "zip", "parquet"
]
DataLicenseLiteral = Literal[
    "DDN2",
    "DDZ2",
    "CC_BY",
    "CC_BY_SA",
    "CC_BY_ND",
    "CC_BY_NC",
    "CC_BY_NC_SA",
    "CC_BY_NC_ND",
    "CC_ZERO",
    "ODC_BY",
    "ODC_ODbL",
    "OTHER",
]
DataCategoryLiteral = Literal[
    "basemap",
    "imagery",
    "boundary",
    "people",
    "transportation",
    "environment",
    "landuse",
    "places",
]
AssetTypeLiteral = Literal["image", "icon"]


class ExportManifest(BaseModel):
    """manifest.json — archive metadata and integrity."""

    model_config = ConfigDict(extra="ignore")

    format_version: str = "1.0"
    goat_version: str | None = None
    exported_at: datetime
    source_instance: str | None = None
    project_name: str
    checksums: dict[str, str] = Field(default_factory=dict)
    layer_count: int = 0
    internal_layer_count: int = 0
    external_layer_count: int = 0
    workflow_count: int = 0
    report_count: int = 0


class ExportProjectMetadata(BaseModel):
    """project.json — project record fields."""

    model_config = ConfigDict(extra="ignore")

    name: str
    description: str | None = None
    basemap: str | None = None
    max_extent: list[float] | None = None
    builder_config: dict[str, Any] | None = None
    tags: list[str] | None = None
    initial_view_state: dict[str, Any] | None = None
    layer_order: list[int] | None = None


class ExportLayerMetadata(BaseModel):
    """layers/{layer_id}/metadata.json — layer record fields."""

    model_config = ConfigDict(extra="ignore")

    id: str
    name: str
    description: str | None = None
    type: LayerTypeLiteral
    feature_layer_type: FeatureTypeLiteral | None = None
    feature_layer_geometry_type: FeatureGeometryTypeLiteral | None = None
    data_type: DataTypeLiteral | None = None
    url: str | None = None
    properties: dict[str, Any] | None = None
    other_properties: dict[str, Any] | None = None
    attribute_mapping: dict[str, Any] | None = None
    upload_reference_system: int | None = None
    upload_file_type: FileUploadTypeLiteral | None = None
    size: int | None = None
    # Quality/metadata fields
    lineage: str | None = None
    positional_accuracy: str | None = None
    attribute_accuracy: str | None = None
    completeness: str | None = None
    geographical_code: str | None = None
    language_code: str | None = None
    distributor_name: str | None = None
    distributor_email: str | None = None
    distribution_url: str | None = None
    license: DataLicenseLiteral | None = None
    attribution: str | None = None
    data_reference_year: int | None = None
    data_category: DataCategoryLiteral | None = None
    is_external: bool = False  # True if WMS/WFS/XYZ/COG (no data.parquet)


class ExportLayerProjectLink(BaseModel):
    """layers/{layer_id}/project_link.json — per-project layer config."""

    model_config = ConfigDict(extra="ignore")

    name: str | None = None
    order: int = 0
    properties: dict[str, Any] | None = None
    other_properties: dict[str, Any] | None = None
    query: dict[str, Any] | None = None
    charts: dict[str, Any] | None = None
    group_id: str | int | None = None  # References group by original ID


class ExportLayerIndex(BaseModel):
    """layers/index.json — list of all layers."""

    model_config = ConfigDict(extra="ignore")

    layers: list[ExportLayerMetadata]


class ExportLayerGroup(BaseModel):
    """Single group in layer_groups.json."""

    model_config = ConfigDict(extra="ignore")

    id: str | int  # Original group ID (int in DB, coerced to str for portability)
    name: str
    order: int = 0
    properties: dict[str, Any] | None = None
    parent_id: str | int | None = None  # Self-referencing for nesting


class ExportLayerGroupTree(BaseModel):
    """layer_groups.json — full group hierarchy."""

    model_config = ConfigDict(extra="ignore")

    groups: list[ExportLayerGroup]


class ExportWorkflow(BaseModel):
    """workflows/{workflow_id}.json — workflow definition."""

    model_config = ConfigDict(extra="ignore")

    id: str
    name: str
    description: str | None = None
    is_default: bool = False
    config: dict[str, Any]  # Nodes, edges, viewport, variables
    thumbnail_url: str | None = None


class ExportReportLayout(BaseModel):
    """reports/{report_id}.json — report layout definition."""

    model_config = ConfigDict(extra="ignore")

    id: str
    name: str
    description: str | None = None
    is_default: bool = False
    is_predefined: bool = False
    config: dict[str, Any]
    thumbnail_url: str | None = None


class ExportAssetEntry(BaseModel):
    """Single asset in the manifest."""

    model_config = ConfigDict(extra="ignore")

    id: str
    file_name: str
    display_name: str | None = None
    category: str | None = None
    mime_type: str
    file_size: int = 0
    asset_type: AssetTypeLiteral
    content_hash: str | None = None
    archive_path: str  # Path within the ZIP (e.g., "assets/abc123.png")


class ExportAssetManifest(BaseModel):
    """assets/index.json — asset listing."""

    model_config = ConfigDict(extra="ignore")

    assets: list[ExportAssetEntry]


# Constants
EXTERNAL_DATA_TYPES = {"wms", "wfs", "xyz", "cog", "wmts", "mvt"}
FORMAT_VERSION = "1.0"
