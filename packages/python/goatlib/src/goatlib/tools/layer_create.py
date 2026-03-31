"""Layer create tool for Windmill.

Creates empty layers (with or without geometry) in DuckLake storage.
Users define a layer name, optional geometry type, and custom field definitions.
"""

import logging
from pathlib import Path
from typing import Any, Literal, Self

import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import BaseModel, Field

from goatlib.models.io import DatasetMetadata
from goatlib.tools.base import BaseToolRunner
from goatlib.tools.schemas import ToolInputBase

logger = logging.getLogger(__name__)

# Map user-facing geometry types to GeoJSON/WKB geometry type names
GEOMETRY_TYPE_MAP: dict[str, str] = {
    "point": "Point",
    "line": "LineString",
    "polygon": "Polygon",
}

# Map user-facing field types to PyArrow types
FIELD_TYPE_MAP: dict[str, pa.DataType] = {
    "string": pa.string(),
    "number": pa.float64(),
}


class FieldDefinition(BaseModel):
    """Definition of a single field (column) for the new layer."""

    name: str = Field(..., description="Column name")
    type: Literal["string", "number"] = Field(..., description="Column data type")


class LayerCreateParams(ToolInputBase):
    """Parameters for layer create tool."""

    name: str = Field(..., description="Layer name")
    geometry_type: Literal["point", "line", "polygon"] | None = Field(
        None,
        description="Geometry type for the layer. None creates a table without geometry.",
    )
    fields: list[FieldDefinition] = Field(
        default_factory=list,
        description="Field definitions for the layer",
    )
    description: str | None = Field(
        None,
        description="Layer description",
    )
    tags: list[str] | None = Field(
        None,
        description="Tags for categorizing the layer",
    )


class LayerCreateToolRunner(BaseToolRunner[LayerCreateParams]):
    """Layer create tool runner for Windmill.

    Creates empty layers with user-defined schema.
    Produces GeoParquet for geometry layers or plain Parquet for tables.
    """

    tool_class = None  # No analysis tool - we create layers directly
    output_geometry_type = None  # Determined at runtime from params
    default_output_name = "New Layer"

    def get_feature_layer_type(self: Self, params: LayerCreateParams) -> str:
        """Return 'standard' for user-created layers.

        Args:
            params: Create parameters

        Returns:
            "standard" for user-created layers
        """
        return "standard"

    def process(
        self: Self, params: LayerCreateParams, temp_dir: Path
    ) -> tuple[Path, DatasetMetadata]:
        """Create an empty Parquet file with the specified schema.

        For geometry layers: creates GeoParquet via geopandas with an empty
        GeoSeries in the specified CRS.
        For table layers: creates plain Parquet via pyarrow.

        Args:
            params: Layer create parameters
            temp_dir: Temporary directory for output file

        Returns:
            Tuple of (output_parquet_path, metadata)
        """
        output_path = temp_dir / "output.parquet"

        # Build PyArrow fields for user-defined columns
        pa_fields = [
            pa.field(f.name, FIELD_TYPE_MAP[f.type]) for f in params.fields
        ]

        geometry_type_name: str | None = None

        if params.geometry_type is not None:
            # Geometry layer: use geopandas to produce valid GeoParquet
            import geopandas as gpd

            geometry_type_name = GEOMETRY_TYPE_MAP[params.geometry_type]

            # Create empty GeoDataFrame with proper CRS
            data = {f.name: pa.array([], type=FIELD_TYPE_MAP[f.type]).to_pylist() for f in params.fields}
            gdf = gpd.GeoDataFrame(
                data,
                geometry=gpd.GeoSeries([], crs="EPSG:4326"),
            )
            gdf.to_parquet(str(output_path))
        else:
            # Table layer: plain Parquet via pyarrow
            schema = pa.schema(pa_fields)
            table = pa.table({f.name: pa.array([], type=f.type) for f in schema}, schema=schema)
            pq.write_table(table, str(output_path))

        metadata = DatasetMetadata(
            path=str(output_path),
            source_type="vector" if params.geometry_type else "tabular",
            format="parquet",
            crs="EPSG:4326" if params.geometry_type else None,
            feature_count=0,
            geometry_type=geometry_type_name,
        )

        logger.info(
            "Created empty layer: geometry_type=%s, fields=%d",
            geometry_type_name,
            len(params.fields),
        )

        return output_path, metadata

    def _ingest_to_ducklake(
        self: Self,
        user_id: str,
        layer_id: str,
        parquet_path: Path,
    ) -> dict[str, Any]:
        """Override to inject geometry type for empty layers.

        BaseToolRunner._get_table_info detects geometry_type from actual rows
        via ST_GeometryType. Empty layers have 0 rows, so it returns None,
        causing the layer to be classified as "table" instead of "feature".

        We call super() then patch the result with the known geometry type.
        """
        table_info = super()._ingest_to_ducklake(user_id, layer_id, parquet_path)

        # For empty geometry layers, the base class can't detect geometry_type
        # from rows. Inject it from our params so the layer is created as "feature".
        if self._create_params and self._create_params.geometry_type:
            if not table_info.get("geometry_type"):
                geom_name = GEOMETRY_TYPE_MAP[self._create_params.geometry_type]
                table_info["geometry_type"] = geom_name
                if not table_info.get("geometry_column"):
                    table_info["geometry_column"] = "geometry"

        return table_info

    def run(self: Self, params: LayerCreateParams) -> dict:
        """Run layer create with custom output name handling.

        Sets result_layer_name from params.name if not already set.
        Stores params on instance so _ingest_to_ducklake can access them.

        Args:
            params: Layer create parameters

        Returns:
            Dict with layer metadata
        """
        if not params.result_layer_name and not params.output_name:
            params.result_layer_name = params.name

        # Store params so _ingest_to_ducklake override can access geometry_type
        self._create_params = params

        return super().run(params)


def main(params: LayerCreateParams) -> dict:
    """Windmill entry point for layer create tool."""
    runner = LayerCreateToolRunner()
    runner.init_from_env()

    try:
        return runner.run(params)
    finally:
        runner.cleanup()
