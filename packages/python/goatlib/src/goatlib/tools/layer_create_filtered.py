"""LayerCreateFiltered Tool - Create a new layer from a filtered subset.

This tool creates a fully independent layer by materializing the filtered
rows of an existing layer into a new DuckLake table with its own PMTiles.

Usage:
    from goatlib.tools.layer_create_filtered import LayerCreateFilteredParams, main

    result = main(LayerCreateFilteredParams(
        user_id="...",
        project_id="...",
        source_layer_id="...",
        cql_filter={"op": "and", "args": [...]},
        result_layer_name="Buildings (filtered)",
    ))
"""

import logging
from pathlib import Path
from typing import Any, Self

from pydantic import ConfigDict, Field

from goatlib.analysis.schemas.ui import (
    SECTION_INPUT,
    SECTION_OUTPUT,
    SECTION_RESULT,
    ui_field,
    ui_sections,
)
from goatlib.models.io import DatasetMetadata
from goatlib.tools.base import BaseToolRunner
from goatlib.tools.schemas import ToolInputBase

logger = logging.getLogger(__name__)


class LayerCreateFilteredParams(ToolInputBase):
    """Parameters for creating a layer from a filtered subset."""

    model_config = ConfigDict(
        json_schema_extra=ui_sections(
            SECTION_INPUT,
            SECTION_RESULT,
            SECTION_OUTPUT,
        )
    )

    source_layer_id: str = Field(
        ...,
        description="ID of the source layer to filter",
        json_schema_extra=ui_field(
            section="input",
            field_order=1,
            hidden=True,
        ),
    )
    cql_filter: dict[str, Any] = Field(
        ...,
        description="CQL2-JSON filter to apply to the source layer",
        json_schema_extra=ui_field(
            section="input",
            field_order=2,
            hidden=True,
        ),
    )
    source_properties: dict[str, Any] | None = Field(
        None,
        description="Style properties to copy from the source layer",
        json_schema_extra=ui_field(
            section="input",
            field_order=3,
            hidden=True,
        ),
    )


class LayerCreateFilteredToolRunner(BaseToolRunner["LayerCreateFilteredParams"]):
    """Runner for creating a layer from a filtered subset.

    Reads the source layer with the CQL filter applied, writes the filtered
    rows to a parquet file, then lets BaseToolRunner handle DuckLake ingestion,
    PMTiles generation, and DB record creation.
    """

    tool_class = "layer_create_filtered"
    output_geometry_type: str | None = None  # Dynamic based on source layer
    default_output_name = "Filtered Layer"

    def process(
        self: Self,
        params: LayerCreateFilteredParams,
        temp_dir: Path,
    ) -> tuple[Path, DatasetMetadata]:
        """Export the filtered source layer as parquet."""
        # Export the source layer with the CQL filter applied
        input_path = self.export_layer_to_parquet(
            layer_id=params.source_layer_id,
            user_id=params.user_id,
            cql_filter=params.cql_filter,
            scenario_id=params.scenario_id,
            project_id=params.project_id,
        )

        # The exported parquet IS our output — no further analysis needed
        output_path = Path(input_path)

        metadata = DatasetMetadata(
            path=str(output_path),
            source_type="vector",
        )
        return output_path, metadata

    def get_layer_properties(
        self: Self,
        params: LayerCreateFilteredParams,
        metadata: DatasetMetadata,
        table_info: Any = None,
        parquet_path: Any = None,
    ) -> dict[str, Any] | None:
        """Return the source layer's style properties."""
        return params.source_properties

    def get_feature_layer_type(
        self: Self,
        params: LayerCreateFilteredParams,
    ) -> str:
        """Mark as standard (not tool output) since this is user data."""
        return "standard"


def main(params: LayerCreateFilteredParams) -> dict[str, Any]:
    """Windmill entry point for LayerCreateFiltered.

    Args:
        params: Validated LayerCreateFilteredParams

    Returns:
        ToolOutputBase as dict
    """
    runner = LayerCreateFilteredToolRunner()
    runner.init_from_env()
    try:
        return runner.run(params)
    finally:
        runner.cleanup()
