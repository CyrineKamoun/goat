"""Heatmap Closest Average tool for Windmill.

Computes closest average heatmap - average value of the closest features within max cost.
"""

import logging
from pathlib import Path
from typing import Any, Self

from pydantic import ConfigDict, Field

from goatlib.analysis.accessibility import HeatmapClosestAverageTool
from goatlib.analysis.schemas.heatmap import HeatmapClosestAverageParams
from goatlib.analysis.schemas.ui import (
    SECTION_CONFIGURATION,
    SECTION_OPPORTUNITIES,
    SECTION_RESULT_ROUTING,
    SECTION_ROUTING,
    ui_field,
    ui_sections,
)
from goatlib.models.io import DatasetMetadata
from goatlib.tools._opportunity_handles import (
    WORKFLOW_ONLY_OPPORTUNITY_FIELDS,
    NumberedOpportunityLayersMixin,
)
from goatlib.tools.base import BaseToolRunner
from goatlib.tools.schemas import (
    ToolInputBase,
    get_default_layer_name,
)
from goatlib.tools.style import get_heatmap_style

logger = logging.getLogger(__name__)


class HeatmapClosestAverageToolParams(
    NumberedOpportunityLayersMixin,
    ToolInputBase,
    HeatmapClosestAverageParams,
):
    """Parameters for heatmap closest average tool.

    Inherits heatmap options from HeatmapClosestAverageParams, adds layer context from ToolInputBase.
    """

    model_config = ConfigDict(
        json_schema_extra=ui_sections(
            SECTION_ROUTING,
            SECTION_CONFIGURATION,
            SECTION_OPPORTUNITIES,
            SECTION_RESULT_ROUTING,
        )
    )

    # Override file paths as optional - they will be resolved internally
    od_matrix_path: str | None = Field(
        default=None,
        description="Path to OD matrix (auto-populated from routing_mode if not provided)",
        json_schema_extra=ui_field(section="configuration", hidden=True),
    )
    output_path: str | None = None  # type: ignore[assignment]
    reference_area_path: str | None = None  # type: ignore[assignment]

    # Layer ID for the reference area (replaces reference_area_path for tools)
    reference_area_layer_id: str | None = Field(
        None,
        description="Layer ID for the reference area polygon",
        json_schema_extra=ui_field(
            section="opportunities",
            field_order=4,
            label_key="reference_area_path",
            widget="layer-selector",
            widget_options={"geometry_types": ["Polygon", "MultiPolygon"]},
            advanced=True,
        ),
    )
    reference_area_layer_filter: dict[str, Any] | None = Field(
        None,
        description="CQL2-JSON filter to apply to the reference area layer",
        json_schema_extra=ui_field(section="configuration", field_order=5, hidden=True),
    )

    # Override result_layer_name with tool-specific defaults
    result_layer_name: str | None = Field(
        default=get_default_layer_name("heatmap_closest_average", "en"),
        description="Name for the heatmap closest average result layer.",
        json_schema_extra=ui_field(
            section="result",
            field_order=1,
            label_key="result_layer_name",
            widget_options={
                "default_en": get_default_layer_name("heatmap_closest_average", "en"),
                "default_de": get_default_layer_name("heatmap_closest_average", "de"),
            },
        ),
    )


class HeatmapClosestAverageToolRunner(BaseToolRunner[HeatmapClosestAverageToolParams]):
    """Heatmap Closest Average tool runner for Windmill."""

    tool_class = HeatmapClosestAverageTool
    output_geometry_type = "polygon"  # H3 cells
    default_output_name = get_default_layer_name("heatmap_closest_average", "en")

    @classmethod
    def predict_output_schema(
        cls,
        input_schemas: dict[str, dict[str, str]],
        params: dict[str, Any],
    ) -> dict[str, str]:
        """Predict heatmap closest average output schema.

        Heatmap closest average outputs:
        - h3_index: H3 cell index
        - total_accessibility: average travel cost to N closest destinations
        - geometry: H3 cell polygon
        """
        return {
            "h3_index": "VARCHAR",
            "total_accessibility": "DOUBLE",
            "geometry": "GEOMETRY",
        }

    def get_layer_properties(
        self: Self,
        params: HeatmapClosestAverageToolParams,
        metadata: DatasetMetadata,
        table_info: dict[str, Any] | None = None,
        parquet_path: Path | str | None = None,
    ) -> dict[str, Any] | None:
        """Return heatmap style for closest average accessibility with quantile breaks."""
        color_field = "total_accessibility"

        # Compute quantile breaks from the DuckLake table (6 breaks for 7 colors)
        color_scale_breaks = None
        table_name = table_info["table_name"] if table_info else None
        if table_name or parquet_path:
            color_scale_breaks = self.compute_quantile_breaks(
                table_name=table_name,
                column_name=color_field,
                num_breaks=6,
                strip_zeros=True,
                parquet_path=parquet_path,
            )
            if color_scale_breaks:
                logger.info(
                    "Computed quantile breaks for %s: %s",
                    color_field,
                    color_scale_breaks,
                )

        # Use Emrld (green) for closest average - represents access to opportunities
        return get_heatmap_style(
            color_field_name=color_field,
            color_scale_breaks=color_scale_breaks,
            color_range_name="Emrld",
        )

    def process(
        self: Self, params: HeatmapClosestAverageToolParams, temp_dir: Path
    ) -> tuple[Path, DatasetMetadata]:
        """Run heatmap closest average analysis."""
        output_path = temp_dir / "output.parquet"

        # Resolve opportunity layer IDs to parquet paths
        resolved_opportunities = self.resolve_layer_paths(
            params.opportunities, params.user_id, "input_path"
        )
        # Export reference area layer
        reference_area_path = None
        if params.reference_area_layer_id is not None:
            reference_area_path = str(
                self.export_layer_to_parquet(
                    layer_id=params.reference_area_layer_id,
                    user_id=params.user_id,
                    cql_filter=params.reference_area_layer_filter,
                )
            )
        # Auto-resolve od_matrix_path from routing_mode if not provided
        od_matrix_path = params.od_matrix_path
        if not od_matrix_path:
            od_matrix_path = (
                f"{self.settings.od_matrix_base_path}/{params.routing_mode.value}/"
            )

        # Build analysis params
        analysis_params = HeatmapClosestAverageParams(
            **params.model_dump(
                exclude={
                    "output_path",
                    "od_matrix_path",
                    "user_id",
                    "folder_id",
                    "project_id",
                    "output_name",
                    "opportunities",  # Use resolved opportunities
                    # Canvas-only plumbing, already folded into opportunities
                    *WORKFLOW_ONLY_OPPORTUNITY_FIELDS,
                    "reference_area_path",
                    "reference_area_layer_id",
                    "reference_area_layer_filter",
                }
            ),
            opportunities=resolved_opportunities,
            od_matrix_path=od_matrix_path,
            reference_area_path=reference_area_path,
            output_path=str(output_path),
        )

        tool = self.tool_class()
        try:
            results = tool.run(analysis_params)
            result_path, metadata = results[0]
            return Path(result_path), metadata
        finally:
            tool.cleanup()


def main(params: HeatmapClosestAverageToolParams) -> dict:
    """Windmill entry point for heatmap closest average tool."""
    runner = HeatmapClosestAverageToolRunner()
    runner.init_from_env()

    try:
        return runner.run(params)
    finally:
        runner.cleanup()
