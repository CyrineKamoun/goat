"""Disjoint tool for Windmill.

Filters an input layer to features that have no spatial intersection
with any feature in the overlay layer (anti-semi-join).
"""

import logging
from pathlib import Path
from typing import Any, Self

from pydantic import ConfigDict, Field

from goatlib.analysis.geoprocessing.disjoint import DisjointTool
from goatlib.analysis.schemas.geoprocessing import DisjointParams
from goatlib.analysis.schemas.ui import (
    SECTION_INPUT,
    SECTION_OUTPUT,
    SECTION_RESULT,
    UISection,
    ui_field,
    ui_sections,
)
from goatlib.models.io import DatasetMetadata
from goatlib.tools.base import BaseToolRunner
from goatlib.tools.schemas import (
    ScenarioSelectorMixin,
    ToolInputBase,
    TwoLayerInputMixin,
    get_default_layer_name,
)

logger = logging.getLogger(__name__)


class DisjointToolParams(
    ScenarioSelectorMixin, ToolInputBase, TwoLayerInputMixin, DisjointParams
):
    """Parameters for disjoint tool.

    Inherits options from DisjointParams; layer IDs come from TwoLayerInputMixin.
    """

    model_config = ConfigDict(
        json_schema_extra=ui_sections(
            SECTION_INPUT,
            UISection(id="overlay", order=2, icon="layers"),
            SECTION_RESULT,
            UISection(
                id="scenario",
                order=8,
                icon="scenario",
                collapsible=True,
                collapsed=True,
                depends_on={"input_layer_id": {"$ne": None}},
            ),
            SECTION_OUTPUT,
        )
    )

    input_path: str | None = None  # type: ignore[assignment]
    overlay_path: str | None = None  # type: ignore[assignment]
    output_path: str | None = None

    result_layer_name: str | None = Field(
        default=get_default_layer_name("disjoint", "en"),
        description="Name for the disjoint result layer.",
        json_schema_extra=ui_field(
            section="result",
            field_order=1,
            label_key="result_layer_name",
            widget_options={
                "default_en": get_default_layer_name("disjoint", "en"),
                "default_de": get_default_layer_name("disjoint", "de"),
            },
        ),
    )


class DisjointToolRunner(BaseToolRunner[DisjointToolParams]):
    """Disjoint tool runner for Windmill."""

    tool_class = DisjointTool
    output_geometry_type = None  # Same as input
    default_output_name = get_default_layer_name("disjoint", "en")

    @classmethod
    def predict_output_schema(
        cls,
        input_schemas: dict[str, dict[str, str]],
        params: dict[str, Any],
    ) -> dict[str, str]:
        """Disjoint preserves all columns from the input layer."""
        primary_input = input_schemas.get("input_layer_id", {})
        return dict(primary_input)

    def process(
        self: Self, params: DisjointToolParams, temp_dir: Path
    ) -> tuple[Path, DatasetMetadata]:
        """Run disjoint analysis."""
        input_path = self.export_layer_to_parquet(
            layer_id=params.input_layer_id,
            user_id=params.user_id,
            cql_filter=params.input_layer_filter,
            scenario_id=params.scenario_id,
            project_id=params.project_id,
        )
        overlay_path = self.export_layer_to_parquet(
            layer_id=params.overlay_layer_id,
            user_id=params.user_id,
            cql_filter=params.overlay_layer_filter,
            scenario_id=params.scenario_id,
            project_id=params.project_id,
        )
        output_path = temp_dir / "output.parquet"

        analysis_params = DisjointParams(
            **params.model_dump(
                exclude={
                    "input_path",
                    "overlay_path",
                    "output_path",
                    "user_id",
                    "folder_id",
                    "project_id",
                    "scenario_id",
                    "output_name",
                    "input_layer_id",
                    "input_layer_filter",
                    "overlay_layer_id",
                    "overlay_layer_filter",
                }
            ),
            input_path=input_path,
            overlay_path=overlay_path,
            output_path=str(output_path),
        )

        tool = self.tool_class()
        try:
            results = tool.run(analysis_params)
            result_path, metadata = results[0]
            return Path(result_path), metadata
        finally:
            tool.cleanup()


def main(params: DisjointToolParams) -> dict:
    """Windmill entry point for disjoint tool."""
    runner = DisjointToolRunner()
    runner.init_from_env()

    try:
        return runner.run(params)
    finally:
        runner.cleanup()
