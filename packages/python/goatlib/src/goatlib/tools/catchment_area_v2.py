"""Catchment Area V2 tool for Windmill.

Uses the local C++ routing backend for all modes. Mirrors the v1 tool runner
structure but builds CatchmentAreaV2Params with cost_type/max_cost.
"""

import logging
import tempfile
from pathlib import Path
from typing import Any, Self

import duckdb
from enum import StrEnum

from pydantic import Field, model_validator

from goatlib.analysis.accessibility import CatchmentAreaToolV2
from goatlib.analysis.schemas.catchment_area import (
    CATCHMENT_AREA_TYPE_LABELS,
    ROUTING_MODE_ICONS,
    ROUTING_MODE_LABELS,
    CatchmentAreaRoutingMode,
    StartingPoints,
)
from goatlib.analysis.schemas.catchment_area_v2 import (
    AccessEgressMode,
    CatchmentAreaV2Params,
    CatchmentType,
    CostType,
    OutputFormat,
    PTMode,
    PTTimeWindow,
    RoutingMode,
    Weekday,
)
from goatlib.analysis.schemas.ui import (
    SECTION_ROUTING,
    UISection,
    ui_field,
    ui_sections,
)
from goatlib.models.io import DatasetMetadata
from goatlib.tools.catchment_area import CatchmentAreaToolRunner
from goatlib.tools.schemas import ToolInputBase, get_default_layer_name

logger = logging.getLogger(__name__)


class StepsStyle(StrEnum):
    separate = "separate"
    cumulative = "cumulative"


# =========================================================================
# UI Sections
# =========================================================================

SECTION_CONFIGURATION = UISection(
    id="configuration",
    order=2,
    icon="settings",
    label_key="configuration",
    depends_on={"routing_mode": {"$ne": None}},
)

SECTION_STARTING = UISection(
    id="starting",
    order=3,
    icon="location",
    label_key="starting_points",
    depends_on={"routing_mode": {"$ne": None}},
)


SECTION_RESULT_CATCHMENT = UISection(
    id="result",
    order=7,
    icon="save",
    label="Result layer",
    label_de="Ergebnis-Layer",
    depends_on={"routing_mode": {"$ne": None}},
)

SECTION_SCENARIO = UISection(
    id="scenario",
    order=8,
    icon="git-branch",
    label_key="scenario",
    collapsible=True,
    collapsed=True,
    depends_on={"routing_mode": {"$ne": None}},
)

# =========================================================================
# Label Mappings
# =========================================================================

STEPS_STYLE_LABELS: dict[str, str] = {
    "separate": "enums.steps_style.separate",
    "cumulative": "enums.steps_style.cumulative",
}

COST_TYPE_LABELS: dict[str, str] = {
    "time": "enums.cost_type.time",
    "distance": "enums.cost_type.distance",
}

COST_TYPE_ICONS: dict[str, str] = {
    "time": "clock",
    "distance": "ruler-horizontal",
}

PT_MODE_ICONS: dict[str, str] = {
    "bus": "bus",
    "tram": "tram",
    "rail": "rail",
    "subway": "subway",
    "ferry": "ferry",
    "cable_car": "cable-car",
    "gondola": "gondola",
    "funicular": "funicular",
}

PT_MODE_LABELS: dict[str, str] = {
    "bus": "routing_modes.bus",
    "tram": "routing_modes.tram",
    "rail": "routing_modes.rail",
    "subway": "routing_modes.subway",
    "ferry": "routing_modes.ferry",
    "cable_car": "routing_modes.cable_car",
    "gondola": "routing_modes.gondola",
    "funicular": "routing_modes.funicular",
}

ACCESS_EGRESS_MODE_LABELS: dict[str, str] = {
    "walk": "routing_modes.walk",
    "bicycle": "routing_modes.bicycle",
    "pedelec": "routing_modes.pedelec",
    "car": "routing_modes.car",
}

ACCESS_EGRESS_MODE_ICONS: dict[str, str] = {
    "walk": "run",
    "bicycle": "bicycle",
    "pedelec": "pedelec",
    "car": "car",
}

OUTPUT_FORMAT_LABELS: dict[str, str] = {
    "geojson": "GeoJSON",
    "parquet": "Parquet",
}

CATCHMENT_TYPE_LABELS: dict[str, str] = {
    **CATCHMENT_AREA_TYPE_LABELS,
    "point_grid": "enums.catchment_area_type.point_grid",
}


# =========================================================================
# Windmill Params
# =========================================================================


class CatchmentAreaV2WindmillParams(ToolInputBase):
    """Catchment areas show how far people can travel within a set travel time or distance from one or more selected points.

    This schema extends ToolInputBase with catchment area specific parameters.
    The frontend renders this dynamically based on x-ui metadata.
    """

    model_config = {
        "json_schema_extra": ui_sections(
            SECTION_ROUTING,
            SECTION_CONFIGURATION,
            SECTION_STARTING,
            SECTION_RESULT_CATCHMENT,
            SECTION_SCENARIO,
        )
    }

    # =========================================================================
    # Result Section
    # =========================================================================

    result_layer_name: str | None = Field(
        default=get_default_layer_name("catchment_area", "en"),
        description="Name for the catchment area result layer.",
        json_schema_extra=ui_field(
            section="result",
            field_order=1,
            label_key="result_layer_name",
            widget_options={
                "default_en": get_default_layer_name("catchment_area", "en"),
                "default_de": get_default_layer_name("catchment_area", "de"),
            },
        ),
    )

    starting_points_layer_name: str | None = Field(
        default=get_default_layer_name("catchment_area_starting_points", "en"),
        description="Name for the starting points layer.",
        json_schema_extra=ui_field(
            section="result",
            field_order=2,
            label_key="starting_points_layer_name",
            widget_options={
                "default_en": get_default_layer_name(
                    "catchment_area_starting_points", "en"
                ),
                "default_de": get_default_layer_name(
                    "catchment_area_starting_points", "de"
                ),
            },
        ),
    )

    # =========================================================================
    # Routing Section
    # =========================================================================

    routing_mode: CatchmentAreaRoutingMode = Field(
        ...,
        description="Transport mode for the catchment area calculation.",
        json_schema_extra=ui_field(
            section="routing",
            field_order=1,
            label_key="routing_mode",
            enum_icons=ROUTING_MODE_ICONS,
            enum_labels=ROUTING_MODE_LABELS,
        ),
    )

    pt_modes: list[PTMode] | None = Field(
        default=list(PTMode),
        description="Public transport modes to include.",
        json_schema_extra=ui_field(
            section="routing",
            field_order=2,
            label_key="choose_pt_mode",
            enum_labels=PT_MODE_LABELS,
            visible_when={"routing_mode": "pt"},
        ),
    )

    pt_max_transfers: int = Field(
        default=5,
        description="Maximum number of transit transfers.",
        json_schema_extra=ui_field(
            section="routing",
            field_order=3,
            label="Max. transfers",
            label_de="Max. Umstiege",
            visible_when={"routing_mode": "pt"},
            widget_options={
                "max_value_from": {
                    "fields": [],
                    "message": "Max transfers must be between 0 and 10",
                    "max": 10,
                    "min": 0,
                },
            },
        ),
    )

    # =========================================================================
    # Starting Points Section
    # =========================================================================

    starting_points: StartingPoints = Field(
        ...,
        description="Starting point(s) for the catchment area.",
        json_schema_extra=ui_field(
            section="starting",
            field_order=1,
            widget="starting-points",
            widget_options={"geometry_types": ["Point", "MultiPoint"]},
        ),
    )

    # =========================================================================
    # Configuration Section
    # =========================================================================

    cost_type: CostType = Field(
        default=CostType.time,
        description="Measure catchment area by travel time or distance.",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=1,
            label_key="measure_type",
            enum_labels=COST_TYPE_LABELS,
            enum_icons=COST_TYPE_ICONS,
            inline_group="cost_config",
            visible_when={
                "routing_mode": {"$in": ["walking", "bicycle", "pedelec", "car"]}
            },
        ),
    )

    # Time budget — active mobility
    max_cost_time_active: int = Field(
        default=15,
        description="Maximum travel time in minutes.",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=2,
            label_key="limit",
            inline_group="cost_config",
            inline_flex="1 0 0",
            widget_options={
                "max_value_from": {
                    "fields": [],
                    "message": "Active mobility travel time must be between 1 and 45 minutes",
                    "max": 45,
                    "min": 1,
                },
            },
            visible_when={
                "$and": [
                    {"routing_mode": {"$in": ["walking", "bicycle", "pedelec"]}},
                    {"cost_type": "time"},
                ]
            },
        ),
    )

    # Time budget — car
    max_cost_time_car: int = Field(
        default=30,
        description="Maximum travel time in minutes.",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=2,
            label_key="limit",
            inline_group="cost_config",
            inline_flex="1 0 0",
            widget_options={
                "max_value_from": {
                    "fields": [],
                    "message": "Car travel time must be between 1 and 90 minutes",
                    "max": 90,
                    "min": 1,
                },
            },
            visible_when={
                "$and": [
                    {"routing_mode": "car"},
                    {"cost_type": "time"},
                ]
            },
        ),
    )

    # Time budget — PT (always time-based, no cost_type selector)
    max_cost_time_pt: int = Field(
        default=30,
        description="Maximum travel time in minutes.",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=2,
            label="Travel time limit (min)",
            label_de="Reisezeitlimit (Min)",
            widget_options={
                "max_value_from": {
                    "fields": [],
                    "message": "PT travel time must be between 1 and 90 minutes",
                    "max": 90,
                    "min": 1,
                },
            },
            visible_when={"routing_mode": "pt"},
        ),
    )

    # Distance budget
    max_cost_distance: int = Field(
        default=500,
        description="Maximum distance in meters.",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=2,
            label_key="limit",
            inline_group="cost_config",
            inline_flex="1 0 0",
            widget_options={
                "max_value_from": {
                    "fields": [],
                    "message": "Distance must be between 50 and 20,000 meters",
                    "max": 20000,
                    "min": 50,
                },
            },
            visible_when={
                "$and": [
                    {"routing_mode": {"$in": ["walking", "bicycle", "pedelec", "car"]}},
                    {"cost_type": "distance"},
                ]
            },
        ),
    )

    speed: float = Field(
        default=5,
        description="Travel speed in km/h.",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=3,
            label_key="speed",
            visible_when={
                "routing_mode": {"$in": ["walking", "bicycle", "pedelec"]},
                "cost_type": "time",
            },
            widget_options={
                "default_by_field": {
                    "field": "routing_mode",
                    "values": {
                        "walking": 5,
                        "bicycle": 15,
                        "pedelec": 23,
                    },
                }
            },
        ),
    )

    steps: int = Field(
        default=5,
        description="Number of isochrone steps/intervals.",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=6,
            label_key="steps",
            visible_when={
                "catchment_area_type": {"$in": ["polygon", "network"]}
            },
            widget_options={
                "max_value_from": {
                    "fields": [
                        {"field": "max_cost_time_pt", "when": {"routing_mode": "pt"}},
                        {"field": "max_cost_time_car", "when": {"routing_mode": "car", "cost_type": "time"}},
                        {"field": "max_cost_distance", "when": {"cost_type": "distance"}},
                        {"field": "max_cost_time_active"},
                    ],
                    "message": "Number of steps cannot exceed the limit",
                    "max": 9,
                },
            },
        ),
    )

    step_sizes: list[int] | None = Field(
        default=None,
        description="Step size intervals. Auto-computed from steps and limit, editable.",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=7,
            label_key="step_sizes",
            visible_when={
                "catchment_area_type": {"$in": ["polygon", "network"]}
            },
            widget="chips",
            widget_options={
                "compute_from": {
                    "steps_field": "steps",
                    "limit_fields": [
                        {"field": "max_cost_time_pt", "when": {"routing_mode": "pt"}},
                        {"field": "max_cost_time_car", "when": {"routing_mode": "car", "cost_type": "time"}},
                        {"field": "max_cost_distance", "when": {"cost_type": "distance"}},
                        {"field": "max_cost_time_active"},
                    ],
                },
            },
        ),
    )

    # PT time window
    pt_day: Weekday = Field(
        default=Weekday.weekday,
        description="Day type for PT schedule.",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=8,
            label_key="weekday",
            visible_when={"routing_mode": "pt"},
        ),
    )
    pt_start_time: int = Field(
        default=25200,
        description="PT window start (seconds from midnight).",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=9,
            label_key="from_time",
            widget="time-picker",
            inline_group="pt_time_window",
            inline_flex="1 0 0",
            visible_when={"routing_mode": "pt"},
        ),
    )
    pt_end_time: int = Field(
        default=32400,
        description="PT window end (seconds from midnight).",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=10,
            label_key="to_time",
            widget="time-picker",
            inline_group="pt_time_window",
            inline_flex="1 0 0",
            visible_when={"routing_mode": "pt"},
        ),
    )

    # =========================================================================
    # PT Access & Egress (under Advanced in Configuration)
    # =========================================================================

    pt_access_mode: AccessEgressMode = Field(
        default=AccessEgressMode.walk,
        description="Mode to reach transit stops.",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=20,
            label_key="access_mode",
            group_label="Access leg",
            enum_icons=ACCESS_EGRESS_MODE_ICONS,
            enum_labels=ACCESS_EGRESS_MODE_LABELS,
            visible_when={
                "$and": [
                    {"routing_mode": "pt"},
                    {"show_advanced": True},
                ]
            },
        ),
    )

    pt_access_cost_type: CostType = Field(
        default=CostType.time,
        description="Access leg cost type.",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=21,
            label_key="measure_type",
            enum_labels=COST_TYPE_LABELS,
            enum_icons=COST_TYPE_ICONS,
            inline_group="pt_access_cost",
            visible_when={
                "$and": [
                    {"routing_mode": "pt"},
                    {"show_advanced": True},
                ]
            },
        ),
    )

    pt_access_max_cost_time: int = Field(
        default=15,
        description="Access leg budget in minutes.",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=22,
            label_key="limit",
            inline_group="pt_access_cost",
            inline_flex="1 0 0",
            visible_when={
                "$and": [
                    {"routing_mode": "pt"},
                    {"show_advanced": True},
                    {"pt_access_cost_type": "time"},
                ]
            },
            widget_options={
                "max_value_from": {
                    "fields": [
                        {"field": "max_cost_time_pt"},
                    ],
                    "message": "access_budget_exceeds_limit",
                    "min": 1,
                },
            },
        ),
    )

    pt_access_max_cost_distance: int = Field(
        default=500,
        description="Access leg budget in meters.",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=22,
            label_key="limit",
            inline_group="pt_access_cost",
            inline_flex="1 0 0",
            visible_when={
                "$and": [
                    {"routing_mode": "pt"},
                    {"show_advanced": True},
                    {"pt_access_cost_type": "distance"},
                ]
            },
            widget_options={
                "max_value_from": {
                    "fields": [],
                    "message": "access_budget_exceeds_limit",
                    "min": 50,
                    "max": 20000,
                },
            },
        ),
    )

    pt_access_speed: float = Field(
        default=0.0,
        description="Access leg speed in km/h (0 = use default speed).",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=23,
            label="Speed (km/h)",
            label_de="Geschw. (km/h)",
            widget_options={"placeholder": "Default"},
            visible_when={
                "$and": [
                    {"routing_mode": "pt"},
                    {"show_advanced": True},
                    {"pt_access_cost_type": "time"},
                ]
            },
        ),
    )

    pt_egress_mode: AccessEgressMode = Field(
        default=AccessEgressMode.walk,
        description="Mode from transit stops to destination.",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=24,
            label_key="pt_egress_mode",
            group_label="Egress leg",
            enum_icons=ACCESS_EGRESS_MODE_ICONS,
            enum_labels=ACCESS_EGRESS_MODE_LABELS,
            visible_when={
                "$and": [
                    {"routing_mode": "pt"},
                    {"show_advanced": True},
                ]
            },
        ),
    )

    pt_egress_cost_type: CostType = Field(
        default=CostType.time,
        description="Egress leg cost type.",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=25,
            label_key="measure_type",
            enum_labels=COST_TYPE_LABELS,
            enum_icons=COST_TYPE_ICONS,
            inline_group="pt_egress_cost",
            visible_when={
                "$and": [
                    {"routing_mode": "pt"},
                    {"show_advanced": True},
                ]
            },
        ),
    )

    pt_egress_max_cost_time: int = Field(
        default=15,
        description="Egress leg budget in minutes.",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=26,
            label_key="limit",
            inline_group="pt_egress_cost",
            inline_flex="1 0 0",
            visible_when={
                "$and": [
                    {"routing_mode": "pt"},
                    {"show_advanced": True},
                    {"pt_egress_cost_type": "time"},
                ]
            },
            widget_options={
                "max_value_from": {
                    "fields": [
                        {"field": "max_cost_time_pt"},
                    ],
                    "message": "egress_budget_exceeds_limit",
                    "min": 1,
                },
            },
        ),
    )

    pt_egress_max_cost_distance: int = Field(
        default=500,
        description="Egress leg budget in meters.",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=26,
            label_key="limit",
            inline_group="pt_egress_cost",
            inline_flex="1 0 0",
            visible_when={
                "$and": [
                    {"routing_mode": "pt"},
                    {"show_advanced": True},
                    {"pt_egress_cost_type": "distance"},
                ]
            },
            widget_options={
                "max_value_from": {
                    "fields": [],
                    "message": "egress_budget_exceeds_limit",
                    "min": 50,
                    "max": 20000,
                },
            },
        ),
    )

    pt_egress_speed: float = Field(
        default=0.0,
        description="Egress leg speed in km/h (0 = use default speed).",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=27,
            label="Speed (km/h)",
            label_de="Geschw. (km/h)",
            widget_options={"placeholder": "Default"},
            visible_when={
                "$and": [
                    {"routing_mode": "pt"},
                    {"show_advanced": True},
                    {"pt_egress_cost_type": "time"},
                ]
            },
        ),
    )

    # =========================================================================
    # Advanced Options (inline toggle within Configuration)
    # =========================================================================

    show_advanced: bool = Field(
        default=False,
        description="Show advanced configuration options.",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=15,
            label_key="advanced_options",
        ),
    )

    catchment_area_type: CatchmentType = Field(
        default=CatchmentType.polygon,
        description="Output geometry type.",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=4,
            label_key="catchment_area_type",
            enum_labels=CATCHMENT_TYPE_LABELS,
        ),
    )

    steps_style: StepsStyle = Field(
        default=StepsStyle.separate,
        description="How steps are displayed in the output.",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=16,
            label_key="steps_style",
            enum_labels=STEPS_STYLE_LABELS,
            visible_when={
                "$and": [
                    {"show_advanced": True},
                    {"catchment_area_type": "polygon"},
                ]
            },
        ),
    )


    point_grid_layer_id: str | None = Field(
        default=None,
        description="Point layer to use as grid for point_grid catchment type.",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=5,
            label_key="point_grid_layer",
            widget="layer-selector",
            widget_options={"geometry_types": ["Point", "MultiPoint"]},
            visible_when={"catchment_area_type": "point_grid"},
        ),
    )

    point_grid_layer_filter: dict[str, Any] | None = Field(
        default=None,
        description="CQL2-JSON filter for point grid layer.",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=17,
            hidden=True,
        ),
    )

    output_format: OutputFormat = Field(
        default=OutputFormat.parquet,
        description="Output file format.",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=18,
            hidden=True,
        ),
    )

    # =========================================================================
    # Validators
    # =========================================================================

    @model_validator(mode="after")
    def validate_distance_limit_by_mode(self: Self) -> Self:
        if self.cost_type != CostType.distance:
            return self
        if self.routing_mode == CatchmentAreaRoutingMode.car:
            if self.max_cost_distance > 100000:
                raise ValueError("Car distance must be ≤ 100000 meters.")
        elif self.max_cost_distance > 20000:
            raise ValueError("Active mobility distance must be ≤ 20000 meters.")
        return self

    def resolve_max_cost(self: Self) -> float:
        """Resolve the effective max_cost from mode-specific UI fields."""
        if self.cost_type == CostType.distance:
            return float(self.max_cost_distance)
        if self.routing_mode == CatchmentAreaRoutingMode.pt:
            return float(self.max_cost_time_pt)
        if self.routing_mode == CatchmentAreaRoutingMode.car:
            return float(self.max_cost_time_car)
        return float(self.max_cost_time_active)



# =========================================================================
# Tool Runner
# =========================================================================


class CatchmentAreaV2ToolRunner(CatchmentAreaToolRunner):
    """Catchment Area V2 tool runner for Windmill — local C++ routing backend."""

    tool_class = CatchmentAreaToolV2
    default_output_name = get_default_layer_name("catchment_area", "en")

    def process(
        self: Self,
        params: CatchmentAreaV2WindmillParams,
        temp_dir: Path,
    ) -> tuple[Path, DatasetMetadata]:
        """Run catchment area V2 analysis."""
        output_path = temp_dir / "output.parquet"

        latitudes, longitudes = self._get_starting_coordinates(
            params.starting_points,
            params.user_id,
            scenario_id=params.scenario_id,
            project_id=params.project_id,
        )

        # Build PT time window
        time_window = None
        if params.routing_mode == CatchmentAreaRoutingMode.pt:
            time_window = PTTimeWindow(
                weekday=params.pt_day,
                from_time=params.pt_start_time,
                to_time=params.pt_end_time,
            )

        max_cost = params.resolve_max_cost()

        # Export point grid layer to parquet if needed
        grid_points_path = None
        if (
            params.catchment_area_type == CatchmentType.point_grid
            and params.point_grid_layer_id
        ):
            raw_layer_path = self.export_layer_to_parquet(
                params.point_grid_layer_id,
                params.user_id,
                cql_filter=params.point_grid_layer_filter,
                scenario_id=params.scenario_id,
                project_id=params.project_id,
            )
            # Convert to the format expected by C++: id, x_3857, y_3857
            grid_parquet = tempfile.NamedTemporaryFile(
                suffix=".parquet", delete=False
            ).name
            con = duckdb.connect()
            con.execute("INSTALL spatial; LOAD spatial;")
            cols = con.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{raw_layer_path}')"
            ).fetchall()
            geom_col = next(
                (c[0] for c in cols if "GEOMETRY" in c[1].upper() or c[0] in ("geom", "geometry")),
                "geometry",
            )
            # Convert WGS84 lon/lat to Web Mercator (EPSG:3857)
            R = 6378137.0
            con.execute(f"""
                COPY (
                    SELECT
                        ROW_NUMBER() OVER () AS id,
                        ST_X("{geom_col}") * PI() / 180.0 * {R} AS x_3857,
                        LN(TAN(PI() / 4.0 + ST_Y("{geom_col}") * PI() / 360.0)) * {R} AS y_3857
                    FROM read_parquet('{raw_layer_path}')
                    WHERE "{geom_col}" IS NOT NULL
                ) TO '{grid_parquet}' (FORMAT PARQUET)
            """)
            con.close()
            grid_points_path = grid_parquet

        # Map routing mode to V2 enum
        routing_mode_map = {
            "walking": RoutingMode.walking,
            "bicycle": RoutingMode.bicycle,
            "pedelec": RoutingMode.pedelec,
            "car": RoutingMode.car,
            "pt": RoutingMode.pt,
        }
        routing_mode_value = (
            params.routing_mode.value
            if hasattr(params.routing_mode, "value")
            else params.routing_mode
        )

        analysis_params = CatchmentAreaV2Params(
            latitude=latitudes,
            longitude=longitudes,
            routing_mode=routing_mode_map[routing_mode_value],
            cost_type=params.cost_type,
            max_cost=max_cost,
            steps=params.steps,
            speed=params.speed,
            cutoffs=params.step_sizes,
            grid_points_path=grid_points_path,
            # PT
            transit_modes=params.pt_modes,
            time_window=time_window,
            max_transfers=params.pt_max_transfers,
            # PT access/egress
            access_mode=params.pt_access_mode,
            egress_mode=params.pt_egress_mode,
            access_cost_type=params.pt_access_cost_type,
            egress_cost_type=params.pt_egress_cost_type,
            access_max_cost=(
                params.pt_access_max_cost_distance
                if params.pt_access_cost_type == CostType.distance
                else params.pt_access_max_cost_time
            ),
            egress_max_cost=(
                params.pt_egress_max_cost_distance
                if params.pt_egress_cost_type == CostType.distance
                else params.pt_egress_max_cost_time
            ),
            access_speed=params.pt_access_speed,
            egress_speed=params.pt_egress_speed,
            # Output
            catchment_type=params.catchment_area_type,
            polygon_difference=params.steps_style == StepsStyle.separate,
            output_format=params.output_format,
            output_path=str(output_path),
        )

        tool = self.tool_class()
        try:
            results = tool.run(analysis_params)
            result_path, metadata = results[0]

            if not self._starting_points_from_layer:
                starting_points_path = temp_dir / "starting_points.parquet"
                self._create_starting_points_parquet(
                    latitudes=latitudes,
                    longitudes=longitudes,
                    output_path=starting_points_path,
                )
                if starting_points_path.exists():
                    self._starting_points_parquet = starting_points_path

            return Path(result_path), metadata
        finally:
            tool.cleanup()


def main(params: CatchmentAreaV2WindmillParams) -> dict:
    """Windmill entry point for catchment area V2 tool."""
    runner = CatchmentAreaV2ToolRunner()
    runner.init_from_env()

    try:
        return runner.run(params)
    finally:
        runner.cleanup()
