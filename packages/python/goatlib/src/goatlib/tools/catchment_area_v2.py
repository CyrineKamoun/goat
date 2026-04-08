"""Catchment Area V2 tool for Windmill.

Uses the local C++ routing backend for all modes. Mirrors the v1 tool runner
structure but builds CatchmentAreaV2Params with cost_type/max_cost.
"""

import logging
from pathlib import Path
from typing import Any, Self

from pydantic import Field, model_validator

from goatlib.analysis.accessibility import CatchmentAreaToolV2
from goatlib.analysis.schemas.catchment_area import (
    CATCHMENT_AREA_TYPE_LABELS,
    ROUTING_MODE_ICONS,
    ROUTING_MODE_LABELS,
    SPEED_LABELS,
    TRAVEL_TIME_LABELS,
    CatchmentAreaRoutingMode,
    StartingPoints,
    TravelTimeLimitActiveMobility,
    TravelTimeLimitMotorized,
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

SECTION_PT_ACCESS_EGRESS = UISection(
    id="pt_access_egress",
    order=4,
    icon="walk",
    label="Access & Egress",
    label_de="Zugang & Abgang",
    collapsible=True,
    collapsed=True,
    depends_on={"routing_mode": "pt"},
)

SECTION_ADVANCED = UISection(
    id="advanced",
    order=5,
    icon="sliders",
    label="Advanced",
    label_de="Erweitert",
    collapsible=True,
    collapsed=True,
    depends_on={"routing_mode": {"$ne": None}},
)

SECTION_RESULT_CATCHMENT = UISection(
    id="result",
    order=7,
    icon="save",
    label="Result Layer",
    label_de="Ergebnisebene",
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

COST_TYPE_LABELS: dict[str, str] = {
    "time": "enums.measure_type.time",
    "distance": "enums.measure_type.distance",
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
    """Parameters for catchment area tool via Windmill/GeoAPI.

    This schema extends ToolInputBase with catchment area specific parameters.
    The frontend renders this dynamically based on x-ui metadata.
    """

    model_config = {
        "json_schema_extra": ui_sections(
            SECTION_ROUTING,
            SECTION_CONFIGURATION,
            SECTION_STARTING,
            SECTION_PT_ACCESS_EGRESS,
            SECTION_ADVANCED,
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
            label_key="routing_pt_mode",
            enum_icons=PT_MODE_ICONS,
            enum_labels=PT_MODE_LABELS,
            visible_when={"routing_mode": "pt"},
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
            visible_when={
                "routing_mode": {"$in": ["walking", "bicycle", "pedelec", "car"]}
            },
        ),
    )

    # Time budget — active mobility: 3-45 min
    max_cost_time_active: TravelTimeLimitActiveMobility = Field(
        default=15,
        description="Maximum travel time in minutes.",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=2,
            label_key="max_traveltime",
            enum_labels=TRAVEL_TIME_LABELS,
            visible_when={
                "$and": [
                    {"routing_mode": {"$in": ["walking", "bicycle", "pedelec"]}},
                    {"cost_type": "time"},
                ]
            },
        ),
    )

    # Time budget — car: 3-90 min
    max_cost_time_car: TravelTimeLimitMotorized = Field(
        default=30,
        description="Maximum travel time in minutes.",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=2,
            label_key="max_traveltime",
            enum_labels=TRAVEL_TIME_LABELS,
            visible_when={
                "$and": [
                    {"routing_mode": "car"},
                    {"cost_type": "time"},
                ]
            },
        ),
    )

    # Time budget — PT: 3-90 min (always time-based)
    max_cost_time_pt: TravelTimeLimitMotorized = Field(
        default=30,
        description="Maximum travel time in minutes.",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=2,
            label_key="max_traveltime",
            enum_labels=TRAVEL_TIME_LABELS,
            visible_when={"routing_mode": "pt"},
        ),
    )

    # Distance budget
    max_cost_distance: int = Field(
        default=500,
        ge=50,
        le=100000,
        description="Maximum distance in meters.",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=2,
            label_key="max_distance",
            widget="slider",
            widget_options={"min": 50, "max": 100000, "step": 50},
            visible_when={
                "$and": [
                    {"routing_mode": {"$in": ["walking", "bicycle", "pedelec", "car"]}},
                    {"cost_type": "distance"},
                ]
            },
        ),
    )

    steps: int = Field(
        default=5,
        ge=1,
        le=20,
        description="Number of isochrone steps/intervals.",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=3,
            label_key="steps",
        ),
    )

    speed: float = Field(
        default=5,
        ge=1.0,
        le=50.0,
        description="Travel speed in km/h.",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=4,
            label_key="speed",
            enum_labels=SPEED_LABELS,
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

    # PT time window
    pt_day: Weekday = Field(
        default=Weekday.weekday,
        description="Day type for PT schedule.",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=5,
            label="Day",
            label_de="Tag",
            visible_when={"routing_mode": "pt"},
        ),
    )
    pt_start_time: int = Field(
        default=25200,
        description="PT window start (seconds from midnight).",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=6,
            label="Departure from",
            label_de="Abfahrt von",
            widget="time-picker",
            visible_when={"routing_mode": "pt"},
        ),
    )
    pt_end_time: int = Field(
        default=32400,
        description="PT window end (seconds from midnight).",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=7,
            label="Departure to",
            label_de="Abfahrt bis",
            widget="time-picker",
            visible_when={"routing_mode": "pt"},
        ),
    )

    # =========================================================================
    # PT Access & Egress Section
    # =========================================================================

    pt_access_mode: AccessEgressMode = Field(
        default=AccessEgressMode.walk,
        description="Mode to reach transit stops.",
        json_schema_extra=ui_field(
            section="pt_access_egress",
            field_order=1,
            label="Access mode",
            label_de="Zugangsmodus",
            enum_icons=ACCESS_EGRESS_MODE_ICONS,
            enum_labels=ACCESS_EGRESS_MODE_LABELS,
        ),
    )

    pt_access_cost_type: CostType = Field(
        default=CostType.time,
        description="Access leg cost type.",
        json_schema_extra=ui_field(
            section="pt_access_egress",
            field_order=2,
            label="Access cost type",
            label_de="Zugangs-Kostentyp",
            enum_labels=COST_TYPE_LABELS,
            enum_icons=COST_TYPE_ICONS,
        ),
    )

    pt_access_max_cost: float = Field(
        default=0.0,
        ge=0.0,
        description="Access leg budget: minutes (time) or meters (distance). 0 = use overall budget.",
        json_schema_extra=ui_field(
            section="pt_access_egress",
            field_order=3,
            label="Access max cost",
            label_de="Zugangs-Maximalkosten",
            widget="slider",
            widget_options={"min": 0, "max": 60, "step": 1},
        ),
    )

    pt_access_speed: float = Field(
        default=0.0,
        ge=0.0,
        le=50.0,
        description="Access leg speed in km/h (0 = use main speed).",
        json_schema_extra=ui_field(
            section="pt_access_egress",
            field_order=4,
            label="Access speed (km/h)",
            label_de="Zugangsgeschwindigkeit (km/h)",
            visible_when={"pt_access_cost_type": "time"},
        ),
    )

    pt_egress_mode: AccessEgressMode = Field(
        default=AccessEgressMode.walk,
        description="Mode from transit stops to destination.",
        json_schema_extra=ui_field(
            section="pt_access_egress",
            field_order=5,
            label="Egress mode",
            label_de="Abgangsmodus",
            enum_icons=ACCESS_EGRESS_MODE_ICONS,
            enum_labels=ACCESS_EGRESS_MODE_LABELS,
        ),
    )

    pt_egress_cost_type: CostType = Field(
        default=CostType.time,
        description="Egress leg cost type.",
        json_schema_extra=ui_field(
            section="pt_access_egress",
            field_order=6,
            label="Egress cost type",
            label_de="Abgangs-Kostentyp",
            enum_labels=COST_TYPE_LABELS,
            enum_icons=COST_TYPE_ICONS,
        ),
    )

    pt_egress_max_cost: float = Field(
        default=0.0,
        ge=0.0,
        description="Egress leg budget: minutes (time) or meters (distance). 0 = use overall budget.",
        json_schema_extra=ui_field(
            section="pt_access_egress",
            field_order=7,
            label="Egress max cost",
            label_de="Abgangs-Maximalkosten",
            widget="slider",
            widget_options={"min": 0, "max": 60, "step": 1},
        ),
    )

    pt_egress_speed: float = Field(
        default=0.0,
        ge=0.0,
        le=50.0,
        description="Egress leg speed in km/h (0 = use main speed).",
        json_schema_extra=ui_field(
            section="pt_access_egress",
            field_order=8,
            label="Egress speed (km/h)",
            label_de="Abgangsgeschwindigkeit (km/h)",
            visible_when={"pt_egress_cost_type": "time"},
        ),
    )

    pt_max_transfers: int = Field(
        default=5,
        ge=0,
        le=10,
        description="Maximum number of transit transfers.",
        json_schema_extra=ui_field(
            section="pt_access_egress",
            field_order=9,
            label="Max transfers",
            label_de="Max. Umstiege",
        ),
    )

    # =========================================================================
    # Advanced Section
    # =========================================================================

    custom_cutoffs: str | None = Field(
        default=None,
        description="Custom step thresholds (comma-separated, e.g. '5,10,15,30'). Overrides steps.",
        json_schema_extra=ui_field(
            section="advanced",
            field_order=1,
            label="Custom cutoffs",
            label_de="Benutzerdefinierte Stufen",
        ),
    )

    catchment_area_type: CatchmentType = Field(
        default=CatchmentType.polygon,
        description="Output geometry type.",
        json_schema_extra=ui_field(
            section="advanced",
            field_order=2,
            label_key="catchment_area_type",
            enum_labels=CATCHMENT_TYPE_LABELS,
        ),
    )

    polygon_difference: bool = Field(
        default=True,
        description="Whether to compute difference between time steps.",
        json_schema_extra=ui_field(
            section="advanced",
            field_order=3,
            label="Polygon difference",
            label_de="Polygondifferenz",
            visible_when={"catchment_area_type": "polygon"},
        ),
    )

    output_format: OutputFormat = Field(
        default=OutputFormat.parquet,
        description="Output file format.",
        json_schema_extra=ui_field(
            section="advanced",
            field_order=4,
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

    def resolve_cutoffs(self: Self) -> list[int] | None:
        """Parse custom cutoffs from comma-separated string."""
        if not self.custom_cutoffs:
            return None
        try:
            return sorted(int(x.strip()) for x in self.custom_cutoffs.split(",") if x.strip())
        except ValueError:
            raise ValueError("Custom cutoffs must be comma-separated integers (e.g. '5,10,15,30')")


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
        cutoffs = params.resolve_cutoffs()

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
            cutoffs=cutoffs,
            # PT
            transit_modes=params.pt_modes,
            time_window=time_window,
            max_transfers=params.pt_max_transfers,
            # PT access/egress
            access_mode=params.pt_access_mode,
            egress_mode=params.pt_egress_mode,
            access_cost_type=params.pt_access_cost_type,
            egress_cost_type=params.pt_egress_cost_type,
            access_max_cost=params.pt_access_max_cost,
            egress_max_cost=params.pt_egress_max_cost,
            access_speed=params.pt_access_speed,
            egress_speed=params.pt_egress_speed,
            # Output
            catchment_type=params.catchment_area_type,
            polygon_difference=params.polygon_difference,
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
