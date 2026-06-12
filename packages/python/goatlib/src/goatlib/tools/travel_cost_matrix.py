"""Travel Cost Matrix tool for Windmill.

Computes many-to-many travel costs between origin and destination point layers
via the local C++ routing backend. Produces two outputs:
1. Matrix table (non-geom): origin_id, destination_id, cost
2. Destination points (geom): destination geometry with min_cost from any origin
"""

import asyncio
import logging
import math
import tempfile
import uuid as uuid_module
from pathlib import Path
from typing import Any, Self

import duckdb
from pydantic import Field

from goatlib.analysis.accessibility import TravelCostMatrixTool
from goatlib.analysis.schemas.travel_cost_matrix import (
    AccessEgressMode,
    CostType,
    PTMode,
    PTTimeWindow,
    RoutingMode,
    TravelCostMatrixParams,
    Weekday,
)
from goatlib.analysis.schemas.ui import (
    SECTION_ROUTING,
    UISection,
    ui_field,
    ui_sections,
)
from goatlib.models.io import DatasetMetadata
from goatlib.tools.base import BaseToolRunner
from goatlib.analysis.schemas.catchment_area import WEEKDAY_LABELS
from goatlib.tools.catchment_area_v2 import (
    ACCESS_EGRESS_MODE_LABELS,
    COST_TYPE_ICONS,
    COST_TYPE_LABELS,
    DEFAULT_MAX_DISTANCE_ACTIVE_M,
    DEFAULT_MAX_DISTANCE_CAR_M,
    DEFAULT_MAX_TIME_ACTIVE_MIN,
    DEFAULT_MAX_TIME_CAR_MIN,
    PT_MODE_LABELS,
    ROUTING_MODE_ICONS as _CATCHMENT_ROUTING_MODE_ICONS,
    ROUTING_MODE_LABELS as _CATCHMENT_ROUTING_MODE_LABELS,
)
from goatlib.tools.schemas import ToolInputBase, ToolOutputBase, get_default_layer_name

logger = logging.getLogger(__name__)

# Extend routing mode icons/labels with matrix-only modes
ROUTING_MODE_ICONS = {
    **_CATCHMENT_ROUTING_MODE_ICONS,
    "flight_distance": "plane",
}
ROUTING_MODE_LABELS = {
    **_CATCHMENT_ROUTING_MODE_LABELS,
    "flight_distance": "routing_modes.flight_distance",
}

# =========================================================================
# UI Sections
# =========================================================================

SECTION_INPUT = UISection(
    id="input",
    order=5,
    icon="layers",
    label_key="input",
    depends_on={"routing_mode": {"$ne": None}},
)

SECTION_CONFIGURATION = UISection(
    id="configuration",
    order=3,
    icon="settings",
    label_key="configuration",
    depends_on={"routing_mode": {"$in": ["walking", "bicycle", "pedelec", "car", "pt"]}},
)

SECTION_RESULT = UISection(
    id="result",
    order=7,
    icon="save",
    label_key="result_layer_section",
    depends_on={"routing_mode": {"$ne": None}},
)


# =========================================================================
# Windmill Params
# =========================================================================


class TravelCostMatrixWindmillParams(ToolInputBase):
    """Compute travel times and distances between origin and destination point layers.

    This schema extends ToolInputBase with travel cost matrix specific parameters.
    The frontend renders this dynamically based on x-ui metadata.
    """

    model_config = {
        "json_schema_extra": ui_sections(
            SECTION_INPUT,
            SECTION_ROUTING,
            SECTION_CONFIGURATION,
            SECTION_RESULT,
        )
    }

    # Hide the generic result_layer_name from ToolInputBase
    result_layer_name: str | None = Field(
        default=None,
        json_schema_extra=ui_field(section="result", hidden=True),
    )

    # =========================================================================
    # Result Section
    # =========================================================================

    destinations_layer_name: str | None = Field(
        default=get_default_layer_name("travel_cost_matrix_destinations", "en"),
        description="Name for the destination points result layer.",
        json_schema_extra=ui_field(
            section="result",
            field_order=1,
            label_key="destinations_layer_name",
            widget_options={
                "default_en": get_default_layer_name("travel_cost_matrix_destinations", "en"),
                "default_de": get_default_layer_name("travel_cost_matrix_destinations", "de"),
            },
        ),
    )

    matrix_layer_name: str | None = Field(
        default=get_default_layer_name("travel_cost_matrix", "en"),
        description="Name for the cost matrix table layer.",
        json_schema_extra=ui_field(
            section="result",
            field_order=2,
            label_key="matrix_layer_name",
            widget_options={
                "default_en": get_default_layer_name("travel_cost_matrix", "en"),
                "default_de": get_default_layer_name("travel_cost_matrix", "de"),
            },
        ),
    )

    # =========================================================================
    # Input Section
    # =========================================================================

    origin_layer_id: str = Field(
        ...,
        description="Layer containing origin points.",
        json_schema_extra=ui_field(
            section="input",
            field_order=1,
            label_key="origins_layer",
            group_label="groups.origins",
            widget="layer-selector",
            widget_options={"geometry_types": ["Point", "MultiPoint"]},
        ),
    )

    origin_layer_filter: dict[str, Any] | None = Field(
        None,
        description="CQL2-JSON filter for origin layer.",
        json_schema_extra=ui_field(section="input", field_order=2, hidden=True),
    )

    destination_layer_id: str = Field(
        ...,
        description="Layer containing destination points.",
        json_schema_extra=ui_field(
            section="input",
            field_order=4,
            label_key="destinations_layer",
            group_label="groups.destinations",
            widget="layer-selector",
            widget_options={"geometry_types": ["Point", "MultiPoint"]},
        ),
    )

    destination_layer_filter: dict[str, Any] | None = Field(
        None,
        description="CQL2-JSON filter for destination layer.",
        json_schema_extra=ui_field(section="input", field_order=4, hidden=True),
    )

    origin_id_column: str = Field(
        ...,
        description="Column used to label origins in the result matrix.",
        json_schema_extra=ui_field(
            section="input",
            field_order=3,
            label_key="origins_label",
            widget="field-selector",
            widget_options={"source_layer": "origin_layer_id"},
        ),
    )

    destination_id_column: str = Field(
        ...,
        description="Column used to label destinations in the result matrix.",
        json_schema_extra=ui_field(
            section="input",
            field_order=5,
            label_key="destinations_label",
            widget="field-selector",
            widget_options={"source_layer": "destination_layer_id"},
        ),
    )

    # =========================================================================
    # Routing Section
    # =========================================================================

    routing_mode: RoutingMode = Field(
        ...,
        description="Transport mode for routing.",
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

    # =========================================================================
    # Configuration Section
    # =========================================================================

    cost_type: CostType = Field(
        default=CostType.time,
        description="Measure travel cost by time or distance.",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=1,
            label_key="calculate_by",
            enum_labels=COST_TYPE_LABELS,
            enum_icons=COST_TYPE_ICONS,
            visible_when={
                "routing_mode": {"$in": ["walking", "bicycle", "pedelec", "car"]}
            },
        ),
    )

    # PT time window (always visible for PT, not behind advanced)
    pt_day: Weekday = Field(
        default=Weekday.weekday,
        description="Day type for PT schedule.",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=2,
            label_key="weekday",
            enum_labels=WEEKDAY_LABELS,
            visible_when={"routing_mode": "pt"},
        ),
    )

    pt_start_time: int = Field(
        default=25200,
        description="PT window start (seconds from midnight).",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=3,
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
            field_order=4,
            label_key="to_time",
            widget="time-picker",
            inline_group="pt_time_window",
            inline_flex="1 0 0",
            visible_when={"routing_mode": "pt"},
        ),
    )

    # =========================================================================
    # Advanced Options
    # =========================================================================

    show_advanced: bool = Field(
        default=False,
        description="Show advanced configuration options.",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=10,
            label_key="advanced_options",
            widget="advanced-toggle",
        ),
    )

    # Cost limits (advanced)
    max_cost_time_active: int | None = Field(
        default=None,
        description="Maximum travel time in minutes (optional; unbounded if unset).",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=11,
            label_key="limit_time_min",
            inline_group="cost_limit",
            inline_flex="1 0 0",
            widget_options={
                # Populate the form value when this field becomes visible (i.e.
                # routing_mode is one of these). Hidden cases (car/pt, distance)
                # leave the form state at the schema default (None), so the
                # visibility filter at submit ends up sending None.
                "default_by_field": {
                    "field": "routing_mode",
                    "values": {
                        "walking": DEFAULT_MAX_TIME_ACTIVE_MIN,
                        "bicycle": DEFAULT_MAX_TIME_ACTIVE_MIN,
                        "pedelec": DEFAULT_MAX_TIME_ACTIVE_MIN,
                    },
                },
                "max_value_from": {
                    "fields": [],
                    "message": "active_mobility_time_limit_message",
                    "max": 45,
                    "min": 1,
                },
            },
            visible_when={
                "$and": [
                    {"show_advanced": True},
                    {"routing_mode": {"$in": ["walking", "bicycle", "pedelec"]}},
                    {"cost_type": "time"},
                ]
            },
        ),
    )

    max_cost_time_car: int | None = Field(
        default=None,
        description="Maximum travel time in minutes (optional; unbounded if unset).",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=11,
            label_key="limit_time_min",
            inline_group="cost_limit",
            inline_flex="1 0 0",
            widget_options={
                "default_by_field": {
                    "field": "routing_mode",
                    "values": {"car": DEFAULT_MAX_TIME_CAR_MIN},
                },
                "max_value_from": {
                    "fields": [],
                    "message": "car_time_limit_message",
                    "max": 90,
                    "min": 1,
                },
            },
            visible_when={
                "$and": [
                    {"show_advanced": True},
                    {"routing_mode": "car"},
                    {"cost_type": "time"},
                ]
            },
        ),
    )

    max_cost_time_pt: int = Field(
        default=30,
        description="Maximum travel time in minutes.",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=5,
            label_key="travel_time_limit_min",
            widget_options={
                "max_value_from": {
                    "fields": [],
                    "message": "pt_time_limit_message",
                    "max": 90,
                    "min": 1,
                },
            },
            visible_when={"routing_mode": "pt"},
        ),
    )

    max_cost_distance: int | None = Field(
        default=None,
        description="Maximum distance in meters (optional; unbounded if unset).",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=11,
            label_key="limit_distance_m",
            inline_group="cost_limit",
            inline_flex="1 0 0",
            widget_options={
                "default_by_field": {
                    "field": "routing_mode",
                    "values": {
                        "walking": DEFAULT_MAX_DISTANCE_ACTIVE_M,
                        "bicycle": DEFAULT_MAX_DISTANCE_ACTIVE_M,
                        "pedelec": DEFAULT_MAX_DISTANCE_ACTIVE_M,
                    },
                },
                "max_value_from": {
                    "fields": [],
                    "message": "active_mobility_distance_limit_message",
                    "max": 20000,
                    "min": 50,
                },
            },
            visible_when={
                "$and": [
                    {"show_advanced": True},
                    {"routing_mode": {"$in": ["walking", "bicycle", "pedelec"]}},
                    {"cost_type": "distance"},
                ]
            },
        ),
    )

    max_cost_distance_car: int | None = Field(
        default=None,
        description="Maximum distance in meters (optional; unbounded if unset).",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=11,
            label_key="limit_distance_m",
            inline_group="cost_limit",
            inline_flex="1 0 0",
            widget_options={
                "default_by_field": {
                    "field": "routing_mode",
                    "values": {"car": DEFAULT_MAX_DISTANCE_CAR_M},
                },
                "max_value_from": {
                    "fields": [],
                    "message": "car_distance_limit_message",
                    "max": 100000,
                    "min": 50,
                },
            },
            visible_when={
                "$and": [
                    {"show_advanced": True},
                    {"routing_mode": "car"},
                    {"cost_type": "distance"},
                ]
            },
        ),
    )

    speed: float | None = Field(
        default=None,
        description="Travel speed in km/h. None when the routing mode doesn't "
                    "use a user-supplied speed (PT/Car/flight_distance).",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=5,
            label_key="speed",
            visible_when={
                "$and": [
                    {"routing_mode": {"$in": ["walking", "bicycle", "pedelec"]}},
                    {"cost_type": "time"},
                ]
            },
            widget_options={
                "default_by_field": {
                    "field": "routing_mode",
                    "values": {"walking": 5, "bicycle": 15, "pedelec": 23},
                },
                "max_value_from": {
                    "fields": [
                        {"value": 30, "when": {"routing_mode": "walking"},
                         "message": "walking_speed_limit_message"},
                        {"value": 60, "when": {"routing_mode": "bicycle"},
                         "message": "bicycle_speed_limit_message"},
                        {"value": 60, "when": {"routing_mode": "pedelec"},
                         "message": "pedelec_speed_limit_message"},
                    ],
                    "min": 1,
                    "message": "walking_speed_limit_message",
                },
            },
        ),
    )

    pt_max_transfers: int = Field(
        default=5,
        description="Maximum number of transit transfers.",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=13,
            label_key="max_transfers",
            visible_when={
                "$and": [
                    {"show_advanced": True},
                    {"routing_mode": "pt"},
                ]
            },
            widget_options={
                "max_value_from": {
                    "fields": [],
                    "message": "max_transfers_limit_message",
                    "max": 5,
                    "min": 0,
                },
            },
        ),
    )

    pt_access_mode: AccessEgressMode = Field(
        default=AccessEgressMode.walk,
        description="Mode to reach transit stops.",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=20,
            label_key="access_mode",
            group_label="groups.access_leg",
            enum_labels=ACCESS_EGRESS_MODE_LABELS,
            visible_when={
                "$and": [
                    {"show_advanced": True},
                    {"routing_mode": "pt"},
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
                    {"show_advanced": True},
                    {"routing_mode": "pt"},
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
                    {"show_advanced": True},
                    {"routing_mode": "pt"},
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
                    {"show_advanced": True},
                    {"routing_mode": "pt"},
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

    pt_access_speed: float | None = Field(
        default=None,
        description="Access leg speed in km/h. None for car access.",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=23,
            label_key="speed_kmh",
            widget_options={
                "default_by_field": {
                    "field": "pt_access_mode",
                    "values": {"walk": 5, "bicycle": 15, "pedelec": 23},
                },
                "max_value_from": {
                    "fields": [
                        {"value": 30, "when": {"pt_access_mode": "walk"},
                         "message": "walking_speed_limit_message"},
                        {"value": 60, "when": {"pt_access_mode": "bicycle"},
                         "message": "bicycle_speed_limit_message"},
                        {"value": 60, "when": {"pt_access_mode": "pedelec"},
                         "message": "pedelec_speed_limit_message"},
                    ],
                    "min": 1,
                    "message": "walking_speed_limit_message",
                },
            },
            visible_when={
                "$and": [
                    {"show_advanced": True},
                    {"routing_mode": "pt"},
                    {"pt_access_cost_type": "time"},
                    {"pt_access_mode": {"$in": ["walk", "bicycle", "pedelec"]}},
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
            group_label="groups.egress_leg",
            enum_labels=ACCESS_EGRESS_MODE_LABELS,
            visible_when={
                "$and": [
                    {"show_advanced": True},
                    {"routing_mode": "pt"},
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
                    {"show_advanced": True},
                    {"routing_mode": "pt"},
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
                    {"show_advanced": True},
                    {"routing_mode": "pt"},
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
                    {"show_advanced": True},
                    {"routing_mode": "pt"},
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

    pt_egress_speed: float | None = Field(
        default=None,
        description="Egress leg speed in km/h. None for car egress.",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=27,
            label_key="speed_kmh",
            widget_options={
                "default_by_field": {
                    "field": "pt_egress_mode",
                    "values": {"walk": 5, "bicycle": 15, "pedelec": 23},
                },
                "max_value_from": {
                    "fields": [
                        {"value": 30, "when": {"pt_egress_mode": "walk"},
                         "message": "walking_speed_limit_message"},
                        {"value": 60, "when": {"pt_egress_mode": "bicycle"},
                         "message": "bicycle_speed_limit_message"},
                        {"value": 60, "when": {"pt_egress_mode": "pedelec"},
                         "message": "pedelec_speed_limit_message"},
                    ],
                    "min": 1,
                    "message": "walking_speed_limit_message",
                },
            },
            visible_when={
                "$and": [
                    {"show_advanced": True},
                    {"routing_mode": "pt"},
                    {"pt_egress_cost_type": "time"},
                    {"pt_egress_mode": {"$in": ["walk", "bicycle", "pedelec"]}},
                ]
            },
        ),
    )

    def resolve_max_cost(self: Self) -> float | None:
        """Resolve the user-set Dijkstra cutoff for this matrix run.

        Returns the value the user picked under advanced (or the always-visible
        max_cost_time_pt for PT). Returns None when no limit was set — TCM
        runs unbounded; every reachable O-D pair gets a cost. The per-mode
        bbox-extent sanity check at process() prevents nonsense inputs from
        triggering huge searches.
        """
        if self.cost_type == CostType.distance:
            value = (self.max_cost_distance_car
                     if self.routing_mode == RoutingMode.car
                     else self.max_cost_distance)
        elif self.routing_mode == RoutingMode.pt:
            value = self.max_cost_time_pt
        elif self.routing_mode == RoutingMode.car:
            value = self.max_cost_time_car
        else:
            value = self.max_cost_time_active
        return float(value) if value is not None else None

# =========================================================================
# Tool Runner
# =========================================================================


class TravelCostMatrixToolRunner(BaseToolRunner[TravelCostMatrixWindmillParams]):
    """Travel Cost Matrix tool runner for Windmill.

    Creates two output layers:
    1. Matrix table (non-geom): origin_id, destination_id, cost
    2. Destination points (geom): destination geometry annotated with min_cost
    """

    tool_class = TravelCostMatrixTool
    output_geometry_type = "Point"
    default_output_name = get_default_layer_name("travel_cost_matrix", "en")
    default_destinations_name = get_default_layer_name("travel_cost_matrix_destinations", "en")

    @classmethod
    def predict_output_schema(
        cls,
        input_schemas: dict[str, dict[str, str]],
        params: dict[str, Any],
    ) -> dict[str, str]:
        return {
            "origin": "VARCHAR",
            "destination": "VARCHAR",
            "travel_cost": "INTEGER",
        }

    @staticmethod
    def _extract_coordinates_from_parquet(
        parquet_path: Path,
        id_column: str,
    ) -> tuple[list[float], list[float], list[str]]:
        """Extract lat/lon coordinates and IDs from a GeoParquet point layer."""
        con = duckdb.connect()
        try:
            con.execute("INSTALL spatial; LOAD spatial;")

            # Detect geometry column from parquet schema
            cols = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{parquet_path}')").fetchall()
            column_names = {c[0] for c in cols}
            geom_col = next(
                (c[0] for c in cols if "GEOMETRY" in c[1].upper() or c[0] in ("geom", "geometry")),
                None,
            )
            if not geom_col:
                raise RuntimeError(f"No geometry column found in {parquet_path}")

            # Validate id_column against the parquet schema so the f-string SQL
            # below can't be coerced into running attacker-controlled DuckDB.
            if id_column not in column_names:
                raise ValueError(
                    f"Column '{id_column}' does not exist in the layer. "
                    f"Available columns: {sorted(column_names)}"
                )

            # NULLs in the chosen id column are kept as empty strings so the
            # matrix still computes for those rows (Pydantic list[str] would
            # otherwise reject None and the f-string SQL would emit 'None').
            id_select = f'COALESCE(CAST("{id_column}" AS VARCHAR), \'\')'

            result = con.execute(f"""
                SELECT
                    ST_Y("{geom_col}") as lat,
                    ST_X("{geom_col}") as lon,
                    {id_select} as id
                FROM read_parquet('{parquet_path}')
                WHERE "{geom_col}" IS NOT NULL
            """).fetchall()

            latitudes = [r[0] for r in result]
            longitudes = [r[1] for r in result]
            ids = [r[2] for r in result]
            return latitudes, longitudes, ids
        finally:
            con.close()

    @staticmethod
    def _compute_flight_distance_matrix(
        origin_lats: list[float], origin_lons: list[float], origin_ids: list[str],
        dest_lats: list[float], dest_lons: list[float], dest_ids: list[str],
        output_path: Path,
    ) -> None:
        """Compute geodesic distances (WGS84 ellipsoid) between all O-D pairs."""
        con = duckdb.connect()
        try:
            con.execute("INSTALL spatial; LOAD spatial;")

            # Build origin/destination value lists. SQL string literals double
            # any embedded single quote, so labels like "O'Brien" don't break
            # the VALUES clause.
            def _quote(s: str) -> str:
                return "'" + s.replace("'", "''") + "'"

            o_values = ",".join(
                f"({_quote(o_id)}, {lat}, {lon})"
                for o_id, lat, lon in zip(origin_ids, origin_lats, origin_lons)
            )
            d_values = ",".join(
                f"({_quote(d_id)}, {lat}, {lon})"
                for d_id, lat, lon in zip(dest_ids, dest_lats, dest_lons)
            )

            con.execute(f"""
                COPY (
                    SELECT
                        o.id AS origin,
                        d.id AS destination,
                        CAST(ROUND(ST_Distance_Spheroid(
                            ST_Point(o.lat, o.lon),
                            ST_Point(d.lat, d.lon)
                        )) AS INTEGER) AS travel_cost
                    FROM (VALUES {o_values}) AS o(id, lat, lon)
                    CROSS JOIN (VALUES {d_values}) AS d(id, lat, lon)
                ) TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """)
        finally:
            con.close()

    def _build_destination_points_parquet(
        self: Self,
        matrix_path: Path,
        dest_layer_parquet: Path,
        output_path: Path,
    ) -> None:
        """Join min travel cost onto the original destination layer."""
        con = duckdb.connect()
        try:
            con.execute("INSTALL spatial; LOAD spatial;")

            # Join by positional index — each destination row gets the average
            # cost across all origins for that specific point, regardless of
            # whether label values are unique.
            n_dests = con.execute(
                f"SELECT count(*) FROM read_parquet('{dest_layer_parquet}')"
            ).fetchone()[0]
            con.execute(f"""
                COPY (
                    SELECT
                        d.* EXCLUDE (_row_idx),
                        CAST(ROUND(m.avg_cost) AS INTEGER) as travel_cost
                    FROM (
                        SELECT *, (ROW_NUMBER() OVER () - 1) AS _row_idx
                        FROM read_parquet('{dest_layer_parquet}')
                    ) d
                    LEFT JOIN (
                        SELECT
                            dest_idx,
                            AVG(travel_cost) as avg_cost
                        FROM (
                            SELECT travel_cost,
                                   (ROW_NUMBER() OVER () - 1) % {n_dests} AS dest_idx
                            FROM read_parquet('{matrix_path}')
                        )
                        WHERE travel_cost IS NOT NULL
                        GROUP BY dest_idx
                    ) m ON d._row_idx = m.dest_idx
                    ORDER BY d._row_idx
                ) TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """)
        finally:
            con.close()

    def process(
        self: Self,
        params: TravelCostMatrixWindmillParams,
        temp_dir: Path,
    ) -> tuple[Path, DatasetMetadata]:
        """Run travel cost matrix analysis."""
        matrix_output_path = temp_dir / "matrix.parquet"
        destinations_output_path = temp_dir / "destinations.parquet"

        # Export layers to parquet
        origin_parquet = self.export_layer_to_parquet(
            layer_id=params.origin_layer_id,
            user_id=params.user_id,
            cql_filter=params.origin_layer_filter,
            scenario_id=params.scenario_id,
            project_id=params.project_id,
        )
        dest_parquet = self.export_layer_to_parquet(
            layer_id=params.destination_layer_id,
            user_id=params.user_id,
            cql_filter=params.destination_layer_filter,
            scenario_id=params.scenario_id,
            project_id=params.project_id,
        )

        # Extract coordinates and IDs from exported parquets
        origin_lats, origin_lons, origin_ids = self._extract_coordinates_from_parquet(
            origin_parquet, id_column=params.origin_id_column)
        dest_lats, dest_lons, dest_ids = self._extract_coordinates_from_parquet(
            dest_parquet, id_column=params.destination_id_column)

        # Reject empty inputs before they trigger downstream crashes
        # (`min()` on empty list, `(VALUES )` in flight-distance SQL, etc.)
        if not origin_lats:
            raise ValueError(
                "Origin layer has no valid point geometries. "
                "Check that the layer contains points and that geometries are not all null."
            )
        if not dest_lats:
            raise ValueError(
                "Destination layer has no valid point geometries. "
                "Check that the layer contains points and that geometries are not all null."
            )

        # Validate matrix size
        n_combos = len(origin_lats) * len(dest_lats)
        if n_combos > 10_000:
            raise ValueError(
                f"Matrix size ({len(origin_lats)} origins × {len(dest_lats)} destinations "
                f"= {n_combos:,} pairs) exceeds the maximum of 10,000. "
                f"Reduce the number of origins or destinations."
            )

        # Compute max possible O-D distance using bbox corners.
        # The farthest O-D pair is bounded by the distance between the
        # farthest corners of the origin and destination bounding boxes.
        R = 6371000.0
        o_min_lat, o_max_lat = min(origin_lats), max(origin_lats)
        o_min_lon, o_max_lon = min(origin_lons), max(origin_lons)
        d_min_lat, d_max_lat = min(dest_lats), max(dest_lats)
        d_min_lon, d_max_lon = min(dest_lons), max(dest_lons)

        # Max lat/lon span between the two bbox extremes
        lat_span = max(abs(o_max_lat - d_min_lat), abs(d_max_lat - o_min_lat))
        lon_span = max(abs(o_max_lon - d_min_lon), abs(d_max_lon - o_min_lon))
        avg_lat = math.radians(
            (o_min_lat + o_max_lat + d_min_lat + d_max_lat) / 4.0)
        dy = math.radians(lat_span) * R
        dx = math.radians(lon_span) * R * math.cos(avg_lat)
        extent_m = math.sqrt(dx * dx + dy * dy)

        # Validate max extent for routed modes.
        if params.routing_mode != RoutingMode.flight_distance:
            max_reach_m = {
                RoutingMode.walking: 100_000,
                RoutingMode.bicycle: 100_000,
                RoutingMode.pedelec: 100_000,
                RoutingMode.car: 300_000,
                RoutingMode.pt: 300_000,
            }.get(params.routing_mode, 300_000)

            if extent_m > max_reach_m:
                raise ValueError(
                    f"Origin-destination extent ({extent_m / 1000:.0f} km) exceeds "
                    f"the maximum reachable distance for {params.routing_mode.value} "
                    f"({max_reach_m / 1000:.0f} km). "
                    f"Reduce the area or choose a different mode."
                )

        if params.routing_mode == RoutingMode.flight_distance:
            # Geodesic distance — no routing needed.
            self._compute_flight_distance_matrix(
                origin_lats, origin_lons, origin_ids,
                dest_lats, dest_lons, dest_ids,
                matrix_output_path,
            )
        else:
            max_cost = params.resolve_max_cost()

            # Build PT time window if applicable
            time_window = None
            if params.routing_mode == RoutingMode.pt:
                time_window = PTTimeWindow(
                    weekday=params.pt_day,
                    from_time=params.pt_start_time,
                    to_time=params.pt_end_time,
                )

            analysis_params = TravelCostMatrixParams(
                origin_latitude=origin_lats,
                origin_longitude=origin_lons,
                origin_id=origin_ids,
                destination_latitude=dest_lats,
                destination_longitude=dest_lons,
                destination_id=dest_ids,
                routing_mode=params.routing_mode,
                cost_type=params.cost_type,
                max_cost=max_cost,
                speed=params.speed,
                # PT
                transit_modes=params.pt_modes,
                time_window=time_window,
                max_transfers=params.pt_max_transfers,
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
                output_path=str(matrix_output_path),
            )

            tool = self.tool_class()
            try:
                tool.run(analysis_params)
            finally:
                tool.cleanup()

        # Build destination points with min cost joined from original layer
        self._build_destination_points_parquet(
            matrix_output_path, dest_parquet,
            destinations_output_path
        )

        # Store for dual-output handling in run()
        self._matrix_path = matrix_output_path
        self._destinations_path = destinations_output_path

        # Return matrix as primary output
        matrix_metadata = DatasetMetadata(
            path=str(matrix_output_path),
            source_type="tabular",
            format="parquet",
        )
        return matrix_output_path, matrix_metadata

    def run(self: Self, params: TravelCostMatrixWindmillParams) -> dict[str, Any]:
        """Run analysis and create both matrix table and destination points layers."""
        temp_mode = getattr(params, "temp_mode", False)

        output_layer_id_matrix = str(uuid_module.uuid4())
        output_layer_id_dests = str(uuid_module.uuid4())

        output_name_matrix = (
            params.matrix_layer_name
            or params.result_layer_name
            or params.output_name
            or self.default_output_name
        )
        output_name_dests = (
            params.destinations_layer_name or self.default_destinations_name
        )

        logger.info(
            f"Starting tool: {self.__class__.__name__} "
            f"(user={params.user_id}, matrix={output_layer_id_matrix}, "
            f"dests={output_layer_id_dests}, temp_mode={temp_mode})"
        )

        asyncio.get_event_loop().run_until_complete(self._init_db_service())

        with tempfile.TemporaryDirectory(
            prefix=f"{self.__class__.__name__.lower()}_"
        ) as temp_dir:
            temp_path = Path(temp_dir)

            # Step 1: Run analysis
            matrix_parquet, matrix_metadata = self.process(params, temp_path)
            dests_parquet = self._destinations_path

            # Temp mode: return matrix only
            if temp_mode:
                result = self._write_temp_result(
                    params=params,
                    output_parquet=matrix_parquet,
                    output_name=output_name_matrix,
                    output_layer_id=output_layer_id_matrix,
                )
                asyncio.get_event_loop().run_until_complete(self._close_db_service())
                return result

            # Step 2: Ingest matrix table to DuckLake
            table_info_matrix = self._ingest_to_ducklake(
                user_id=params.user_id,
                layer_id=output_layer_id_matrix,
                parquet_path=matrix_parquet,
            )
            logger.info(f"Matrix table: {table_info_matrix['table_name']}")

            # Step 3: Ingest destination points to DuckLake
            table_info_dests = self._ingest_to_ducklake(
                user_id=params.user_id,
                layer_id=output_layer_id_dests,
                parquet_path=dests_parquet,
            )
            logger.info(f"Destinations table: {table_info_dests['table_name']}")

            # Refresh database pool — connections may have gone stale during analysis
            asyncio.get_event_loop().run_until_complete(self._close_db_service())

            # Step 4: Create matrix layer record (table type)
            result_info_matrix = asyncio.get_event_loop().run_until_complete(
                self._create_db_records(
                    output_layer_id=output_layer_id_matrix,
                    params=params,
                    output_name=output_name_matrix,
                    metadata=matrix_metadata,
                    table_info=table_info_matrix,
                )
            )

            # Step 5: Create destination points layer record
            dests_metadata = DatasetMetadata(
                path=str(dests_parquet),
                source_type="vector",
                format="geoparquet",
                geometry_type="Point",
                geometry_column="geometry",
            )
            result_info_dests = asyncio.get_event_loop().run_until_complete(
                self._create_db_records(
                    output_layer_id=output_layer_id_dests,
                    params=params,
                    output_name=output_name_dests,
                    metadata=dests_metadata,
                    table_info=table_info_dests,
                )
            )

        asyncio.get_event_loop().run_until_complete(self._close_db_service())

        # Build wm_labels
        wm_labels: list[str] = []
        if params.triggered_by_email:
            wm_labels.append(params.triggered_by_email)

        # Primary output: matrix table
        output_matrix = ToolOutputBase(
            layer_id=output_layer_id_matrix,
            name=output_name_matrix,
            folder_id=result_info_matrix["folder_id"],
            user_id=params.user_id,
            project_id=params.project_id,
            layer_project_id=result_info_matrix.get("layer_project_id"),
            type="table",
            feature_layer_type="tool",
            table_name=table_info_matrix["table_name"],
            wm_labels=wm_labels,
        )

        # Secondary output: destination points
        output_dests = ToolOutputBase(
            layer_id=output_layer_id_dests,
            name=output_name_dests,
            folder_id=result_info_dests["folder_id"],
            user_id=params.user_id,
            project_id=params.project_id,
            layer_project_id=result_info_dests.get("layer_project_id"),
            type="feature",
            feature_layer_type="tool",
            geometry_type=table_info_dests.get("geometry_type"),
            feature_count=table_info_dests.get("feature_count", 0),
            extent=table_info_dests.get("extent"),
            table_name=table_info_dests["table_name"],
            wm_labels=wm_labels,
        )

        logger.info(
            f"Tool completed: matrix={output_layer_id_matrix}, dests={output_layer_id_dests}"
        )

        result = output_matrix.model_dump()
        result["secondary_layers"] = [output_dests.model_dump()]
        return result


def main(params: TravelCostMatrixWindmillParams) -> dict:
    """Windmill entry point for travel cost matrix tool."""
    runner = TravelCostMatrixToolRunner()
    runner.init_from_env()

    try:
        return runner.run(params)
    finally:
        runner.cleanup()
