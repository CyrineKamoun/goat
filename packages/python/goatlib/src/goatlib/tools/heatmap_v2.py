"""Heatmap V2 tool for Windmill — on-the-fly via local C++ routing.

Supports walking, bicycle, pedelec, and car modes (PT dropped pending a
stop-walk precomputed matrix redesign). Per-opportunity max_cost drives
the routing budget; the resolver turns layer IDs into parquet paths.
"""

from __future__ import annotations

import logging
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal, Self

from pydantic import ConfigDict, Field

from goatlib.analysis.accessibility import HeatmapV2Tool
from goatlib.analysis.schemas.catchment_area_v2 import (
    CostType,
    RoutingMode,
)
from goatlib.analysis.schemas.heatmap import (
    PotentialExpression,
    PotentialType,
    ROUTING_MODE_ICONS,
    ROUTING_MODE_LABELS,
    SensitivityValue,
)
from goatlib.analysis.schemas.heatmap_v2 import (
    GravityDecay,
    HeatmapType,
    HeatmapV2Params,
    OpportunityV2,
)
from goatlib.tools._routing_limits import (
    ACTIVE_DISTANCE_LIMIT_MSG,
    ACTIVE_TIME_LIMIT_MSG,
    CAR_DISTANCE_LIMIT_MSG,
    CAR_TIME_LIMIT_MSG,
    DEFAULT_MAX_DISTANCE_ACTIVE_M,
    DEFAULT_MAX_DISTANCE_CAR_M,
    DEFAULT_MAX_TIME_ACTIVE_MIN,
    DEFAULT_MAX_TIME_CAR_MIN,
    MAX_DISTANCE_ACTIVE_M,
    MAX_DISTANCE_CAR_M,
    MAX_TIME_ACTIVE_MIN,
    MAX_TIME_CAR_MIN,
)
from goatlib.analysis.schemas.ui import (
    UISection,
    ui_field,
    ui_sections,
)
from goatlib.models.io import DatasetMetadata
from goatlib.tools.base import BaseToolRunner
from goatlib.tools.catchment_area_v2 import (
    COST_TYPE_ICONS,
    COST_TYPE_LABELS,
    SECTION_CONFIGURATION,
)
from goatlib.tools.schemas import (
    ToolInputBase,
    get_default_layer_name,
)
from goatlib.tools.style import get_heatmap_style

logger = logging.getLogger(__name__)


class HeatmapRoutingMode(StrEnum):
    """Routing modes supported by heatmap v2. PT is intentionally not
    listed — PT support will return in a future rewrite with the
    stop-walk precomputed matrix approach.
    """

    walking = "walking"
    bicycle = "bicycle"
    pedelec = "pedelec"
    car = "car"


# =========================================================================
# UI Sections
# =========================================================================

SECTION_ROUTING_HM = UISection(
    id="routing",
    order=1,
    icon="route",
    label_key="routing",
)

SECTION_OPPORTUNITIES_HM = UISection(
    id="opportunities",
    order=4,
    icon="opportunity",
    label_key="opportunities",
    depends_on={"routing_mode": {"$ne": None}},
)

SECTION_RESULT_HM = UISection(
    id="result",
    order=7,
    icon="save",
    label_key="result_layer_section",
    depends_on={"routing_mode": {"$ne": None}},
)

# =========================================================================
# Label Mappings
# =========================================================================

HEATMAP_TYPE_LABELS: dict[str, str] = {
    "gravity": "enums.heatmap_type.gravity",
    "closest_average": "enums.heatmap_type.closest_average",
}

GRAVITY_DECAY_LABELS: dict[str, str] = {
    "gaussian": "enums.gravity_decay.gaussian",
    "exponential": "enums.gravity_decay.exponential",
    "linear": "enums.gravity_decay.linear",
    "power": "enums.gravity_decay.power",
}


# =========================================================================
# Form-layer opportunity schema
#
# Adds a Point / MultiPoint geometry filter to the layer selector on top of
# the analysis-layer OpportunityV2. v2's network prep loads edges in buffers
# around opportunity points, so polygons / lines aren't meaningful input. At
# runtime, instances of this subclass remain compatible with the analysis
# layer's OpportunityV2 via inheritance.
# =========================================================================


class OpportunityV2PointBase(OpportunityV2):
    """Shared form-layer base for v2 opportunity cards. Restricts input_path
    to Point/MultiPoint layers, splits the per-opportunity budget into
    time/distance variants (gated by the outer cost_type, with per-mode
    caps), and hides all formula-specific extras from
    OpportunityV2/OpportunityGravity. The gravity / closest-average
    subclasses below re-expose the fields each formula needs."""

    input_path: str = Field(
        ...,
        description="Path to opportunity dataset (point layer).",
        json_schema_extra=ui_field(
            section="opportunities",
            field_order=1,
            label_key="input_path",
            widget="layer-selector",
            widget_options={"geometry_types": ["Point", "MultiPoint"]},
        ),
    )

    # Hide the inherited analysis-layer scalar `max_cost`. The runner sets
    # it via resolve_max_cost(cost_type) before handing the opportunity to
    # the analysis layer.
    max_cost: int = Field(
        default=DEFAULT_MAX_TIME_ACTIVE_MIN,
        gt=0,
        json_schema_extra=ui_field(section="opportunities", hidden=True),
    )

    max_cost_time: int = Field(
        default=DEFAULT_MAX_TIME_ACTIVE_MIN,
        ge=1,
        description="Maximum travel time in minutes.",
        json_schema_extra=ui_field(
            section="opportunities",
            field_order=2,
            label_key="max_cost_time",
            visible_when={
                "$and": [
                    {"input_path": {"$ne": None}},
                    {"cost_type": "time"},
                ]
            },
            widget_options={
                "default_by_field": {
                    "field": "routing_mode",
                    "values": {
                        "walking": DEFAULT_MAX_TIME_ACTIVE_MIN,
                        "bicycle": DEFAULT_MAX_TIME_ACTIVE_MIN,
                        "pedelec": DEFAULT_MAX_TIME_ACTIVE_MIN,
                        "car": DEFAULT_MAX_TIME_CAR_MIN,
                    },
                },
                "max_value_from": {
                    "fields": [
                        {
                            "value": MAX_TIME_ACTIVE_MIN,
                            "when": {"routing_mode": "walking"},
                            "message": ACTIVE_TIME_LIMIT_MSG,
                        },
                        {
                            "value": MAX_TIME_ACTIVE_MIN,
                            "when": {"routing_mode": "bicycle"},
                            "message": ACTIVE_TIME_LIMIT_MSG,
                        },
                        {
                            "value": MAX_TIME_ACTIVE_MIN,
                            "when": {"routing_mode": "pedelec"},
                            "message": ACTIVE_TIME_LIMIT_MSG,
                        },
                        {
                            "value": MAX_TIME_CAR_MIN,
                            "when": {"routing_mode": "car"},
                            "message": CAR_TIME_LIMIT_MSG,
                        },
                    ],
                    "min": 1,
                    "message": ACTIVE_TIME_LIMIT_MSG,
                },
            },
        ),
    )

    max_cost_distance: int = Field(
        default=DEFAULT_MAX_DISTANCE_ACTIVE_M,
        ge=50,
        description="Maximum travel distance in meters.",
        json_schema_extra=ui_field(
            section="opportunities",
            field_order=2,
            label_key="max_cost_distance",
            visible_when={
                "$and": [
                    {"input_path": {"$ne": None}},
                    {"cost_type": "distance"},
                ]
            },
            widget_options={
                "default_by_field": {
                    "field": "routing_mode",
                    "values": {
                        "walking": DEFAULT_MAX_DISTANCE_ACTIVE_M,
                        "bicycle": DEFAULT_MAX_DISTANCE_ACTIVE_M,
                        "pedelec": DEFAULT_MAX_DISTANCE_ACTIVE_M,
                        "car": DEFAULT_MAX_DISTANCE_CAR_M,
                    },
                },
                "max_value_from": {
                    "fields": [
                        {
                            "value": MAX_DISTANCE_ACTIVE_M,
                            "when": {"routing_mode": "walking"},
                            "message": ACTIVE_DISTANCE_LIMIT_MSG,
                        },
                        {
                            "value": MAX_DISTANCE_ACTIVE_M,
                            "when": {"routing_mode": "bicycle"},
                            "message": ACTIVE_DISTANCE_LIMIT_MSG,
                        },
                        {
                            "value": MAX_DISTANCE_ACTIVE_M,
                            "when": {"routing_mode": "pedelec"},
                            "message": ACTIVE_DISTANCE_LIMIT_MSG,
                        },
                        {
                            "value": MAX_DISTANCE_CAR_M,
                            "when": {"routing_mode": "car"},
                            "message": CAR_DISTANCE_LIMIT_MSG,
                        },
                    ],
                    "min": 50,
                    "message": ACTIVE_DISTANCE_LIMIT_MSG,
                },
            },
        ),
    )

    # Hide all formula-specific extras from the base. The two formula
    # subclasses below re-declare the fields they need as visible.
    sensitivity: SensitivityValue = Field(
        default=300000,
        json_schema_extra=ui_field(section="opportunities", hidden=True),
    )
    potential_type: PotentialType = Field(
        default=PotentialType.constant,
        json_schema_extra=ui_field(section="opportunities", hidden=True),
    )
    potential_field: str | None = Field(
        None,
        json_schema_extra=ui_field(section="opportunities", hidden=True),
    )
    potential_constant: float | None = Field(
        1.0,
        gt=0.0,
        json_schema_extra=ui_field(section="opportunities", hidden=True),
    )
    potential_expression: PotentialExpression | None = Field(
        None,
        json_schema_extra=ui_field(section="opportunities", hidden=True),
    )
    n_destinations: Literal[1, 2, 3, 4, 5, 6, 7, 8, 9, 10] = Field(
        default=1,
        title="Number of Destinations",
        json_schema_extra=ui_field(section="opportunities", hidden=True),
    )

    def resolve_max_cost(self: Self, cost_type: CostType) -> int:
        """Return the active per-opp budget for the outer cost_type —
        minutes when time, meters when distance."""
        return (
            self.max_cost_distance if cost_type == CostType.distance
            else self.max_cost_time
        )


class OpportunityV2PointGravity(OpportunityV2PointBase):
    """Gravity opportunity card: re-exposes sensitivity + potential_* fields."""

    sensitivity: SensitivityValue = Field(
        default=300000,
        description="Sensitivity parameter for gravity decay function.",
        json_schema_extra=ui_field(
            section="opportunities",
            field_order=10,
            visible_when={"input_path": {"$ne": None}},
        ),
    )
    potential_type: PotentialType = Field(
        default=PotentialType.constant,
        description="How to determine the potential value for each opportunity.",
        json_schema_extra=ui_field(
            section="opportunities",
            field_order=4,
            visible_when={"input_path": {"$ne": None}},
            widget_options={
                "enum_geometry_filter": {
                    "source_layer": "input_path",
                    "expression": ["Polygon", "MultiPolygon"],
                }
            },
        ),
    )
    potential_field: str | None = Field(
        None,
        description="Field name to use as potential.",
        json_schema_extra=ui_field(
            section="opportunities",
            field_order=5,
            widget="field-selector",
            widget_options={
                "source_layer": "input_path",
                "field_types": ["number"],
            },
            visible_when={
                "input_path": {"$ne": None},
                "potential_type": "field",
            },
        ),
    )
    potential_constant: float | None = Field(
        1.0,
        gt=0.0,
        description="Constant potential value applied to all features.",
        json_schema_extra=ui_field(
            section="opportunities",
            field_order=6,
            widget="number",
            visible_when={
                "input_path": {"$ne": None},
                "potential_type": "constant",
            },
        ),
    )
    potential_expression: PotentialExpression | None = Field(
        None,
        description="Expression to compute potential for polygon layers.",
        json_schema_extra=ui_field(
            section="opportunities",
            field_order=7,
            visible_when={
                "input_path": {"$ne": None},
                "potential_type": "expression",
            },
        ),
    )


class OpportunityV2PointClosestAverage(OpportunityV2PointBase):
    """Closest-Average opportunity card: re-exposes n_destinations."""

    n_destinations: Literal[1, 2, 3, 4, 5, 6, 7, 8, 9, 10] = Field(
        default=1,
        title="Number of Destinations",
        description="Number of closest destinations to average",
        json_schema_extra=ui_field(
            section="opportunities",
            field_order=5,
            visible_when={"input_path": {"$ne": None}},
        ),
    )


# =========================================================================
# Windmill Params
# =========================================================================


class HeatmapV2WindmillParams(ToolInputBase):
    """Windmill-facing params for HeatmapV2.

    Mode/cost/PT fields mirror catchment_area_v2's structure exactly. The
    runner's `process()` resolves the per-mode budget fields into a single
    `max_cost`, builds the analysis-level `HeatmapV2Params`, and hands off
    to `HeatmapV2Tool`.
    """

    model_config = ConfigDict(
        json_schema_extra=ui_sections(
            SECTION_ROUTING_HM,
            SECTION_CONFIGURATION,
            SECTION_OPPORTUNITIES_HM,
            SECTION_RESULT_HM,
        )
    )

    # =========================================================================
    # Result Section
    # =========================================================================

    result_layer_name: str | None = Field(
        default=get_default_layer_name("heatmap_gravity", "en"),
        description="Name for the heatmap result layer.",
        json_schema_extra=ui_field(
            section="result",
            field_order=1,
            label_key="result_layer_name",
        ),
    )

    # =========================================================================
    # Routing Section
    # =========================================================================

    routing_mode: HeatmapRoutingMode = Field(
        ...,
        description="Transport mode for the heatmap.",
        json_schema_extra=ui_field(
            section="routing",
            field_order=1,
            label_key="routing_mode",
            enum_icons=ROUTING_MODE_ICONS,
            enum_labels=ROUTING_MODE_LABELS,
        ),
    )

    # =========================================================================
    # Configuration Section — heatmap formula + measure unit
    # =========================================================================

    heatmap_type: HeatmapType = Field(
        default=HeatmapType.gravity,
        description="Formula used to score each cell from reachable opportunities.",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=0,
            label_key="heatmap_type",
            enum_labels=HEATMAP_TYPE_LABELS,
        ),
    )

    # Cost type (time vs distance) — mirrors catchment_area_v2. Drives which
    # set of per-opportunity budget fields apply.
    cost_type: CostType = Field(
        default=CostType.time,
        description="Measure the heatmap by travel time or travel distance.",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=2,
            label_key="measure_type",
            enum_labels=COST_TYPE_LABELS,
            enum_icons=COST_TYPE_ICONS,
            inline_group="cost_config",
        ),
    )

    decay: GravityDecay = Field(
        default=GravityDecay.gaussian,
        description="Shape of the distance-decay curve applied to travel cost.",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=1,
            label_key="impedance",
            enum_labels=GRAVITY_DECAY_LABELS,
            visible_when={"heatmap_type": "gravity"},
        ),
    )

    max_sensitivity: float = Field(
        default=1_000_000.0,
        gt=0.0,
        description="Gravity sensitivity normalization anchor.",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=14,
            hidden=True,  # Internal normalization constant
        ),
    )

    # Mirrors catchment_area_v2.show_advanced: gates the Speed override
    # (and any other advanced-only field) behind a single toggle.
    show_advanced: bool = Field(
        default=False,
        description="Show advanced configuration options.",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=15,
            label_key="advanced_options",
            widget="advanced-toggle",
        ),
    )

    h3_resolution: int | None = Field(
        default=None,
        ge=5,
        le=12,
        description="H3 resolution for origin tiling. None → per-mode default.",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=16,
            label_key="h3_resolution",
            hidden=True,  # Advanced; expose later if needed
        ),
    )

    # Optional reference-area clip — when set, output cells are restricted
    # to those inside the polygon (cells outside dropped; cells inside that
    # weren't reached become NULL). Gated behind show_advanced so it shares
    # the same toggle as speed; connectivity overrides this to required +
    # always-visible.
    reference_area_layer_id: str | None = Field(
        None,
        description="Layer ID for the reference area polygon.",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=21,
            label_key="reference_area_path",
            widget="layer-selector",
            widget_options={"geometry_types": ["Polygon", "MultiPolygon"]},
            visible_when={"show_advanced": True},
        ),
    )
    reference_area_layer_filter: dict[str, Any] | None = Field(
        None,
        description="CQL2-JSON filter to apply to the reference area layer.",
        json_schema_extra=ui_field(
            section="configuration", field_order=22, hidden=True
        ),
    )

    # =========================================================================
    # Cost / budget / speed
    #
    # v2 mirrors the matrix-based gravity tool: per-opportunity `max_cost`
    # lives on each OpportunityGravity entry under the Opportunities section
    # (in minutes). Mode-default speeds drive routing — surfaced as advanced
    # if the user needs to override.
    # =========================================================================

    speed: float | None = Field(
        default=None,
        description=(
            "Travel speed in km/h. Leave blank to use the mode default "
            "(walking 5, bicycle 15, pedelec 23). Car uses per-road speed "
            "limits and ignores this setting."
        ),
        json_schema_extra=ui_field(
            section="configuration",
            field_order=20,
            label_key="speed",
            visible_when={
                "$and": [
                    {"routing_mode": {"$in": ["walking", "bicycle", "pedelec"]}},
                    {"cost_type": "time"},
                    {"show_advanced": True},
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

    # =========================================================================
    # Opportunity layers (Gravity / ClosestAverage — workflow-canvas
    # connectors, so hidden in the form). Up to 3 layers.
    # =========================================================================

    opportunity_layer_1_id: str | None = Field(
        None,
        description="First opportunity layer.",
        json_schema_extra=ui_field(
            section="opportunities",
            field_order=1,
            widget="layer-selector",
            label_key="opportunity_layer_1",
            hidden=True,
            visible_when={
                "heatmap_type": {"$in": ["gravity", "closest_average"]}
            },
        ),
    )
    opportunity_layer_1_filter: dict[str, Any] | None = Field(
        None,
        json_schema_extra=ui_field(section="opportunities", field_order=2, hidden=True),
    )
    opportunity_layer_2_id: str | None = Field(
        None,
        description="Second opportunity layer.",
        json_schema_extra=ui_field(
            section="opportunities",
            field_order=3,
            widget="layer-selector",
            label_key="opportunity_layer_2",
            hidden=True,
            visible_when={
                "heatmap_type": {"$in": ["gravity", "closest_average"]}
            },
        ),
    )
    opportunity_layer_2_filter: dict[str, Any] | None = Field(
        None,
        json_schema_extra=ui_field(section="opportunities", field_order=4, hidden=True),
    )
    opportunity_layer_3_id: str | None = Field(
        None,
        description="Third opportunity layer.",
        json_schema_extra=ui_field(
            section="opportunities",
            field_order=5,
            widget="layer-selector",
            label_key="opportunity_layer_3",
            hidden=True,
            visible_when={
                "heatmap_type": {"$in": ["gravity", "closest_average"]}
            },
        ),
    )
    opportunity_layer_3_filter: dict[str, Any] | None = Field(
        None,
        json_schema_extra=ui_field(section="opportunities", field_order=6, hidden=True),
    )

    # =========================================================================
    # Opportunities list (toolbox UI: repeatable layer entries with per-layer
    # weight + sensitivity controls). Workflow-canvas numbered layer-IDs above
    # take precedence when present (the runner re-packs them into this list).
    # =========================================================================

    opportunities: list[OpportunityV2] | None = Field(
        default=None,
        description="Opportunity layers to score against.",
        json_schema_extra=ui_field(
            section="opportunities",
            field_order=10,
            repeatable=True,
            min_items=1,
            visible_when={
                "heatmap_type": {"$in": ["gravity", "closest_average"]}
            },
        ),
    )

    # =========================================================================
    # Helpers
    # =========================================================================

    def aggregate_max_cost(self: Self) -> float:
        """Maximum per-opportunity budget across all layers — used so the
        loaded network buffer covers every layer's reach. Unit follows
        cost_type: minutes for time, meters for distance."""
        if not self.opportunities:
            return 30.0
        budgets = [
            o.resolve_max_cost(self.cost_type)
            if isinstance(o, OpportunityV2PointBase)
            else o.max_cost
            for o in self.opportunities
        ]
        return float(max(budgets))

    def resolved_opportunities(self: Self) -> list[OpportunityV2]:
        """Project form opportunities to analysis-layer OpportunityV2,
        collapsing max_cost_time / max_cost_distance into max_cost based on
        the form's cost_type."""
        if not self.opportunities:
            return []
        out: list[OpportunityV2] = []
        for o in self.opportunities:
            if isinstance(o, OpportunityV2PointBase):
                data = o.model_dump(
                    exclude={"max_cost_time", "max_cost_distance"}
                )
                data["max_cost"] = o.resolve_max_cost(self.cost_type)
                out.append(OpportunityV2(**data))
            else:
                out.append(o)
        return out


# =========================================================================
# Per-formula entry points
#
# The toolbox surfaces one tool per heatmap formula (Gravity / ClosestAverage).
# Each pre-binds heatmap_type and hides the selector so the formula-specific
# tile renders a focused form.
# =========================================================================


class HeatmapGravityV2WindmillParams(HeatmapV2WindmillParams):
    """Gravity-based spatial accessibility analysis."""

    heatmap_type: HeatmapType = Field(
        default=HeatmapType.gravity,
        json_schema_extra=ui_field(section="configuration", hidden=True),
    )
    # Mirrors v1 HeatmapGravityParams: opportunities required, 1-3 layers.
    opportunities: list[OpportunityV2PointGravity] = Field(
        ...,
        title="Opportunity Layers",
        min_length=1,
        max_length=3,
        description="Opportunity layers to score against.",
        json_schema_extra=ui_field(
            section="opportunities",
            field_order=10,
            repeatable=True,
            min_items=1,
            max_items=3,
        ),
    )
    result_layer_name: str | None = Field(
        default=get_default_layer_name("heatmap_gravity", "en"),
        json_schema_extra=ui_field(
            section="result",
            field_order=1,
            label_key="result_layer_name",
        ),
    )


class HeatmapClosestAverageV2WindmillParams(HeatmapV2WindmillParams):
    """Average distance/time to N closest destinations."""

    heatmap_type: HeatmapType = Field(
        default=HeatmapType.closest_average,
        json_schema_extra=ui_field(section="configuration", hidden=True),
    )
    decay: GravityDecay = Field(
        default=GravityDecay.gaussian,
        json_schema_extra=ui_field(section="configuration", hidden=True),
    )
    # Mirrors v1 HeatmapClosestAverageParams: opportunities required, 1-3 layers.
    opportunities: list[OpportunityV2PointClosestAverage] = Field(
        ...,
        title="Opportunity Layers",
        min_length=1,
        max_length=3,
        description="Opportunity layers to score against.",
        json_schema_extra=ui_field(
            section="opportunities",
            field_order=10,
            repeatable=True,
            min_items=1,
            max_items=3,
        ),
    )
    result_layer_name: str | None = Field(
        default=get_default_layer_name("heatmap_closest_average", "en"),
        json_schema_extra=ui_field(
            section="result",
            field_order=1,
            label_key="result_layer_name",
        ),
    )


class HeatmapConnectivityV2WindmillParams(HeatmapV2WindmillParams):
    """Total area reachable within max travel cost."""

    # Hide the heatmap_type selector and pre-bind to connectivity
    heatmap_type: HeatmapType = Field(
        default=HeatmapType.connectivity,
        description="Heatmap formula (pre-bound to connectivity).",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=2,
            hidden=True,
        ),
    )

    # Opportunity layers are not used for connectivity — override as optional
    # with empty default so the base-class validator doesn't demand them.
    opportunities: list[OpportunityV2] | None = Field(
        default=None,
        description="Not used for connectivity (pre-bound formula).",
        json_schema_extra=ui_field(
            section="opportunities",
            field_order=10,
            repeatable=True,
            min_items=1,
            hidden=True,
            visible_when={
                "heatmap_type": {"$in": ["gravity", "closest_average"]}
            },
        ),
    )

    # Travel budget — two fields gated by cost_type. Per-mode caps come from
    # widget_options.max_value_from (active vs. car). Same pattern as the
    # per-opportunity max_cost on OpportunityV2Point.
    max_cost_time: int = Field(
        default=DEFAULT_MAX_TIME_ACTIVE_MIN,
        ge=1,
        description="Maximum travel time in minutes.",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=3,
            label_key="max_cost_time",
            visible_when={"cost_type": "time"},
            widget_options={
                "default_by_field": {
                    "field": "routing_mode",
                    "values": {
                        "walking": DEFAULT_MAX_TIME_ACTIVE_MIN,
                        "bicycle": DEFAULT_MAX_TIME_ACTIVE_MIN,
                        "pedelec": DEFAULT_MAX_TIME_ACTIVE_MIN,
                        "car": DEFAULT_MAX_TIME_CAR_MIN,
                    },
                },
                "max_value_from": {
                    "fields": [
                        {
                            "value": MAX_TIME_ACTIVE_MIN,
                            "when": {"routing_mode": "walking"},
                            "message": ACTIVE_TIME_LIMIT_MSG,
                        },
                        {
                            "value": MAX_TIME_ACTIVE_MIN,
                            "when": {"routing_mode": "bicycle"},
                            "message": ACTIVE_TIME_LIMIT_MSG,
                        },
                        {
                            "value": MAX_TIME_ACTIVE_MIN,
                            "when": {"routing_mode": "pedelec"},
                            "message": ACTIVE_TIME_LIMIT_MSG,
                        },
                        {
                            "value": MAX_TIME_CAR_MIN,
                            "when": {"routing_mode": "car"},
                            "message": CAR_TIME_LIMIT_MSG,
                        },
                    ],
                    "min": 1,
                    "message": ACTIVE_TIME_LIMIT_MSG,
                },
            },
        ),
    )

    max_cost_distance: int = Field(
        default=DEFAULT_MAX_DISTANCE_ACTIVE_M,
        ge=50,
        description="Maximum travel distance in meters.",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=3,
            label_key="max_cost_distance",
            visible_when={"cost_type": "distance"},
            widget_options={
                "default_by_field": {
                    "field": "routing_mode",
                    "values": {
                        "walking": DEFAULT_MAX_DISTANCE_ACTIVE_M,
                        "bicycle": DEFAULT_MAX_DISTANCE_ACTIVE_M,
                        "pedelec": DEFAULT_MAX_DISTANCE_ACTIVE_M,
                        "car": DEFAULT_MAX_DISTANCE_CAR_M,
                    },
                },
                "max_value_from": {
                    "fields": [
                        {
                            "value": MAX_DISTANCE_ACTIVE_M,
                            "when": {"routing_mode": "walking"},
                            "message": ACTIVE_DISTANCE_LIMIT_MSG,
                        },
                        {
                            "value": MAX_DISTANCE_ACTIVE_M,
                            "when": {"routing_mode": "bicycle"},
                            "message": ACTIVE_DISTANCE_LIMIT_MSG,
                        },
                        {
                            "value": MAX_DISTANCE_ACTIVE_M,
                            "when": {"routing_mode": "pedelec"},
                            "message": ACTIVE_DISTANCE_LIMIT_MSG,
                        },
                        {
                            "value": MAX_DISTANCE_CAR_M,
                            "when": {"routing_mode": "car"},
                            "message": CAR_DISTANCE_LIMIT_MSG,
                        },
                    ],
                    "min": 50,
                    "message": ACTIVE_DISTANCE_LIMIT_MSG,
                },
            },
        ),
    )

    # Connectivity requires the reference area; promote the inherited
    # optional+advanced field to required+non-advanced.
    reference_area_layer_id: str = Field(
        ...,
        description="Layer ID for the reference area polygon.",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=4,
            label_key="reference_area_path",
            widget="layer-selector",
            widget_options={"geometry_types": ["Polygon", "MultiPolygon"]},
        ),
    )

    result_layer_name: str | None = Field(
        default=get_default_layer_name("heatmap_connectivity", "en"),
        json_schema_extra=ui_field(
            section="result",
            field_order=1,
            label_key="result_layer_name",
        ),
    )

    def aggregate_max_cost(self: Self) -> float:
        """Connectivity budget — minutes (time) or meters (distance)."""
        return float(
            self.max_cost_distance if self.cost_type == CostType.distance
            else self.max_cost_time
        )


# =========================================================================
# Runner
# =========================================================================


# HeatmapRoutingMode (UI layer) → analysis-layer RoutingMode.
# Both enums have identical values for the supported modes; the map exists
# only to make the type conversion explicit.
_ROUTING_MODE_MAP: dict[HeatmapRoutingMode, RoutingMode] = {
    HeatmapRoutingMode.walking: RoutingMode.walking,
    HeatmapRoutingMode.bicycle: RoutingMode.bicycle,
    HeatmapRoutingMode.pedelec: RoutingMode.pedelec,
    HeatmapRoutingMode.car: RoutingMode.car,
}


class HeatmapV2ToolRunner(BaseToolRunner[HeatmapV2WindmillParams]):
    """Heatmap V2 tool runner for Windmill — local C++ routing backend."""

    tool_class = HeatmapV2Tool
    output_geometry_type = "polygon"  # H3 cells
    default_output_name = get_default_layer_name("heatmap_gravity", "en")

    @classmethod
    def predict_output_schema(
        cls,
        input_schemas: dict[str, dict[str, str]],
        params: dict[str, Any],
    ) -> dict[str, str]:
        return {
            "h3_index": "VARCHAR",
            "total_accessibility": "DOUBLE",
            "geometry": "GEOMETRY",
        }

    def get_layer_properties(
        self: Self,
        params: HeatmapV2WindmillParams,
        metadata: DatasetMetadata,
        table_info: dict[str, Any] | None = None,
        parquet_path: Path | str | None = None,
    ) -> dict[str, Any] | None:
        color_field = "total_accessibility"
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
        return get_heatmap_style(
            color_field_name=color_field,
            color_scale_breaks=color_scale_breaks,
            color_range_name="Emrld",
        )

    # --------------------------------------------------------- helpers

    def _resolve_opportunities(
        self: Self, params: HeatmapV2WindmillParams
    ) -> list[OpportunityV2]:
        """Resolve opportunity layer IDs → parquet paths.

        Two entry shapes:
          (1) Toolbox form: `params.opportunities` holds OpportunityV2
              instances where `input_path` is a layer UUID and the
              per-layer fields (potential_type / potential_field /
              potential_constant / potential_expression / sensitivity /
              input_layer_filter / name) are already populated by the UI.
              `resolve_layer_paths` swaps the UUID for an exported parquet
              path, applies the CQL filter, and preserves every other
              field on the model.
          (2) Workflow canvas: the canvas connector only supplies layer
              IDs via the numbered `opportunity_layer_{1,2,3}_id` fields;
              per-layer config is unavailable, so we construct stub
              OpportunityV2 entries with just `input_path`.
        Workflow-canvas IDs take precedence when present.
        """
        numbered_ids = [
            params.opportunity_layer_1_id,
            params.opportunity_layer_2_id,
            params.opportunity_layer_3_id,
        ]
        if any(numbered_ids):
            # Workflow-canvas path: bare layer IDs, no per-layer config —
            # take the form's resolved budget as the default for every
            # canvas opportunity.
            fallback_max_cost = int(params.aggregate_max_cost())
            opps: list[OpportunityV2] = []
            for layer_id in numbered_ids:
                if not layer_id:
                    continue
                parquet_path = str(
                    self.export_layer_to_parquet(
                        layer_id=layer_id,
                        user_id=params.user_id,
                        project_id=params.project_id,
                    )
                )
                opps.append(
                    OpportunityV2(
                        input_path=parquet_path,
                        max_cost=fallback_max_cost,
                    )
                )
            return opps

        if not params.opportunities:
            raise ValueError(
                "At least one opportunity layer is required "
                "(via opportunity_layer_{1,2,3}_id or the opportunities list)."
            )
        # Toolbox-form path: collapse OpportunityV2PointBase's per-mode budget
        # fields into the analysis-layer OpportunityV2.max_cost (resolved
        # against the form's outer routing_mode + cost_type), then swap
        # layer UUIDs for parquet paths.
        resolved = params.resolved_opportunities()
        return self.resolve_layer_paths(
            resolved, params.user_id, "input_path"
        )

    def _resolve_reference_area(
        self: Self, params: HeatmapV2WindmillParams
    ) -> str | None:
        """Export the reference area layer (if any) to a parquet path.
        Returns None when the user didn't supply one (gravity/closest_avg
        advanced field left blank). Connectivity's subclass marks the
        field as required, so this returns a path for that tool."""
        if not getattr(params, "reference_area_layer_id", None):
            return None
        return str(
            self.export_layer_to_parquet(
                layer_id=params.reference_area_layer_id,
                user_id=params.user_id,
                cql_filter=params.reference_area_layer_filter,
                scenario_id=params.scenario_id,
                project_id=params.project_id,
            )
        )

    # --------------------------------------------------------- main

    def process(
        self: Self, params: HeatmapV2WindmillParams, temp_dir: Path
    ) -> tuple[Path, DatasetMetadata]:
        output_path = temp_dir / "output.parquet"

        # Connectivity uses the reference area as the input domain; gravity
        # / closest_avg uses it (optional, advanced) to clip the output.
        is_connectivity = params.heatmap_type == HeatmapType.connectivity
        reference_area_path = self._resolve_reference_area(params)
        resolved_opportunities = (
            [] if is_connectivity else self._resolve_opportunities(params)
        )

        analysis_params = HeatmapV2Params(
            # Study area
            h3_resolution=params.h3_resolution,
            # Routing
            routing_mode=_ROUTING_MODE_MAP[params.routing_mode],
            cost_type=params.cost_type,
            max_cost=params.aggregate_max_cost(),
            speed=params.speed,
            # Formula
            heatmap_type=params.heatmap_type,
            decay=params.decay,
            max_sensitivity=params.max_sensitivity,
            # Opportunities + optional reference-area clip
            opportunities=resolved_opportunities,
            reference_area_path=reference_area_path,
            # Output
            output_path=str(output_path),
        )

        tool = self.tool_class()
        try:
            results = tool.run(analysis_params)
            result_path, metadata = results[0]
            return Path(result_path), metadata
        finally:
            tool.cleanup()


class HeatmapGravityV2ToolRunner(HeatmapV2ToolRunner):
    """Per-formula entry point: pre-binds heatmap_type=gravity in the UI."""

    default_output_name = get_default_layer_name("heatmap_gravity", "en")


class HeatmapClosestAverageV2ToolRunner(HeatmapV2ToolRunner):
    """Per-formula entry point: pre-binds heatmap_type=closest_average."""

    default_output_name = get_default_layer_name("heatmap_closest_average", "en")


class HeatmapConnectivityV2ToolRunner(HeatmapV2ToolRunner):
    """Per-formula entry point: pre-binds heatmap_type=connectivity in the UI.
    The base runner handles reference-area export + dispatch."""

    default_output_name = get_default_layer_name("heatmap_connectivity", "en")


def main(params: HeatmapV2WindmillParams) -> dict:
    """Formula-agnostic Windmill entry point — kept for programmatic callers.

    Toolbox tiles use the per-formula shim modules
    (`heatmap_gravity_v2`, `heatmap_closest_average_v2`) so each tile
    pre-binds heatmap_type and hides the selector.
    """
    runner = HeatmapV2ToolRunner()
    runner.init_from_env()
    try:
        return runner.run(params)
    finally:
        runner.cleanup()
