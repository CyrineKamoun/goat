"""Heatmap V2 schemas — for the local C++ routing backend.

Computes accessibility scores on-the-fly (no precomputed OD matrix) by
running per-origin shortest paths and applying a heatmap formula
(Gravity / ClosestAverage) against opportunity layers.

Mirrors the catchment_area_v2 schemas style; reuses the enums where the
underlying semantics are identical to keep cross-tool consistency.
"""

from enum import StrEnum
from typing import Literal, Self

from pydantic import BaseModel, Field, model_validator

from goatlib.analysis.schemas.catchment_area_v2 import (
    CostType,
    RoutingMode,
)
from goatlib.analysis.schemas.heatmap import OpportunityGravity


class HeatmapType(StrEnum):
    gravity = "gravity"
    closest_average = "closest_average"
    connectivity = "connectivity"


class GravityDecay(StrEnum):
    gaussian = "gaussian"
    exponential = "exponential"
    linear = "linear"
    power = "power"


# Analysis-layer opportunity schema. Relaxes the v1 per-opportunity max_cost
# (which is restricted to TravelTimeLimit = Literal[3..30] minutes) to a
# plain positive int so distance-mode budgets (meters, potentially up to
# 100000) pass validation too. Adds n_destinations so each layer can specify
# its own ClosestAverage k (mirrors v1's OpportunityClosestAverage). The unit
# of max_cost (minutes vs meters) is determined by HeatmapV2Params.cost_type
# at the parent level.
class OpportunityV2(OpportunityGravity):
    max_cost: int = Field(
        default=15,
        gt=0,
        le=100000,
        description=(
            "Travel budget for this opportunity layer — minutes when "
            "cost_type=time, meters when cost_type=distance."
        ),
    )
    n_destinations: Literal[1, 2, 3, 4, 5, 6, 7, 8, 9, 10] = Field(
        default=1,
        description="Number of closest destinations to average (closest_average only).",
    )


class HeatmapV2Params(BaseModel):
    """Parameters for HeatmapV2Tool (on-the-fly C++ routing).

    cost_type + max_cost define the budget:
    - time: max_cost is minutes (e.g. 15 = 15-min walking heatmap)
    - distance: max_cost is meters
    """

    # ---- Study area ----------------------------------------------------------
    # H3 resolution for origin tiling; if omitted the tool picks per mode.
    h3_resolution: int | None = Field(
        default=None,
        ge=5,
        le=12,
        description=(
            "H3 resolution for origin tiling. If None, picked per mode "
            "(walking=10, bicycle/pedelec=9, car=8, pt=9)."
        ),
    )

    # ---- Routing -------------------------------------------------------------
    routing_mode: RoutingMode = RoutingMode.walking
    cost_type: CostType = CostType.time
    max_cost: float = Field(
        default=15.0, gt=0.0,
        description="Budget: minutes (time) or meters (distance)",
    )
    speed: float | None = Field(
        default=None, ge=0.0, le=60.0,
        description="Travel speed in km/h. None for PT/Car (mode default).",
    )

    # ---- Heatmap formula -----------------------------------------------------
    heatmap_type: HeatmapType = HeatmapType.gravity
    decay: GravityDecay = GravityDecay.gaussian
    max_sensitivity: float = Field(
        default=1_000_000.0,
        gt=0.0,
        description="Gravity sensitivity normalization anchor (matches goatlib default).",
    )
    # ---- Opportunities (1-3 layers) ------------------------------------------
    # Required for gravity / closest_average.
    opportunities: list[OpportunityV2] | None = Field(
        default=None,
        max_length=3,
        description="1-3 opportunity layers; per-layer scores plus a total are emitted.",
    )

    # ---- Connectivity --------------------------------------------------------
    reference_area_path: str | None = Field(
        default=None,
        description=(
            "Path to a polygon parquet defining the reference AOI. "
            "Required when heatmap_type == connectivity; ignored otherwise."
        ),
    )

    # ---- Output --------------------------------------------------------------
    output_path: str = Field(
        ...,
        description="Output GeoParquet path: (h3_index, geometry, "
                    "<opp_name>_accessibility..., total_accessibility).",
    )

    @model_validator(mode="after")
    def validate_inputs_by_type(self: Self) -> Self:
        if self.heatmap_type == HeatmapType.connectivity:
            if not self.reference_area_path:
                raise ValueError(
                    "connectivity requires reference_area_path."
                )
        else:
            if not self.opportunities:
                raise ValueError(
                    f"{self.heatmap_type.value} requires at least one "
                    "opportunity layer."
                )
        return self
