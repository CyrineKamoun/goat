"""Catchment Area V2 schemas — for the local C++ routing backend.

Exposes all parameters supported by the C++ RequestConfig.
"""

from enum import StrEnum
from typing import Any, Self

from pydantic import BaseModel, Field, model_validator


class RoutingMode(StrEnum):
    walking = "walking"
    bicycle = "bicycle"
    pedelec = "pedelec"
    car = "car"
    pt = "pt"


class CostType(StrEnum):
    time = "time"
    distance = "distance"


class CatchmentType(StrEnum):
    polygon = "polygon"
    network = "network"
    hexagonal_grid = "hexagonal_grid"
    point_grid = "point_grid"


class OutputFormat(StrEnum):
    geojson = "geojson"
    parquet = "parquet"


class AccessEgressMode(StrEnum):
    walk = "walk"
    bicycle = "bicycle"
    pedelec = "pedelec"
    car = "car"


class PTMode(StrEnum):
    bus = "bus"
    tram = "tram"
    rail = "rail"
    subway = "subway"
    ferry = "ferry"
    cable_car = "cable_car"
    gondola = "gondola"
    funicular = "funicular"


class Weekday(StrEnum):
    weekday = "weekday"
    saturday = "saturday"
    sunday = "sunday"


class PTTimeWindow(BaseModel):
    """Time window for PT departure sweep."""

    weekday: Weekday = Weekday.weekday
    from_time: int = Field(
        ..., description="Start time in seconds from midnight (e.g. 25200 = 07:00)"
    )
    to_time: int = Field(
        ..., description="End time in seconds from midnight (e.g. 32400 = 09:00)"
    )


class CatchmentAreaV2Params(BaseModel):
    """Parameters for CatchmentAreaToolV2 (local C++ routing backend).

    Maps directly to the C++ RequestConfig. All transport modes are supported.

    cost_type + max_cost define the budget:
    - time: max_cost is minutes (e.g. 15 = 15 min catchment)
    - distance: max_cost is meters (e.g. 2000 = 2km catchment)
    """

    # Starting point(s) in WGS84
    latitude: float | list[float]
    longitude: float | list[float]

    # Mode & cost
    routing_mode: RoutingMode = RoutingMode.walking
    cost_type: CostType = CostType.time
    max_cost: float = Field(
        default=15.0, gt=0,
        description="Budget: minutes (time) or meters (distance)",
    )
    speed: float = Field(default=5.0, ge=1.0, le=50.0, description="Travel speed in km/h")
    steps: int = Field(default=3, ge=1, le=20, description="Number of isochrone steps")
    cutoffs: list[int] | None = Field(
        default=None,
        description="Explicit step thresholds (minutes or meters). Overrides steps.",
    )

    # Output
    catchment_type: CatchmentType = CatchmentType.polygon
    output_format: OutputFormat = OutputFormat.parquet
    output_path: str
    polygon_difference: bool = True

    # PT settings
    transit_modes: list[PTMode] | None = None
    time_window: PTTimeWindow | None = None
    max_transfers: int = Field(default=5, ge=0, le=10, description="RAPTOR transfer limit")

    # PT access/egress
    access_mode: AccessEgressMode = AccessEgressMode.walk
    egress_mode: AccessEgressMode = AccessEgressMode.walk
    access_cost_type: CostType = CostType.time
    egress_cost_type: CostType = CostType.time
    access_max_cost: float = Field(default=15.0, ge=0.0, description="Access leg budget: minutes (time) or meters (distance). 0 = default (15 min / 500 m).")
    egress_max_cost: float = Field(default=15.0, ge=0.0, description="Egress leg budget: minutes (time) or meters (distance). 0 = default (15 min / 500 m).")
    access_speed: float = Field(default=0.0, ge=0.0, description="Access leg speed in km/h (time cost type only, 0 = use speed)")
    egress_speed: float = Field(default=0.0, ge=0.0, description="Egress leg speed in km/h (time cost type only, 0 = use speed)")

    # PointGrid settings
    grid_points_path: str | None = Field(default=None, description="Parquet with grid points (id, x_3857, y_3857)")
    grid_snap_distance: float = Field(default=0.0, ge=0.0, description="PointGrid snap distance in meters (0 = default 500m)")

    @model_validator(mode="after")
    def validate_pt_settings(self: Self) -> Self:
        if self.routing_mode == RoutingMode.pt:
            if not self.transit_modes:
                raise ValueError("transit_modes is required for PT mode")
            if not self.time_window:
                raise ValueError("time_window is required for PT mode")
        return self

    @model_validator(mode="after")
    def validate_point_grid(self: Self) -> Self:
        if self.catchment_type == CatchmentType.point_grid and not self.grid_points_path:
            raise ValueError("grid_points_path is required for point_grid catchment type")
        return self

    @model_validator(mode="after")
    def validate_polygon_difference(self: Self) -> Self:
        if self.polygon_difference and self.catchment_type != CatchmentType.polygon:
            self.polygon_difference = False
        return self
