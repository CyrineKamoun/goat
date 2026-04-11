"""Travel Cost Matrix schemas — for the local C++ routing backend.

Computes many-to-many travel costs between origin and destination points.
"""

from datetime import time
from enum import StrEnum
from typing import Self

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
    weekday: Weekday = Weekday.weekday
    from_time: int = Field(
        ..., description="Start time in seconds from midnight"
    )
    to_time: int = Field(
        ..., description="End time in seconds from midnight"
    )


class TravelCostMatrixParams(BaseModel):
    """Parameters for TravelCostMatrixTool (local C++ routing backend).

    Computes travel cost from every origin to every destination.
    Results: a matrix table (origin_id, destination_id, cost) and
    destination points annotated with their minimum cost from any origin.
    """

    # Origin/destination coordinates in WGS84
    origin_latitude: list[float]
    origin_longitude: list[float]
    destination_latitude: list[float]
    destination_longitude: list[float]

    # Routing
    routing_mode: RoutingMode = RoutingMode.walking
    cost_type: CostType = CostType.time
    max_cost: float = Field(
        default=30.0, gt=0,
        description="Budget: minutes (time) or meters (distance). "
                    "Origin-destination pairs beyond this cost get null.",
    )
    speed: float = Field(
        default=5.0, ge=1.0, le=50.0,
        description="Travel speed in km/h (time cost type only).",
    )

    # PT settings
    transit_modes: list[PTMode] | None = None
    time_window: PTTimeWindow | None = None
    max_transfers: int = Field(default=5, ge=0, le=10)
    access_mode: AccessEgressMode = AccessEgressMode.walk
    egress_mode: AccessEgressMode = AccessEgressMode.walk
    access_speed: float = Field(default=0.0, ge=0.0, description="Access speed km/h (0 = use speed)")
    egress_speed: float = Field(default=0.0, ge=0.0, description="Egress speed km/h (0 = use speed)")

    # Output
    output_path: str = Field(
        ..., description="Path for the matrix parquet output."
    )

    @model_validator(mode="after")
    def validate_coordinates(self: Self) -> Self:
        if len(self.origin_latitude) != len(self.origin_longitude):
            raise ValueError("Origin latitude and longitude must have the same length")
        if len(self.destination_latitude) != len(self.destination_longitude):
            raise ValueError("Destination latitude and longitude must have the same length")
        if not self.origin_latitude:
            raise ValueError("At least one origin is required")
        if not self.destination_latitude:
            raise ValueError("At least one destination is required")
        return self

    @model_validator(mode="after")
    def validate_pt_settings(self: Self) -> Self:
        if self.routing_mode == RoutingMode.pt:
            if not self.transit_modes:
                raise ValueError("transit_modes is required for PT mode")
            if not self.time_window:
                raise ValueError("time_window is required for PT mode")
        return self
