"""Travel Cost Matrix — local routing backend.

Computes many-to-many travel costs between origin and destination points
via the C++ routing package. Supports all transport modes including PT.
"""

import importlib
import logging
import math
import time
from datetime import date, datetime, time as time_of_day, timedelta, timezone
from pathlib import Path
from typing import Any, Self

from goatlib.analysis.core.base import AnalysisTool
from goatlib.analysis.schemas.travel_cost_matrix import (
    AccessEgressMode,
    CostType,
    RoutingMode,
    TravelCostMatrixParams,
)
from goatlib.config.settings import settings
from goatlib.models.io import DatasetMetadata

logger = logging.getLogger(__name__)

WEB_MERCATOR_RADIUS_M = 6378137.0

DEFAULT_ROUTING_EDGE_DIR = "/app/apps/routing/cache/street_network/hive"
DEFAULT_ROUTING_TIMETABLE_PATH = "/app/data/gtfs/latest_de.bin"


class TravelCostMatrixTool(AnalysisTool):
    """Compute many-to-many travel costs via the local C++ routing package.

    Supports walking, bicycle, pedelec, car, and public transport.

    Example:
        tool = TravelCostMatrixTool()
        result = tool.run(TravelCostMatrixParams(
            origin_latitude=[48.137], origin_longitude=[11.576],
            destination_latitude=[48.14, 48.15], destination_longitude=[11.58, 11.59],
            routing_mode="walking", cost_type="time", max_cost=30,
            output_path="output/matrix.parquet",
        ))
    """

    def __init__(self: Self) -> None:
        super().__init__()
        self._edge_dir = getattr(
            settings.routing, "street_network_dir", DEFAULT_ROUTING_EDGE_DIR
        )
        self._timetable_path = getattr(
            settings.routing, "pt_timetable_path", DEFAULT_ROUTING_TIMETABLE_PATH
        )

    @staticmethod
    def _get_routing_module() -> Any:
        try:
            return importlib.import_module("routing")
        except Exception as exc:
            raise RuntimeError(
                "Local routing package is not available. "
                "Install the 'routing' package (packages/cpp/routing)."
            ) from exc

    @staticmethod
    def _to_web_mercator(lon: float, lat: float) -> tuple[float, float]:
        x = WEB_MERCATOR_RADIUS_M * math.radians(lon)
        y = WEB_MERCATOR_RADIUS_M * math.log(
            math.tan(math.pi / 4.0 + math.radians(lat) / 2.0)
        )
        return x, y

    @staticmethod
    def _pt_departure_unix_minutes(params: TravelCostMatrixParams) -> int:
        weekday_value = "weekday"
        from_seconds = 25200

        if params.time_window:
            weekday_value = (
                params.time_window.weekday.value
                if hasattr(params.time_window.weekday, "value")
                else str(params.time_window.weekday)
            )
            from_seconds = params.time_window.from_time

        weekday_dates = {
            "weekday": date(2026, 3, 10),
            "saturday": date(2026, 3, 14),
            "sunday": date(2026, 3, 15),
        }
        anchor_date = weekday_dates.get(weekday_value, weekday_dates["weekday"])
        departure_dt = datetime.combine(
            anchor_date, time_of_day.min, tzinfo=timezone.utc
        ) + timedelta(seconds=from_seconds)
        return int(departure_dt.timestamp() // 60)

    def _build_matrix_config(
        self: Self,
        params: TravelCostMatrixParams,
    ) -> Any:
        routing = self._get_routing_module()

        origins = [
            routing.Point3857(*self._to_web_mercator(lon, lat))
            for lat, lon in zip(params.origin_latitude, params.origin_longitude)
        ]
        destinations = [
            routing.Point3857(*self._to_web_mercator(lon, lat))
            for lat, lon in zip(params.destination_latitude, params.destination_longitude)
        ]

        mode_map = {
            RoutingMode.walking: routing.RoutingMode.Walking,
            RoutingMode.bicycle: routing.RoutingMode.Bicycle,
            RoutingMode.pedelec: routing.RoutingMode.Pedelec,
            RoutingMode.car: routing.RoutingMode.Car,
            RoutingMode.pt: routing.RoutingMode.PublicTransport,
        }
        access_mode_map = {
            AccessEgressMode.walk: routing.RoutingMode.Walking,
            AccessEgressMode.bicycle: routing.RoutingMode.Bicycle,
            AccessEgressMode.pedelec: routing.RoutingMode.Pedelec,
            AccessEgressMode.car: routing.RoutingMode.Car,
        }

        cfg = routing.MatrixConfig()
        cfg.origins = origins
        cfg.destinations = destinations
        cfg.mode = mode_map[params.routing_mode]
        cfg.cost_type = (
            routing.CostType.Distance
            if params.cost_type == CostType.distance
            else routing.CostType.Time
        )
        cfg.max_cost = params.max_cost
        cfg.speed_km_h = params.speed
        cfg.edge_dir = self._edge_dir
        cfg.output_path = params.output_path

        if params.routing_mode == RoutingMode.pt:
            cfg.timetable_path = str(self._timetable_path)
            cfg.departure_time = self._pt_departure_unix_minutes(params)
            cfg.max_transfers = params.max_transfers
            cfg.access_mode = access_mode_map[params.access_mode]
            cfg.egress_mode = access_mode_map[params.egress_mode]
            if params.access_speed > 0:
                cfg.access_speed_km_h = params.access_speed
            if params.egress_speed > 0:
                cfg.egress_speed_km_h = params.egress_speed

            if params.transit_modes:
                cfg.transit_modes = [m.value for m in params.transit_modes]

            if params.time_window:
                from_sec = params.time_window.from_time
                to_sec = params.time_window.to_time
                window_min = max(0, (to_sec - from_sec) // 60)
                if window_min > 0:
                    cfg.departure_window = window_min

        return cfg

    def _run_implementation(
        self: Self, params: TravelCostMatrixParams
    ) -> list[tuple[Path, DatasetMetadata]]:
        routing = self._get_routing_module()
        cfg = self._build_matrix_config(params)

        path = Path(params.output_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        t0 = time.perf_counter()
        routing.compute_travel_cost_matrix(cfg)
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        logger.info("compute_travel_cost_matrix total_ms=%.1f", elapsed_ms)

        metadata = DatasetMetadata(
            path=str(path),
            source_type="vector",
            format="geoparquet",
            geometry_type="Point",
            geometry_column="geometry",
        )
        return [(path, metadata)]
