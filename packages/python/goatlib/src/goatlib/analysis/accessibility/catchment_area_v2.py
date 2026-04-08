"""Catchment Area V2 — local routing backend.

Computes catchment areas for all transport modes via the C++ routing package
(walking, bicycle, pedelec, car, public transport). No external HTTP services.
"""

import importlib
import logging
import math
import time
from datetime import date, datetime, time as time_of_day, timedelta, timezone
from pathlib import Path
from typing import Any, Self

from goatlib.analysis.core.base import AnalysisTool
from goatlib.analysis.schemas.catchment_area_v2 import (
    AccessEgressMode,
    CatchmentAreaV2Params,
    CatchmentType,
    CostType,
    OutputFormat,
    RoutingMode,
)
from goatlib.config.settings import settings
from goatlib.models.io import DatasetMetadata

logger = logging.getLogger(__name__)

WEB_MERCATOR_RADIUS_M = 6378137.0

DEFAULT_ROUTING_EDGE_DIR = "/app/apps/routing/cache/street_network/hive"
DEFAULT_ROUTING_TIMETABLE_PATH = "/app/data/gtfs/latest_de.bin"


class CatchmentAreaToolV2(AnalysisTool):
    """Compute catchment areas via the local C++ routing package.

    All modes (walking, bicycle, pedelec, car, public transport) use the same
    local routing backend. No external HTTP calls.

    Example:
        tool = CatchmentAreaToolV2()
        result = tool.run(CatchmentAreaV2Params(
            latitude=48.137, longitude=11.576,
            routing_mode="walking", cost_type="time", max_cost=15,
            steps=3, output_path="output/catchment.parquet",
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
    def _pt_departure_unix_minutes(params: CatchmentAreaV2Params) -> int:
        weekday_value = "weekday"
        from_seconds = 25200  # 07:00 default

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

    def _build_request_config(
        self: Self,
        params: CatchmentAreaV2Params,
    ) -> Any:
        routing = self._get_routing_module()

        lat_list = (
            [params.latitude]
            if isinstance(params.latitude, (int, float))
            else list(params.latitude)
        )
        lon_list = (
            [params.longitude]
            if isinstance(params.longitude, (int, float))
            else list(params.longitude)
        )
        if len(lat_list) != len(lon_list):
            raise ValueError("Latitude and longitude must have the same length")

        mode_map = {
            RoutingMode.walking: routing.RoutingMode.Walking,
            RoutingMode.bicycle: routing.RoutingMode.Bicycle,
            RoutingMode.pedelec: routing.RoutingMode.Pedelec,
            RoutingMode.car: routing.RoutingMode.Car,
            RoutingMode.pt: routing.RoutingMode.PublicTransport,
        }
        catchment_map = {
            CatchmentType.polygon: routing.CatchmentType.Polygon,
            CatchmentType.network: routing.CatchmentType.Network,
            CatchmentType.hexagonal_grid: routing.CatchmentType.HexagonalGrid,
            CatchmentType.point_grid: routing.CatchmentType.PointGrid,
        }
        access_mode_map = {
            AccessEgressMode.walk: routing.RoutingMode.Walking,
            AccessEgressMode.bicycle: routing.RoutingMode.Bicycle,
            AccessEgressMode.pedelec: routing.RoutingMode.Pedelec,
            AccessEgressMode.car: routing.RoutingMode.Car,
        }

        cfg = routing.RequestConfig()
        cfg.starting_points = [
            routing.Point3857(*self._to_web_mercator(lon, lat))
            for lat, lon in zip(lat_list, lon_list)
        ]
        cfg.mode = mode_map[params.routing_mode]
        cfg.cost_type = (
            routing.CostType.Distance
            if params.cost_type == CostType.distance
            else routing.CostType.Time
        )
        cfg.max_cost = params.max_cost
        cfg.steps = params.steps
        cfg.speed_km_h = params.speed
        cfg.edge_dir = self._edge_dir
        cfg.output_path = params.output_path
        cfg.catchment_type = catchment_map[params.catchment_type]
        cfg.output_format = (
            routing.OutputFormat.GeoJSON
            if params.output_format == OutputFormat.geojson
            else routing.OutputFormat.Parquet
        )
        cfg.polygon_difference = params.polygon_difference

        if params.cutoffs:
            cfg.cutoffs = list(params.cutoffs)

        # PT settings
        if params.routing_mode == RoutingMode.pt:
            cfg.timetable_path = str(self._timetable_path)
            cfg.departure_time = self._pt_departure_unix_minutes(params)
            cfg.max_transfers = params.max_transfers
            cfg.access_mode = access_mode_map[params.access_mode]
            cfg.egress_mode = access_mode_map[params.egress_mode]
            cfg.access_cost_type = (
                routing.CostType.Distance
                if params.access_cost_type == CostType.distance
                else routing.CostType.Time
            )
            cfg.egress_cost_type = (
                routing.CostType.Distance
                if params.egress_cost_type == CostType.distance
                else routing.CostType.Time
            )
            cfg.access_max_cost = params.access_max_cost
            cfg.egress_max_cost = params.egress_max_cost
            cfg.access_speed_km_h = params.access_speed
            cfg.egress_speed_km_h = params.egress_speed

            if params.transit_modes:
                cfg.transit_modes = [m.value for m in params.transit_modes]

            if params.time_window:
                from_sec = params.time_window.from_time
                to_sec = params.time_window.to_time
                window_min = max(0, (to_sec - from_sec) // 60)
                if window_min > 0:
                    cfg.departure_window = window_min

        # PointGrid settings
        if params.grid_points_path:
            cfg.grid_points_path = params.grid_points_path
        if params.grid_snap_distance > 0:
            cfg.grid_snap_distance = params.grid_snap_distance

        return cfg

    def _run_implementation(
        self: Self, params: CatchmentAreaV2Params
    ) -> list[tuple[Path, DatasetMetadata]]:
        routing = self._get_routing_module()
        cfg = self._build_request_config(params)

        path = Path(params.output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        cfg.output_path = str(path)

        t0 = time.perf_counter()
        result = routing.compute_catchment(cfg)
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        logger.info("compute_catchment total_ms=%.1f", elapsed_ms)

        if params.output_format == OutputFormat.geojson:
            path.write_text(result, encoding="utf-8")

        metadata = DatasetMetadata(
            path=str(path),
            source_type="vector",
            format="geoparquet",
            geometry_type="Polygon",
            geometry_column="geometry",
        )
        return [(path, metadata)]
