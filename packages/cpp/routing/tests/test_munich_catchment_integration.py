"""Integration tests for the C++ routing engine using Munich parquet data.

These tests exercise the pybind11 bindings for compute_catchment() against
real network edges from apps/routing/cache. They validate:
  - Basic catchment computation for all routing modes
  - Time and distance cost modes
  - Budget monotonicity (more time → more reachable nodes)
  - Speed monotonicity (faster speed → more reachable nodes)
  - Relative reachability across modes (bicycle > walking, car > bicycle)
  - All output type variants
  - Polygon difference mode
  - Input validation error handling
"""

from __future__ import annotations

import json
import math
from pathlib import Path

import pytest

try:
    import routing
except Exception as exc:
    routing = None
    ROUTING_IMPORT_ERROR = exc
else:
    ROUTING_IMPORT_ERROR = None

CACHE_DIR = Path(__file__).resolve().parents[4] / "apps" / "routing" / "cache"
PARQUET_FILES = list(CACHE_DIR.glob("*_edge.parquet"))

# Marienplatz, Munich in EPSG:3857
MARIENPLATZ_3857 = (1288578.0, 6130064.0)

pytestmark = [
    pytest.mark.skipif(
        routing is None,
        reason=f"routing module is not importable: {ROUTING_IMPORT_ERROR}",
    ),
    pytest.mark.skipif(
        not PARQUET_FILES,
        reason=f"no *_edge.parquet files found in {CACHE_DIR}",
    ),
    pytest.mark.integration,
]


def _make_config(**overrides):
    """Build a RequestConfig for Marienplatz with sensible defaults."""
    cfg = routing.RequestConfig()
    cfg.starting_points = [routing.Point3857(*MARIENPLATZ_3857)]
    cfg.mode = routing.RoutingMode.Walking
    cfg.cost_mode = routing.CostMode.Time
    cfg.max_traveltime = 10.0
    cfg.steps = 10
    cfg.speed_km_h = 5.0
    cfg.edge_dir = str(CACHE_DIR)
    cfg.catchment_type = routing.CatchmentType.Network
    cfg.output_format = routing.OutputFormat.GeoJSON
    cfg.polygon_difference = False
    for key, value in overrides.items():
        setattr(cfg, key, value)
    return cfg


def _reachable_count(costs, budget):
    """Count nodes reachable within the given cost budget."""
    return sum(1 for c in costs if math.isfinite(c) and c <= budget)


def _assert_field_valid(field):
    """Assert that a ReachabilityField has consistent, non-negative costs."""
    assert field.node_count > 0, "field should contain nodes"
    assert len(field.costs) == field.node_count
    assert all(c >= 0 for c in field.costs), "all costs must be non-negative"


def _assert_step_bucket(cost: float, step_cost: float, step_size: float) -> None:
    expected = math.ceil(cost / step_size) * step_size
    assert step_cost >= cost
    assert math.isclose(step_cost, expected, rel_tol=1e-9, abs_tol=1e-9)


# ── Smoke tests ──────────────────────────────────────────────────────────


class TestSmoke:
    """Basic smoke tests: each mode produces a valid catchment."""

    def test_walking(self):
        cfg = _make_config(mode=routing.RoutingMode.Walking, max_traveltime=10.0)
        field = routing.compute_reachability_field(cfg)
        _assert_field_valid(field)
        assert _reachable_count(field.costs, 10.0) > 0

    def test_bicycle(self):
        cfg = _make_config(
            mode=routing.RoutingMode.Bicycle, speed_km_h=15.0, max_traveltime=10.0
        )
        field = routing.compute_reachability_field(cfg)
        _assert_field_valid(field)
        assert _reachable_count(field.costs, 10.0) > 0

    def test_pedelec(self):
        cfg = _make_config(
            mode=routing.RoutingMode.Pedelec, speed_km_h=20.0, max_traveltime=10.0
        )
        field = routing.compute_reachability_field(cfg)
        _assert_field_valid(field)
        assert _reachable_count(field.costs, 10.0) > 0

    def test_car(self):
        cfg = _make_config(
            mode=routing.RoutingMode.Car, speed_km_h=30.0, max_traveltime=10.0
        )
        field = routing.compute_reachability_field(cfg)
        _assert_field_valid(field)
        assert _reachable_count(field.costs, 10.0) > 0

    def test_origin_has_zero_cost(self):
        """The starting point should have cost ~0."""
        cfg = _make_config(max_traveltime=10.0)
        field = routing.compute_reachability_field(cfg)
        assert min(field.costs) < 0.01


# ── Cost mode tests ─────────────────────────────────────────────────────


class TestCostModes:
    """Test time vs. distance cost modes."""

    def test_distance_mode_produces_results(self):
        cfg = _make_config(cost_mode=routing.CostMode.Distance, max_traveltime=1200.0)
        field = routing.compute_reachability_field(cfg)
        _assert_field_valid(field)
        assert _reachable_count(field.costs, 1200.0) > 0

    def test_time_mode_produces_results(self):
        cfg = _make_config(cost_mode=routing.CostMode.Time, max_traveltime=10.0)
        field = routing.compute_reachability_field(cfg)
        _assert_field_valid(field)
        assert _reachable_count(field.costs, 10.0) > 0


# ── Monotonicity tests ──────────────────────────────────────────────────


class TestMonotonicity:
    """Increasing budget or speed should yield more reachable nodes."""

    def test_time_budget_monotonicity(self):
        short = routing.compute_reachability_field(_make_config(max_traveltime=5.0))
        long = routing.compute_reachability_field(_make_config(max_traveltime=15.0))
        assert _reachable_count(long.costs, 15.0) > _reachable_count(short.costs, 5.0)

    def test_speed_monotonicity(self):
        slow = routing.compute_reachability_field(_make_config(speed_km_h=3.0))
        fast = routing.compute_reachability_field(_make_config(speed_km_h=7.0))
        assert _reachable_count(fast.costs, 10.0) >= _reachable_count(
            slow.costs, 10.0
        )

    def test_distance_budget_monotonicity(self):
        short = routing.compute_reachability_field(
            _make_config(cost_mode=routing.CostMode.Distance, max_traveltime=500.0)
        )
        long = routing.compute_reachability_field(
            _make_config(cost_mode=routing.CostMode.Distance, max_traveltime=2000.0)
        )
        assert _reachable_count(long.costs, 2000.0) > _reachable_count(
            short.costs, 500.0
        )


# ── Mode comparison tests ───────────────────────────────────────────────


class TestModeComparison:
    """Compare reachability across transport modes."""

    def test_bicycle_reaches_more_than_walking(self):
        walk = routing.compute_reachability_field(
            _make_config(mode=routing.RoutingMode.Walking, speed_km_h=5.0)
        )
        bike = routing.compute_reachability_field(
            _make_config(mode=routing.RoutingMode.Bicycle, speed_km_h=15.0)
        )
        assert _reachable_count(bike.costs, 10.0) > _reachable_count(
            walk.costs, 10.0
        )

    def test_car_reaches_more_than_bicycle(self):
        bike = routing.compute_reachability_field(
            _make_config(mode=routing.RoutingMode.Bicycle, speed_km_h=15.0)
        )
        car = routing.compute_reachability_field(
            _make_config(mode=routing.RoutingMode.Car, speed_km_h=50.0)
        )
        assert _reachable_count(car.costs, 10.0) > _reachable_count(
            bike.costs, 10.0
        )


# ── Output type tests ───────────────────────────────────────────────────


class TestOutputTypes:
    """All OutputType variants should produce valid results."""

    @pytest.mark.parametrize(
        "catchment_type",
        [
            routing.CatchmentType.Polygon,
            routing.CatchmentType.Network,
            routing.CatchmentType.HexagonalGrid,
        ]
        if routing
        else [],
    )
    def test_output_type_produces_valid_field(self, catchment_type):
        cfg = _make_config(catchment_type=catchment_type)
        field = routing.compute_reachability_field(cfg)
        _assert_field_valid(field)
        assert _reachable_count(field.costs, 10.0) > 0

    def test_polygon_difference_mode(self):
        cfg = _make_config(
            catchment_type=routing.CatchmentType.Polygon, polygon_difference=True
        )
        field = routing.compute_reachability_field(cfg)
        _assert_field_valid(field)

    def test_results_consistent_across_output_types(self):
        """Network and Polygon output should yield the same reachability field."""
        net = routing.compute_reachability_field(
            _make_config(catchment_type=routing.CatchmentType.Network)
        )
        poly = routing.compute_reachability_field(
            _make_config(catchment_type=routing.CatchmentType.Polygon)
        )
        assert net.node_count == poly.node_count
        assert _reachable_count(net.costs, 10.0) == _reachable_count(
            poly.costs, 10.0
        )

    @pytest.mark.parametrize(
        "catchment_type",
        [
            routing.CatchmentType.Polygon,
            routing.CatchmentType.Network,
            routing.CatchmentType.HexagonalGrid,
        ]
        if routing
        else [],
    )
    def test_geojson_api_returns_feature_collection(self, catchment_type):
        cfg = _make_config(catchment_type=catchment_type)
        payload = routing.compute_catchment(cfg)

        doc = json.loads(payload)
        assert doc["type"] == "FeatureCollection"
        assert isinstance(doc["features"], list)

    def test_geojson_network_contains_linestrings(self):
        cfg = _make_config(catchment_type=routing.CatchmentType.Network)
        payload = routing.compute_catchment(cfg)
        doc = json.loads(payload)

        assert len(doc["features"]) > 0
        first = doc["features"][0]
        assert first["type"] == "Feature"
        assert first["geometry"]["type"] == "LineString"
        assert "id" in first["properties"]
        assert "cost" in first["properties"]
        assert "step_cost" in first["properties"]

    def test_geojson_network_coordinates_are_wgs84(self):
        cfg = _make_config(catchment_type=routing.CatchmentType.Network)
        payload = routing.compute_catchment(cfg)
        doc = json.loads(payload)

        assert len(doc["features"]) > 0
        first = doc["features"][0]
        coords = first["geometry"]["coordinates"]
        assert len(coords) >= 2

        for lon, lat in coords[:10]:
            assert -180.0 <= lon <= 180.0
            assert -90.0 <= lat <= 90.0

    def test_geojson_network_feature_properties_are_numeric(self):
        cfg = _make_config(catchment_type=routing.CatchmentType.Network)
        payload = routing.compute_catchment(cfg)
        doc = json.loads(payload)

        assert len(doc["features"]) > 0
        for feature in doc["features"]:
            props = feature["properties"]
            assert isinstance(props["id"], int)
            assert isinstance(props["cost"], (int, float))
            assert isinstance(props["step_cost"], (int, float))

    def test_geojson_network_step_cost_bucket_is_correct(self):
        cfg = _make_config(catchment_type=routing.CatchmentType.Network, steps=6)
        payload = routing.compute_catchment(cfg)
        doc = json.loads(payload)

        assert len(doc["features"]) > 0
        step_size = cfg.max_traveltime / cfg.steps
        for feature in doc["features"]:
            props = feature["properties"]
            _assert_step_bucket(props["cost"], props["step_cost"], step_size)

    def test_geojson_hexagonal_grid_contains_h3_points(self):
        cfg = _make_config(catchment_type=routing.CatchmentType.HexagonalGrid)
        payload = routing.compute_catchment(cfg)
        doc = json.loads(payload)

        assert len(doc["features"]) > 0
        first = doc["features"][0]
        assert first["geometry"]["type"] == "Polygon"
        ring = first["geometry"]["coordinates"][0]
        assert isinstance(ring, list)
        assert len(ring) >= 6
        props = first["properties"]
        assert "h3" in props
        assert "resolution" in props
        assert "cost" in props
        assert "step_cost" in props

    @pytest.mark.parametrize(
        "mode,expected_resolution",
        [
            (routing.RoutingMode.Car, 8),
            (routing.RoutingMode.Bicycle, 9),
            (routing.RoutingMode.Pedelec, 9),
            (routing.RoutingMode.Walking, 10),
        ]
        if routing
        else [],
    )
    def test_geojson_hexagonal_grid_resolution_depends_on_mode(
        self, mode, expected_resolution
    ):
        cfg = _make_config(catchment_type=routing.CatchmentType.HexagonalGrid, mode=mode)
        payload = routing.compute_catchment(cfg)
        doc = json.loads(payload)

        assert len(doc["features"]) > 0
        assert doc["features"][0]["properties"]["resolution"] == expected_resolution


# ── Validation tests ────────────────────────────────────────────────────


class TestValidation:
    """Input validation should reject invalid configs."""

    def test_empty_starting_points_raises(self):
        cfg = _make_config(starting_points=[])
        with pytest.raises((ValueError, RuntimeError)):
            routing.compute_reachability_field(cfg)

    def test_zero_traveltime_raises(self):
        cfg = _make_config(max_traveltime=0.0)
        with pytest.raises((ValueError, RuntimeError)):
            routing.compute_reachability_field(cfg)

    def test_empty_edge_dir_raises(self):
        cfg = _make_config(edge_dir="")
        with pytest.raises((ValueError, RuntimeError)):
            routing.compute_reachability_field(cfg)

    def test_far_start_point_snaps_to_network(self):
        """A far point outside cached H3 coverage should fail with no loaded edges."""
        far_point = routing.Point3857(0.0, 0.0)
        cfg = _make_config(starting_points=[far_point])
        with pytest.raises(RuntimeError, match="No edges loaded"):
            routing.compute_reachability_field(cfg)


