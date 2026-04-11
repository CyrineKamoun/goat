from __future__ import annotations

import json
import math
from pathlib import Path

import pytest

try:
    import routing
except Exception as exc:  # pragma: no cover - import guard for local dev
    routing = None
    ROUTING_IMPORT_ERROR = exc
else:
    ROUTING_IMPORT_ERROR = None

CACHE_DIR = Path(__file__).resolve().parents[4] / "apps" / "routing" / "cache"
PARQUET_FILES = list(CACHE_DIR.glob("*.parquet"))
MARIENPLATZ_3857 = (1288578.0, 6130064.0)

pytestmark = [
    pytest.mark.skipif(
        routing is None,
        reason=f"routing module is not importable: {ROUTING_IMPORT_ERROR}",
    ),
    pytest.mark.skipif(
        not PARQUET_FILES,
        reason=f"no parquet files found in {CACHE_DIR}",
    ),
]


def _make_config(**overrides):
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


def _assert_field_consistent(field):
    assert field.node_count > 0
    assert len(field.costs) == field.node_count
    assert all(c >= 0 for c in field.costs)


def _reachable_count(costs, budget):
    return sum(1 for c in costs if math.isfinite(c) and c <= budget)


def _assert_step_bucket(cost: float, step_cost: float, step_size: float) -> None:
    expected = math.ceil(cost / step_size) * step_size
    assert step_cost >= cost
    assert math.isclose(step_cost, expected, rel_tol=1e-9, abs_tol=1e-9)


def test_walking_catchment_basic():
    cfg = _make_config(mode=routing.RoutingMode.Walking, max_traveltime=10.0)
    field = routing.compute_reachability_field(cfg)

    _assert_field_consistent(field)
    assert any(c <= 1e-6 for c in field.costs)


def test_distance_mode_basic():
    cfg = _make_config(cost_mode=routing.CostMode.Distance, max_traveltime=1200.0)
    field = routing.compute_reachability_field(cfg)

    _assert_field_consistent(field)
    assert _reachable_count(field.costs, cfg.max_traveltime) > 0


def test_budget_monotonicity_time():
    short_cfg = _make_config(max_traveltime=8.0)
    long_cfg = _make_config(max_traveltime=15.0)

    short_field = routing.compute_reachability_field(short_cfg)
    long_field = routing.compute_reachability_field(long_cfg)

    short_reachable = _reachable_count(short_field.costs, short_cfg.max_traveltime)
    long_reachable = _reachable_count(long_field.costs, long_cfg.max_traveltime)

    assert long_reachable >= short_reachable


def test_speed_monotonicity_time():
    slow_cfg = _make_config(speed_km_h=4.0, max_traveltime=10.0)
    fast_cfg = _make_config(speed_km_h=6.0, max_traveltime=10.0)

    slow_field = routing.compute_reachability_field(slow_cfg)
    fast_field = routing.compute_reachability_field(fast_cfg)

    slow_reachable = _reachable_count(slow_field.costs, slow_cfg.max_traveltime)
    fast_reachable = _reachable_count(fast_field.costs, fast_cfg.max_traveltime)

    assert fast_reachable >= slow_reachable


def test_bicycle_vs_walking_reachability_smoke():
    walking_cfg = _make_config(mode=routing.RoutingMode.Walking, speed_km_h=5.0)
    bicycle_cfg = _make_config(mode=routing.RoutingMode.Bicycle, speed_km_h=15.0)

    walking_field = routing.compute_reachability_field(walking_cfg)
    bicycle_field = routing.compute_reachability_field(bicycle_cfg)

    walking_reachable = _reachable_count(
        walking_field.costs, walking_cfg.max_traveltime
    )
    bicycle_reachable = _reachable_count(
        bicycle_field.costs, bicycle_cfg.max_traveltime
    )

    assert bicycle_reachable >= walking_reachable


def test_validation_requires_starting_points():
    cfg = _make_config(starting_points=[])

    with pytest.raises(ValueError, match="starting point"):
        routing.compute_reachability_field(cfg)


def test_validation_requires_positive_max_traveltime():
    cfg = _make_config(max_traveltime=0.0)

    with pytest.raises(ValueError, match="max_traveltime"):
        routing.compute_reachability_field(cfg)


def test_validation_requires_edge_dir():
    cfg = _make_config(edge_dir="")

    with pytest.raises(ValueError, match="edge_dir"):
        routing.compute_reachability_field(cfg)


def test_runtime_error_for_disconnected_start_point():
    far_point_cfg = _make_config(starting_points=[routing.Point3857(0.0, 0.0)])

    with pytest.raises(RuntimeError, match="No edges loaded"):
        routing.compute_reachability_field(far_point_cfg)


def test_geojson_api_returns_feature_collection():
    cfg = _make_config(catchment_type=routing.CatchmentType.Network)
    payload = routing.compute_catchment(cfg)

    doc = json.loads(payload)
    assert doc["type"] == "FeatureCollection"
    assert isinstance(doc["features"], list)
    assert len(doc["features"]) > 0

    first = doc["features"][0]
    assert first["type"] == "Feature"
    assert first["geometry"]["type"] == "LineString"
    assert "cost" in first["properties"]
    assert "step_cost" in first["properties"]


def test_geojson_network_step_cost_bucket_is_correct():
    cfg = _make_config(catchment_type=routing.CatchmentType.Network, steps=8)
    payload = routing.compute_catchment(cfg)
    doc = json.loads(payload)

    assert len(doc["features"]) > 0
    step_size = cfg.max_traveltime / cfg.steps

    for feature in doc["features"]:
        props = feature["properties"]
        _assert_step_bucket(props["cost"], props["step_cost"], step_size)


def test_geojson_network_linestring_coordinates_are_valid():
    cfg = _make_config(catchment_type=routing.CatchmentType.Network)
    payload = routing.compute_catchment(cfg)
    doc = json.loads(payload)

    assert len(doc["features"]) > 0
    for feature in doc["features"]:
        geom = feature["geometry"]
        assert geom["type"] == "LineString"
        coords = geom["coordinates"]
        assert isinstance(coords, list)
        assert len(coords) >= 2
        for point in coords:
            assert isinstance(point, list)
            assert len(point) == 2
            assert all(isinstance(v, (int, float)) for v in point)


def test_geojson_h3_grid_returns_point_features():
    cfg = _make_config(catchment_type=routing.CatchmentType.HexagonalGrid)
    payload = routing.compute_catchment(cfg)
    doc = json.loads(payload)

    assert doc["type"] == "FeatureCollection"
    assert isinstance(doc["features"], list)
    assert len(doc["features"]) > 0

    first = doc["features"][0]
    assert first["geometry"]["type"] == "Polygon"
    ring = first["geometry"]["coordinates"][0]
    assert isinstance(ring, list)
    assert len(ring) >= 6
    assert "h3" in first["properties"]
    assert "resolution" in first["properties"]
    assert first["properties"]["resolution"] == 10
    assert "cost" in first["properties"]
    assert "step_cost" in first["properties"]


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
def test_geojson_h3_resolution_depends_on_mode(mode, expected_resolution):
    cfg = _make_config(catchment_type=routing.CatchmentType.HexagonalGrid, mode=mode)
    payload = routing.compute_catchment(cfg)
    doc = json.loads(payload)

    assert len(doc["features"]) > 0
    assert doc["features"][0]["properties"]["resolution"] == expected_resolution


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
def test_geojson_api_always_returns_geojson(catchment_type):
    cfg = _make_config(catchment_type=catchment_type)
    payload = routing.compute_catchment(cfg)
    doc = json.loads(payload)

    assert doc["type"] == "FeatureCollection"
    assert isinstance(doc["features"], list)
