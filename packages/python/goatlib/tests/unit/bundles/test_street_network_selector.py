"""Tests for the street network bundle selector across the routing tools.

Every v2 tool that routes on the street network offers the same advanced field,
and every analysis schema behind them accepts the resulting graph. These are
contract tests: they fail when a new routing tool forgets the field, or when one
tool's copy of it drifts from the rest.
"""

import inspect
import json

import pydantic
import pytest
from goatlib.analysis.schemas.catchment_area_v2 import CatchmentAreaV2Params
from goatlib.analysis.schemas.heatmap import HuffmodelV2Params
from goatlib.analysis.schemas.heatmap_v2 import HeatmapV2Params
from goatlib.analysis.schemas.travel_cost_matrix import TravelCostMatrixParams
from goatlib.tools.registry import TOOL_REGISTRY

# The registered tools that route over the street network.
ROUTING_TOOLS = (
    "catchment_area_v2",
    "heatmap_gravity",
    "heatmap_closest_average",
    "heatmap_connectivity",
    "heatmap_2sfca",
    "huff_model",
    "travel_cost_matrix",
)

ANALYSIS_SCHEMAS = (
    CatchmentAreaV2Params,
    HeatmapV2Params,
    HuffmodelV2Params,
    TravelCostMatrixParams,
)

STREET_MODES = {"walking", "bicycle", "pedelec", "car"}


def _properties(tool_name: str) -> dict:
    definition = next(d for d in TOOL_REGISTRY if d.name == tool_name)
    return definition.get_params_class().model_json_schema()["properties"]


@pytest.mark.parametrize("tool_name", ROUTING_TOOLS)
def test_every_routing_tool_offers_the_selector(tool_name: str) -> None:
    field = _properties(tool_name).get("street_network_bundle_id")

    assert field is not None, f"{tool_name} cannot use an uploaded network"
    assert field["default"] is None, "the global network stays the default"


@pytest.mark.parametrize("tool_name", ROUTING_TOOLS)
def test_the_selector_lists_only_routable_street_bundles(tool_name: str) -> None:
    """A bundle of the wrong type, or one whose graph is still building, would
    fail at run time — so the picker filters on both."""
    ui = _properties(tool_name)["street_network_bundle_id"]["x-ui"]

    assert ui["widget"] == "bundle-selector"
    assert ui["widget_options"] == {
        "bundle_type": "street_network",
        "artifact_kind": "street_network_graph",
    }


@pytest.mark.parametrize("tool_name", ROUTING_TOOLS)
def test_the_selector_is_advanced_and_street_only(tool_name: str) -> None:
    """PT legs route on the global network, so the field is meaningless in PT
    mode; and it is an override, so it stays behind Advanced."""
    ui = _properties(tool_name)["street_network_bundle_id"]["x-ui"]
    condition = json.dumps(ui["visible_when"])

    assert "show_advanced" in condition
    modes = ui["visible_when"]["$and"][0]["routing_mode"]["$in"]
    assert set(modes) == STREET_MODES


@pytest.mark.parametrize("tool_name", ROUTING_TOOLS)
def test_raw_graph_paths_never_render(tool_name: str) -> None:
    """Tools that inherit their analysis params would otherwise show edge_path
    and node_path as free-text inputs."""
    properties = _properties(tool_name)

    for name in ("edge_path", "node_path"):
        if name in properties:
            assert (
                properties[name]["x-ui"].get("hidden") is True
            ), f"{tool_name} renders {name} as an input"


@pytest.mark.parametrize("schema", ANALYSIS_SCHEMAS, ids=lambda s: s.__name__)
def test_analysis_schemas_accept_a_graph_override(schema: type) -> None:
    assert {"edge_path", "node_path"} <= set(schema.model_fields)


@pytest.mark.parametrize("schema", ANALYSIS_SCHEMAS, ids=lambda s: s.__name__)
@pytest.mark.parametrize("half", ("edge_path", "node_path"))
def test_half_an_override_is_rejected(schema: type, half: str) -> None:
    """Edges from one network joined to another's nodes routes over a graph whose
    ids don't match, which yields a near-empty result instead of an error."""
    minimal = {
        "CatchmentAreaV2Params": dict(
            latitude=[48.137],
            longitude=[11.575],
            max_cost=15,
            output_path="/tmp/out.parquet",
        ),
        "HeatmapV2Params": dict(output_path="/tmp/out.parquet", opportunities=[]),
        "HuffmodelV2Params": dict(
            reference_area_path="a",
            demand_path="b",
            demand_field="c",
            opportunity_path="d",
            attractivity="e",
            output_path="/tmp/out.parquet",
        ),
        "TravelCostMatrixParams": dict(
            origin_latitude=[48.137],
            origin_longitude=[11.575],
            origin_id=["o"],
            destination_latitude=[48.14],
            destination_longitude=[11.58],
            destination_id=["d"],
            output_path="/tmp/out.parquet",
        ),
    }[schema.__name__]

    with pytest.raises(pydantic.ValidationError) as excinfo:
        schema(**minimal, **{half: "/tmp/graph.parquet"})

    assert "only coherent as a pair" in str(excinfo.value)


@pytest.mark.parametrize(
    "module_name",
    ("heatmap_v2", "travel_cost_matrix", "huff_model_v2", "catchment_area_v2"),
)
def test_the_override_reaches_the_routing_config(module_name: str) -> None:
    """The field is inert unless the analysis layer prefers it over the settings
    default, which is a single line easy to miss when adding a tool."""
    module = __import__(f"goatlib.analysis.accessibility.{module_name}", fromlist=["_"])
    source = inspect.getsource(module)

    assert "cfg.edge_dir = str(params.edge_path or self._edge_dir)" in source
    assert "cfg.node_dir = str(params.node_path or self._node_dir)" in source
    assert "cfg.edge_dir = self._edge_dir" not in source, "an unpatched site remains"


@pytest.mark.parametrize("tool_name", ROUTING_TOOLS)
def test_the_tool_fetches_the_graph_when_a_bundle_is_chosen(tool_name: str) -> None:
    """Without this call the selector is silently ignored and the run quietly
    uses the global network."""
    definition = next(d for d in TOOL_REGISTRY if d.name == tool_name)
    module = __import__(definition.module_path, fromlist=["_"])
    source = inspect.getsource(module)

    if "fetch_routing_network" not in source:
        # Per-formula entry points re-export the shared runner.
        source = inspect.getsource(
            __import__("goatlib.tools.heatmap_v2", fromlist=["_"])
        )

    assert "if params.street_network_bundle_id:" in source
    assert "fetch_routing_network(" in source
