"""Where the bundle selectors sit among a routing tool's fields.

Each selector has a section of its own — "Street Network", "Public Transport
Network" — placed after routing and before configuration. After routing because
which network is relevant follows from the mode that was chosen; before
configuration because it decides what the settings there apply to.

Only one of the two is ever on screen: the mode decides which, so they share a
position rather than stacking.
"""

import pytest
from goatlib.tools.registry import TOOL_REGISTRY

ROUTING_TOOLS = (
    "catchment_area_v2",
    "heatmap_gravity",
    "heatmap_closest_average",
    "heatmap_connectivity",
    "heatmap_2sfca",
    "huff_model",
    "travel_cost_matrix",
)

BUNDLE_SELECTORS = ("pt_network_bundle_id", "street_network_bundle_id")


def _ui(tool_name: str) -> dict[str, dict]:
    definition = next(d for d in TOOL_REGISTRY if d.name == tool_name)
    properties = definition.get_params_class().model_json_schema()["properties"]
    return {name: prop["x-ui"] for name, prop in properties.items() if "x-ui" in prop}


def _order(ui: dict[str, dict], name: str) -> int:
    return ui[name]["field_order"]


@pytest.mark.parametrize("tool_name", ROUTING_TOOLS)
@pytest.mark.parametrize("selector", BUNDLE_SELECTORS)
def test_the_selector_has_its_own_section(tool_name: str, selector: str) -> None:
    """Its own section, not the one the rest of the settings live in."""
    ui = _ui(tool_name)
    expected = "pt_network" if selector.startswith("pt_") else "street_network"
    assert ui[selector]["section"] == expected
    assert ui[selector]["section"] != ui["pt_max_transfers"]["section"]


@pytest.mark.parametrize("tool_name", ROUTING_TOOLS)
@pytest.mark.parametrize("selector", BUNDLE_SELECTORS)
def test_the_selector_comes_before_the_legs(tool_name: str, selector: str) -> None:
    """The first field of the access-leg group opens it; anything ordered
    after that field renders under the group's heading."""
    ui = _ui(tool_name)
    legs = [
        _order(ui, name)
        for name, meta in ui.items()
        if meta.get("group_label") in ("groups.access_leg", "groups.egress_leg")
    ]
    assert legs, f"{tool_name} has no leg groups"
    assert _order(ui, selector) < min(legs)
