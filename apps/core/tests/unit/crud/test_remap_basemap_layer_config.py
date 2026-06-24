from core.crud.crud_project_copy import _remap_basemap_layer_config


def test_remaps_known_target_ids_and_falls_back_for_unknown() -> None:
    custom_basemaps = [
        {
            "type": "vector",
            "id": "11111111-1111-1111-1111-111111111111",
            "url": "https://example.com/style.json",
            "layer_config": {
                "road_label": {"visible": True, "relation": "above", "target": "10"},
                "water": {"visible": True, "relation": "below", "target": "all"},
                "poi": {"visible": True, "relation": "above", "target": "99"},
            },
        }
    ]
    result = _remap_basemap_layer_config(custom_basemaps, {10: 200})
    cfg = result[0]["layer_config"]
    assert cfg["road_label"]["target"] == "200"  # remapped
    assert cfg["water"]["target"] == "all"        # untouched
    assert cfg["poi"]["target"] == "all"          # unmapped → fallback


def test_ignores_basemaps_without_layer_config() -> None:
    custom_basemaps = [
        {"type": "solid", "id": "x", "color": "#abcdef"},
        {"type": "vector", "id": "y", "url": "https://e.com/s.json"},
    ]
    result = _remap_basemap_layer_config(custom_basemaps, {1: 2})
    assert result == custom_basemaps
