"""Unit tests for the record-first catalog write logic (core.services.catalog).

Pure functions, no DB: apply_user_edits (in-place record editing) is the path a
UI metadata edit takes now that core owns customer.layer.record_jsonb directly.
"""

from core.services.catalog import apply_user_edits


def _record() -> dict:
    return {
        "id": "abc",
        "type": "Feature",
        "geometry": None,
        "time": {"interval": [["2019-01-01", None]]},
        "links": [
            {"rel": "via", "href": "http://src/dataset/x"},
            {"rel": "enclosure", "href": "http://old-download"},
        ],
        "properties": {
            "type": "dataset",
            "title": "Old",
            "description": "old desc",
            "license": "CC_BY",
            "language": {"code": "de"},
            "contacts": [{"name": "Org", "roles": ["publisher"]}],
            "keywords": ["a"],
            "themes": [{"concepts": [{"id": "other"}]}],
        },
    }


def test_apply_sets_scalar_fields() -> None:
    rec, changed = apply_user_edits(_record(), {"name": "New", "license": "CC_BY_SA"})
    assert changed is True
    assert rec["properties"]["title"] == "New"
    assert rec["properties"]["license"] == "CC_BY_SA"


def test_apply_year_int_to_rfc3339() -> None:
    rec, _ = apply_user_edits(_record(), {"data_reference_year": 2021})
    assert rec["time"]["interval"][0][0] == "2021-01-01"


def test_apply_publisher_and_email_into_contacts() -> None:
    rec, _ = apply_user_edits(
        _record(), {"distributor_name": "New Org", "distributor_email": "x@y.z"}
    )
    assert rec["properties"]["contacts"][0]["name"] == "New Org"
    assert rec["properties"]["contacts"][0]["emails"][0]["value"] == "x@y.z"


def test_apply_data_category_to_themes() -> None:
    rec, _ = apply_user_edits(_record(), {"data_category": "transportation"})
    assert rec["properties"]["themes"][0]["concepts"][0]["id"] == "transportation"


def test_apply_distribution_url_swaps_enclosure_keeps_via() -> None:
    rec, _ = apply_user_edits(_record(), {"distribution_url": "http://new-download"})
    encs = [lk["href"] for lk in rec["links"] if lk["rel"] == "enclosure"]
    vias = [lk["href"] for lk in rec["links"] if lk["rel"] == "via"]
    assert encs == ["http://new-download"]
    assert vias == ["http://src/dataset/x"]  # source link preserved


def test_apply_empty_value_removes_field() -> None:
    rec, changed = apply_user_edits(_record(), {"description": ""})
    assert changed is True
    assert "description" not in rec["properties"]


def test_apply_no_change_returns_false() -> None:
    rec, changed = apply_user_edits(_record(), {"name": "Old", "license": "CC_BY"})
    assert changed is False


def test_apply_does_not_mutate_input() -> None:
    original = _record()
    apply_user_edits(original, {"name": "Mutated"})
    assert original["properties"]["title"] == "Old"


def test_apply_keywords_list_replaced() -> None:
    rec, _ = apply_user_edits(_record(), {"tags": ["x", "y"]})
    assert rec["properties"]["keywords"] == ["x", "y"]
