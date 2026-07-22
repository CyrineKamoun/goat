"""Catalog metadata service — builds and edits record_jsonb on Layer objects.

Record-first: while a record exists it owns the descriptive metadata (the flat
columns stay NULL); publish materializes it, native withdrawal dissolves it.
"""

from __future__ import annotations

import copy
import re
from typing import TYPE_CHECKING, Any, Dict, List

if TYPE_CHECKING:
    from core.db.models.layer import Layer


def layer_to_record_jsonb(layer: "Layer") -> Dict[str, Any]:
    """Build a record from the flat columns — the native publish builder.

    Called when a user adds their own layer to the catalog
    (crud_layer._publish_to_catalog) and by the record-first migration.
    Harvested layers never come through here: the pipeline materializes
    their records via dcat_to_record().
    """
    geometry, bbox = _geometry_from_extent(str(layer.extent) if layer.extent else None)

    links: list[Dict[str, Any]] = []
    if layer.distribution_url:
        links.append(
            {
                "rel": "enclosure",
                "type": "application/octet-stream",
                "title": "Download",
                "href": str(layer.distribution_url),
            }
        )

    contacts: list[Dict[str, Any]] = []
    if layer.distributor_name:
        _pub: Dict[str, Any] = {"name": layer.distributor_name, "roles": ["publisher"]}
        if layer.distributor_email:
            _pub["emails"] = [{"value": str(layer.distributor_email)}]
        contacts.append(_pub)

    return {
        "id": str(layer.id),
        "type": "Feature",
        "geometry": geometry,
        "time": {"interval": [[f"{layer.data_reference_year}-01-01", None]]}
        if layer.data_reference_year
        else None,
        "links": links,
        "properties": {
            "type": "dataset",
            "title": layer.name or "",
            "description": layer.description or "",
            "keywords": list(layer.tags or []),
            "themes": [{"concepts": [{"id": layer.data_category.value}]}]
            if layer.data_category
            else [],
            "language": {"code": layer.language_code} if layer.language_code else None,
            "created": layer.created_at.isoformat() if layer.created_at else None,
            "updated": layer.updated_at.isoformat() if layer.updated_at else None,
            "contacts": contacts,
            "license": layer.license.value if layer.license else None,
            "rights": None,
            "externalIds": [],
        },
    }


def _geometry_from_extent(
    extent_wkt: str | None,
) -> tuple[Dict[str, Any] | None, list | None]:
    """Parse a WKT extent string into (GeoJSON geometry, bbox list) or (None, None)."""
    if not extent_wkt:
        return None, None
    nums = re.findall(r"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?", extent_wkt)
    floats = list(map(float, nums))
    if len(floats) < 8:
        return None, None
    xs = floats[0::2]
    ys = floats[1::2]
    w, e = min(xs), max(xs)
    s, n = min(ys), max(ys)
    coords = [[[w, s], [e, s], [e, n], [w, n], [w, s]]]
    return {"type": "Polygon", "coordinates": coords}, [[w, s, e, n]]


# Maps edit-form layer fields to their position in the record (root-relative;
# recordGeoJSON: descriptive fields under properties, time/links top-level).
FIELD_TO_JSONB_PATH: Dict[str, List[Any]] = {
    "name": ["properties", "title"],
    "description": ["properties", "description"],
    "license": ["properties", "license"],
    "language_code": ["properties", "language", "code"],
    "distributor_name": ["properties", "contacts", 0, "name"],
    "distributor_email": ["properties", "contacts", 0, "emails", 0, "value"],
    "data_reference_year": ["time", "interval", 0, 0],
    "tags": ["properties", "keywords"],
    "geographical_code": ["properties", "goat:geographical_code"],
}


def _get_nested(d: Any, path: List[Any]) -> Any:
    """Walk a nested dict/list path. Returns None if any segment is missing."""
    cur = d
    for key in path:
        if cur is None:
            return None
        if isinstance(key, int):
            if not isinstance(cur, list) or key >= len(cur):
                return None
            cur = cur[key]
        else:
            if not isinstance(cur, dict):
                return None
            cur = cur.get(key)
    return cur


def _set_nested(d: Dict[str, Any], path: List[Any], value: Any) -> None:
    """Set a nested value, creating intermediate dicts/lists as needed."""
    cur: Any = d
    for i, key in enumerate(path[:-1]):
        next_key = path[i + 1]
        if isinstance(key, int):
            while len(cur) <= key:
                cur.append({} if not isinstance(next_key, int) else [])
            if cur[key] is None:
                cur[key] = {} if not isinstance(next_key, int) else []
            cur = cur[key]
        else:
            if key not in cur or cur[key] is None:
                cur[key] = {} if not isinstance(next_key, int) else []
            cur = cur[key]
    last = path[-1]
    if isinstance(last, int):
        while len(cur) <= last:
            cur.append(None)
        cur[last] = value
    else:
        cur[last] = value


def _remove_nested(d: Dict[str, Any], path: List[Any]) -> bool:
    """Remove a nested key. Returns True if something was removed."""
    parent = _get_nested(d, path[:-1]) if len(path) > 1 else d
    last = path[-1]
    if isinstance(last, int):
        if isinstance(parent, list) and 0 <= last < len(parent):
            parent[last] = None
            return True
    else:
        if isinstance(parent, dict) and last in parent:
            parent.pop(last)
            return True
    return False


def apply_user_edits(
    record_jsonb: Dict[str, Any] | None,
    layer_in: Dict[str, Any],
) -> tuple[Dict[str, Any], bool]:
    """Write edited flat fields directly into a copy of ``record_jsonb``.

    Record-first: core owns ``customer.layer.record_jsonb`` and edits it in
    place — no contribution overlay, no datacatalog. On re-harvest the harvester
    is responsible for not clobbering these edits. Returns (updated_record,
    changed). An empty submitted value removes the field (revert to source).
    """
    record = copy.deepcopy(record_jsonb) if record_jsonb else {}
    props = record.setdefault("properties", {})
    changed = False

    for src_field, path in FIELD_TO_JSONB_PATH.items():
        if src_field not in layer_in:
            continue
        new_val = layer_in[src_field]
        if src_field == "data_reference_year" and new_val not in (None, ""):
            new_val = f"{int(new_val)}-01-01"
        current_val = _get_nested(record, path)
        if new_val in (None, ""):
            if _remove_nested(record, path):
                changed = True
        elif new_val != current_val:
            _set_nested(record, path, new_val)
            changed = True

    if "distribution_url" in layer_in:
        new_url = layer_in["distribution_url"]
        cur_links = record.get("links") or []
        cur_enc = next(
            (lk.get("href") for lk in cur_links if lk.get("rel") == "enclosure"), None
        )
        keep = [lk for lk in cur_links if lk.get("rel") != "enclosure"]
        if new_url in (None, ""):
            if len(keep) != len(cur_links):
                record["links"] = keep
                changed = True
        elif str(new_url) != cur_enc:
            record["links"] = keep + [
                {
                    "rel": "enclosure",
                    "type": "application/octet-stream",
                    "title": "Download",
                    "href": str(new_url),
                }
            ]
            changed = True

    if "data_category" in layer_in:
        new_cat = layer_in["data_category"]
        cur_themes = props.get("themes") or []
        cur_cat = None
        if cur_themes and isinstance(cur_themes[0], dict):
            concepts = cur_themes[0].get("concepts") or []
            if concepts and isinstance(concepts[0], dict):
                cur_cat = concepts[0].get("id")
        if new_cat in (None, ""):
            if props.pop("themes", None) is not None:
                changed = True
        elif new_cat != cur_cat:
            props["themes"] = [
                {
                    "concepts": [{"id": new_cat}],
                    "scheme": "https://goat.plan4better.de/data-categories",
                }
            ]
            changed = True

    return record, changed
