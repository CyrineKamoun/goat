"""Catalog metadata service — manages record_jsonb on Layer objects.

For catalog layers: record_jsonb is the single source of truth (OGC-compliant).
User edits flow into datacatalog.record_overrides with source='user' (priority 100)
so they are preserved when the pipeline re-harvests. merge_record_overrides rebuilds
layer.record_jsonb by applying harvest < ai_* < user in priority order.

For user-uploaded layers: record_jsonb is built from flat columns on create,
then updated incrementally on edit.
"""

from __future__ import annotations

import copy
import json
import re
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Dict, List, Union
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from core.core.config import settings

if TYPE_CHECKING:
    from core.db.models.layer import Layer


def update_record_jsonb(layer: "Layer") -> Dict[str, Any]:
    """Update record_jsonb by merging user-editable flat columns into
    the existing record_jsonb. Preserves harvested fields the user didn't change.

    Called on every layer update (PUT /api/v2/layer/{id}).
    """
    existing = layer.record_jsonb
    if not existing or not isinstance(existing, dict):
        # No existing record_jsonb — build from scratch (user-uploaded layer)
        return layer_to_record_jsonb(layer)

    record = copy.deepcopy(existing)
    props = record.setdefault("properties", {})

    # Merge user-editable fields — only overwrite if the flat column has a value
    if layer.name:
        props["title"] = layer.name
    if layer.description is not None:
        props["description"] = layer.description or ""
    if layer.language_code:
        props["language"] = layer.language_code
    if layer.data_category:
        themes = [{"concepts": [{"id": layer.data_category.value}]}]
        props["themes"] = themes
    if layer.distributor_name:
        pub = props.get("publisher") or {}
        pub["name"] = layer.distributor_name
        if layer.distributor_email:
            pub["email"] = str(layer.distributor_email)
        props["publisher"] = pub
    if layer.license:
        props["license"] = layer.license.value
    if layer.data_reference_year is not None:
        extent = props.setdefault("extent", {})
        extent["temporal"] = {"interval": [[layer.data_reference_year, None]]}
    if layer.tags:
        props["keywords"] = [{"value": t} for t in layer.tags]
    if layer.distribution_url:
        # Update or add enclosure link
        links = [lk for lk in (props.get("links") or []) if lk.get("rel") != "enclosure"]
        links.append({
            "rel": "enclosure",
            "type": "application/octet-stream",
            "title": "Download",
            "href": str(layer.distribution_url),
        })
        props["links"] = links

    # Update extent geometry from actual data extent
    geometry, bbox_list = _geometry_from_extent(str(layer.extent) if layer.extent else None)
    if geometry:
        record["geometry"] = geometry
        extent = props.setdefault("extent", {})
        extent["spatial"] = {"bbox": bbox_list}

    # Update timestamp
    props["updated"] = datetime.now(timezone.utc).isoformat()
    props["goat_layer_id"] = str(layer.id)

    return record


def layer_to_record_jsonb(layer: "Layer") -> Dict[str, Any]:
    """Build record_jsonb from flat columns only (for new/user-uploaded layers).

    For catalog layers, the pipeline writes richer metadata via
    dcat_to_record() or iso19139_to_record(). This function is only
    the fallback for layers without existing record_jsonb.
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

    return {
        "id": str(layer.id),
        "type": "Feature",
        "geometry": geometry,
        "properties": {
            "type": "dataset",
            "title": layer.name or "",
            "description": layer.description or "",
            "keywords": [{"value": t} for t in (layer.tags or [])],
            "themes": [{"concepts": [{"id": layer.data_category.value}]}]
            if layer.data_category
            else [],
            "language": layer.language_code,
            "created": layer.created_at.isoformat() if layer.created_at else None,
            "updated": layer.updated_at.isoformat() if layer.updated_at else None,
            "publisher": {
                "name": layer.distributor_name,
                "email": str(layer.distributor_email) if layer.distributor_email else None,
            }
            if layer.distributor_name
            else None,
            "license": layer.license.value if layer.license else None,
            "extent": {
                "spatial": {"bbox": bbox} if bbox else None,
                "temporal": {"interval": [[layer.data_reference_year, None]]}
                if layer.data_reference_year
                else None,
            },
            "links": links,
            "source_format": "layer_model",
            "goat_layer_id": str(layer.id),
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


# ---------------------------------------------------------------------------
# record_overrides: user edit overlay + merge (OGC-compliant catalog flow)
# ---------------------------------------------------------------------------

# Maps edit-form layer fields to their position in record_jsonb.properties
FIELD_TO_JSONB_PATH: Dict[str, List[Any]] = {
    "name":                ["title"],
    "description":         ["description"],
    "license":             ["license"],
    "language_code":       ["language"],
    "distributor_name":    ["publisher", "name"],
    "distributor_email":   ["publisher", "email"],
    "distribution_url":    ["distribution_url"],
    "data_reference_year": ["extent", "temporal", "interval", 0, 0],
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


def _deep_merge(base: Dict[str, Any], overlay: Dict[str, Any]) -> Dict[str, Any]:
    """Recursively merge overlay into base. Overlay leaf values win; None is skipped."""
    result = dict(base)
    for key, val in overlay.items():
        if val is None:
            continue
        if key in result and isinstance(result[key], dict) and isinstance(val, dict):
            result[key] = _deep_merge(result[key], val)
        else:
            result[key] = val
    return result


async def get_record_override(
    session: AsyncSession, layer_id: Union[UUID, str], source: str
) -> Dict[str, Any] | None:
    """Fetch the overrides_jsonb for a (layer_id, source) pair, or None if absent."""
    res = await session.execute(
        text(
            f"SELECT overrides_jsonb FROM {settings.CATALOG_SCHEMA}.record_overrides"
            f" WHERE layer_id = :lid AND source = :src LIMIT 1"
        ),
        {"lid": str(layer_id), "src": source},
    )
    row = res.fetchone()
    return row.overrides_jsonb if row else None


async def upsert_record_override(
    session: AsyncSession,
    layer_id: Union[UUID, str],
    source: str,
    priority: int,
    overrides_jsonb: Dict[str, Any],
) -> None:
    """Write or update a record_overrides row."""
    await session.execute(
        text(
            f"""
            INSERT INTO {settings.CATALOG_SCHEMA}.record_overrides
                (layer_id, source, priority, overrides_jsonb, updated_at)
            VALUES (:lid, :src, :pri, CAST(:ov AS jsonb), now())
            ON CONFLICT (layer_id, source) DO UPDATE SET
                overrides_jsonb = EXCLUDED.overrides_jsonb,
                priority = EXCLUDED.priority,
                updated_at = now()
            """
        ),
        {
            "lid": str(layer_id),
            "src": source,
            "pri": priority,
            "ov": json.dumps(overrides_jsonb),
        },
    )


async def merge_record_overrides(
    session: AsyncSession, layer_id: Union[UUID, str]
) -> Dict[str, Any] | None:
    """Rebuild merged record_jsonb from all overrides, in priority order (lowest first)."""
    res = await session.execute(
        text(
            f"SELECT source, priority, overrides_jsonb"
            f" FROM {settings.CATALOG_SCHEMA}.record_overrides"
            f" WHERE layer_id = :lid"
            f" ORDER BY priority ASC, updated_at ASC"
        ),
        {"lid": str(layer_id)},
    )
    merged: Dict[str, Any] = {}
    for row in res.fetchall():
        overlay = row.overrides_jsonb
        if isinstance(overlay, dict):
            merged = _deep_merge(merged, overlay)
    return merged or None


def compute_user_override_diff(
    current_record_jsonb: Dict[str, Any] | None,
    existing_user_override: Dict[str, Any] | None,
    layer_in: Dict[str, Any],
) -> tuple[Dict[str, Any], bool]:
    """Compute the updated user override based on what changed in layer_in.

    For each editable field in layer_in:
    - If submitted value differs from current effective value → set in override
    - If submitted value is empty → remove from override (revert to harvest/AI)
    - If unchanged → do nothing

    Returns (updated_user_override, changed: bool).
    """
    current_props = ((current_record_jsonb or {}).get("properties")) or {}
    user_override = copy.deepcopy(existing_user_override) if existing_user_override else {"properties": {}}
    user_props = user_override.setdefault("properties", {})
    changed = False

    for src_field, path in FIELD_TO_JSONB_PATH.items():
        if src_field not in layer_in:
            continue
        new_val = layer_in[src_field]
        current_val = _get_nested(current_props, path)

        if new_val in (None, ""):
            if _remove_nested(user_props, path):
                changed = True
        elif new_val != current_val:
            _set_nested(user_props, path, new_val)
            changed = True

    # Special case: data_category → themes[0].concepts[0].id
    if "data_category" in layer_in:
        new_cat = layer_in["data_category"]
        current_themes = current_props.get("themes") or []
        current_cat = None
        if current_themes and isinstance(current_themes[0], dict):
            concepts = current_themes[0].get("concepts") or []
            if concepts and isinstance(concepts[0], dict):
                current_cat = concepts[0].get("id")

        if new_cat in (None, ""):
            if "themes" in user_props:
                user_props.pop("themes")
                changed = True
        elif new_cat != current_cat:
            user_props["themes"] = [{
                "concepts": [{"id": new_cat}],
                "scheme": "https://goat.plan4better.de/data-categories",
            }]
            changed = True

    return user_override, changed
