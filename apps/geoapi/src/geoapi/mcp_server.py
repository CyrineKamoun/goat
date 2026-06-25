"""MCP server exposing the GOAT data catalog as tools for LLM clients.

Mounted into the geoapi FastAPI app at ``/mcp`` (Streamable HTTP). Tools call the
shared ``catalog_search`` query functions in-process via geoapi's asyncpg pool.
"""

from __future__ import annotations

import asyncio
import json
import os
from typing import Any, Dict, List, Optional

from mcp.server.fastmcp import FastMCP

from geoapi.dependencies import get_layer_info
from geoapi.services import catalog_search
from geoapi.services.feature_service import feature_service
from geoapi.services.layer_service import layer_service

_MAX_GEOJSON_FEATURES = 5000
# Stay well under the MCP 1 MB tool-result cap. The client may count the result
# twice (structured + text echo), so budget ~450 KB → under 1 MB even when doubled.
_MAX_GEOJSON_BYTES = 450_000
# Coordinate decimals to keep (~1 m precision) — shrinks line/polygon payloads a lot.
_COORD_DECIMALS = 5


def _round_coords(node: Any) -> Any:
    if isinstance(node, float):
        return round(node, _COORD_DECIMALS)
    if isinstance(node, list):
        return [_round_coords(x) for x in node]
    return node


mcp = FastMCP("goat-catalog", stateless_http=True, json_response=True)
mcp.settings.streamable_http_path = "/"

# Relative base for the links embedded in records. The MCP server has no request
# context; clients use the returned ids with get_catalog_record, not these links.
_BASE_URL = "/catalog/records/collections/datasets/items"

# Public GOAT frontend base, used to build a clickable dataset link per result.
# The /datasets/<id> route resolves both dataset (group) ids and layer ids.
_WEB_BASE = os.environ.get("GOAT_WEB_URL", "http://localhost:3000").rstrip("/")


def _dataset_link(item_id: Any) -> Optional[str]:
    return f"{_WEB_BASE}/datasets/{item_id}" if item_id else None


def _pool() -> Any:
    pool = layer_service._pool
    if pool is None:
        raise RuntimeError("Metadata database unavailable")
    return pool


def _trim_feature(feature: Dict[str, Any]) -> Dict[str, Any]:
    """Reduce a full OGC record to the fields useful for an LLM, to save tokens."""
    props = feature.get("properties") or {}
    extent = props.get("extent") or {}
    spatial = extent.get("spatial") or {}
    bbox_list = spatial.get("bbox")
    bbox = bbox_list[0] if isinstance(bbox_list, list) and bbox_list else None
    publisher = props.get("publisher")
    publisher_name = publisher.get("name") if isinstance(publisher, dict) else publisher
    return {
        "id": feature.get("id"),
        "link": _dataset_link(feature.get("id")),
        "title": props.get("title"),
        "description": props.get("description"),
        "type": props.get("type"),
        "publisher": publisher_name,
        "license": props.get("license"),
        "keywords": props.get("keywords"),
        "themes": props.get("themes"),
        "language": props.get("language"),
        "updated": props.get("updated"),
        "bbox": bbox,
        "distribution_count": len(props.get("distributions") or []),
    }


def _geojson_bbox(geometry: Optional[Dict[str, Any]]) -> Optional[str]:
    """Compute a 'west,south,east,north' bbox string from a GeoJSON geometry."""
    if not geometry:
        return None
    coords = geometry.get("coordinates")
    if coords is None:
        return None

    xs: List[float] = []
    ys: List[float] = []

    def walk(node: Any) -> None:
        if (
            isinstance(node, (list, tuple))
            and len(node) == 2
            and all(isinstance(c, (int, float)) for c in node)
        ):
            xs.append(float(node[0]))
            ys.append(float(node[1]))
        elif isinstance(node, (list, tuple)):
            for child in node:
                walk(child)

    walk(coords)
    if not xs or not ys:
        return None
    return f"{min(xs)},{min(ys)},{max(xs)},{max(ys)}"


@mcp.tool()
async def search_catalog(
    q: Optional[str] = None,
    bbox: Optional[str] = None,
    bbox_boost: Optional[str] = None,
    themes: Optional[str] = None,
    language: Optional[str] = None,
    year: Optional[int] = None,
    source_format: Optional[str] = None,
    license: Optional[str] = None,
    publisher: Optional[str] = None,
    type: Optional[str] = None,
    geographical_code: Optional[str] = None,
    datetime: Optional[str] = None,
    sortby: Optional[str] = None,
    limit: int = 10,
    offset: int = 0,
) -> Dict[str, Any]:
    """Search the GOAT data catalog and return matching datasets.

    Datasets are grouped (one record per dataset, multiple distributions/layers).
    Results are trimmed; use get_catalog_record(id) for the full record.

    Args:
        q: Free-text search across title, description, and keywords.
        bbox: Spatial hard filter 'west,south,east,north' in WGS84 lon/lat (e.g. '9.5,47.2,13.8,55.1').
        bbox_boost: 'west,south,east,north' for spatial ranking only (matches ranked first, nothing excluded).
        themes: Comma-separated data categories: transportation, landuse, environment, places, people, imagery, boundary, basemap, other.
        language: ISO 639-1 code, e.g. 'de' or 'en'.
        year: Data reference year, e.g. 2023.
        source_format: One of iso19139, dcat, synthetic, layer_model.
        license: Comma-separated licenses, e.g. 'CC_BY,CC_BY_SA'.
        publisher: Comma-separated publisher/distributor names.
        type: Comma-separated layer types: feature, raster, table.
        geographical_code: Comma-separated ISO 3166-1 alpha-2 codes, e.g. 'DE,AT'.
        datetime: OGC temporal filter, e.g. '2023-01-01' or '2023-01-01/2024-12-31' or '../2024-12-31'.
        sortby: Sort by title, updated, created, or type; prefix '-' for descending (e.g. '-updated').
        limit: Max datasets to return (1-100, default 10).
        offset: Number of datasets to skip for pagination.
    """
    limit = max(1, min(limit, 100))
    result = await catalog_search.search_records(
        _pool(),
        base_url=_BASE_URL,
        q=q,
        bbox=bbox,
        bbox_boost=bbox_boost,
        themes=themes,
        language=language,
        year=year,
        source_format=source_format,
        license=license,
        publisher=publisher,
        type=type,
        geographical_code=geographical_code,
        datetime=datetime,
        sortby=sortby,
        limit=limit,
        offset=offset,
    )
    return {
        "numberMatched": result.get("numberMatched", 0),
        "numberReturned": result.get("numberReturned", 0),
        "results": [_trim_feature(f) for f in result.get("features", [])],
    }


@mcp.tool()
async def get_catalog_record(item_id: str) -> Dict[str, Any]:
    """Return the full catalog record for a dataset id (from search_catalog results).

    Includes all distributions (individual layers/files) and complete metadata.
    """
    record = await catalog_search.get_record(_pool(), item_id, base_url=_BASE_URL)
    if record is None:
        raise ValueError(f"Catalog record '{item_id}' not found")
    record["link"] = _dataset_link(record.get("id"))
    return record


@mcp.tool()
async def search_nuts_regions(
    q: str, level: Optional[int] = None, limit: int = 20
) -> List[Dict[str, Any]]:
    """Resolve a place name or NUTS code to NUTS regions (id, name, level, country, bbox).

    Use the returned bbox with search_catalog(bbox=...) to find datasets covering a region.

    Args:
        q: Region name or NUTS code prefix, e.g. 'Bavaria' or 'DE2'.
        level: Optional NUTS level filter (0=country ... 3=district).
        limit: Max regions to return (1-100, default 20).
    """
    limit = max(1, min(limit, 100))
    return await catalog_search.search_nuts(_pool(), q, level, limit)


@mcp.tool()
async def get_layer_geojson(
    layer_id: str,
    bbox: Optional[str] = None,
    limit: int = 1000,
) -> str:
    """Return a layer's features as a GeoJSON FeatureCollection (JSON string) for inline maps.

    Use the `layer_id` from a dataset's distributions (get_catalog_record), NOT the dataset id.
    Parse the returned JSON and embed it directly into a MapLibre GL JS artifact as a `geojson`
    source — do NOT use tile/style URLs (the artifact sandbox blocks external tiles → blank map).
    ALWAYS clip to the study area with `bbox` and keep `limit` modest. The result is hard-capped
    in size and `truncated` is set true when features were dropped; if so, use a smaller bbox.

    Args:
        layer_id: The layer UUID (a distribution's layer_id, not the dataset id).
        bbox: Optional 'west,south,east,north' (WGS84) to clip features to the study area.
        limit: Max features to fetch (capped at 5000; the byte cap may keep fewer).
    """
    limit = max(1, min(limit, _MAX_GEOJSON_FEATURES))
    bbox_list: Optional[List[float]] = None
    if bbox:
        parts = bbox.split(",")
        if len(parts) == 4:
            try:
                bbox_list = [float(p) for p in parts]
            except ValueError:
                bbox_list = None

    try:
        layer_info = await get_layer_info(layer_id)
    except Exception as exc:
        raise ValueError(f"Layer '{layer_id}' has no servable data: {exc}")

    metadata = await layer_service.get_layer_metadata(layer_info)
    if metadata is None:
        raise ValueError(f"Layer '{layer_id}' not found")

    features, total = await asyncio.to_thread(
        feature_service.get_features,
        layer_info=layer_info,
        limit=limit,
        offset=0,
        bbox=bbox_list,
        column_names=metadata.column_names,
        geometry_column=metadata.geometry_column or "geometry",
        has_geometry=metadata.has_geometry,
        native_column_types=metadata.native_column_types,
    )

    # Round coordinates and cap by serialized size to stay under the MCP 1 MB
    # tool-result limit — line/polygon layers blow past it otherwise.
    kept: List[Dict[str, Any]] = []
    size = 256
    for feat in features:
        geom = feat.get("geometry")
        if isinstance(geom, dict) and isinstance(geom.get("coordinates"), list):
            feat = {
                **feat,
                "geometry": {**geom, "coordinates": _round_coords(geom["coordinates"])},
            }
        feat_bytes = len(json.dumps(feat, separators=(",", ":"), default=str)) + 1
        if kept and size + feat_bytes > _MAX_GEOJSON_BYTES:
            break
        kept.append(feat)
        size += feat_bytes

    collection = {
        "type": "FeatureCollection",
        "numberReturned": len(kept),
        "numberMatched": total,
        "truncated": total > len(kept) or len(kept) < len(features),
        "features": kept,
    }
    # Return a compact JSON string (not a dict): a dict result is echoed by
    # FastMCP as both structured + text content, doubling the payload.
    return json.dumps(collection, separators=(",", ":"), default=str)


@mcp.tool()
async def get_nuts_geometry(nuts_id: str) -> Dict[str, Any]:
    """Return a NUTS region boundary as a GeoJSON Feature plus a 'bbox' string.

    The 'bbox' ('west,south,east,north') can be passed straight to search_catalog(bbox=...).
    """
    feature = await catalog_search.get_nuts_geometry(_pool(), nuts_id)
    if feature is None:
        raise ValueError(f"NUTS region '{nuts_id}' not found")
    feature["bbox"] = _geojson_bbox(feature.get("geometry"))
    return feature
