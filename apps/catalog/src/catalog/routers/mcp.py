"""MCP server exposing the GOAT catalog as tools for LLM clients.

Mounted into the catalog FastAPI app at ``/mcp`` (Streamable HTTP) --
see ``catalog.app.create_app``, which imports this module lazily and
guards the import so a broken/missing ``mcp`` install never takes down
the HTTP API (see that module's docstring for the guard). ``create_app``
also wraps the mount in ``catalog.auth.BearerAuthASGIMiddleware`` (the same
bearer-token gate that ``/stac``/``/nuts`` deliberately no longer apply
(they are public reads; ``/mcp`` is not), api spec
§1 -- a raw ASGI ``Mount`` can't be reached by FastAPI's ``Depends``, so
that gate is reimplemented as plain ASGI middleware instead) and pairs the
built sub-app with its own ``StreamableHTTPSessionManager`` on ``app.state``
at mount time -- see that module's docstring for why a lazy re-read of the
module-level ``mcp`` singleton's session manager would be wrong.

Ported from ``cyrine/catalog-harvester:apps/geoapi/src/geoapi/mcp_server.py``.
That reference exposed five tools against geoapi's PostgreSQL-backed
``catalog_search`` service: ``search_catalog``, ``get_catalog_record``,
``search_nuts_regions``, ``get_layer_geojson``, ``get_nuts_geometry``.

Kept here (re-backed by this service's own modules -- ``catalog.services
.search``, ``catalog.services.aggregations``, ``catalog.services
.stac_build`` -- instead of geoapi's asyncpg pool):

- ``search_catalog`` -- Collection Search (dataset-level) + facet counts.
- ``get_catalog_record`` -- full record for an id, item or collection/bundle.

**Dropped**: ``get_layer_geojson``, the feature-preview tool that fetched a
layer's actual geometries via geoapi's ``feature_service``/``layer_service``
(DuckLake-backed). This service has no DuckLake connection at all -- it is a
DB-less mirror of the published catalog (see ``catalog.store``) -- so serving
individual feature geometries is out of scope here. ``search_nuts_regions``
and ``get_nuts_geometry`` are also not ported in this pass: the brief for
this port scopes the kept tools to the catalog-search/get-item surface
backed by ``catalog.services.search``/``aggregations``/``stac_build``; NUTS
lookup (``catalog.routers.nuts``, Task 10) is a separate HTTP-only surface
for now and can be added as MCP tools in a follow-up if useful.

Wiring / store access
----------------------
The reference geoapi ``mcp_server`` reached its data through a *module-level*
asyncpg pool (``layer_service._pool``) rather than through FastAPI's request-
scoped DI, because the Streamable HTTP transport is a separately-mounted
Starlette ASGI app (``mcp.streamable_http_app()``): a tool call's request
scope belongs to that sub-app, not to the outer FastAPI app, so there is no
``Request`` object here to depend-inject ``catalog.deps.get_store`` from.

This module follows the same shape: a module-level ``_store`` accessor, set
once by ``catalog.app``'s lifespan (via ``set_store``) right after it builds
the app's ``CatalogStore``, and read by every tool call via
``_require_store()``. This mirrors ``CatalogStore`` itself (already a
per-process singleton hung off ``app.state``) and is fine for the single
running app per process this service is deployed as; it is *not* safe for
two ``CatalogStore``s to be live concurrently in the same interpreter (e.g.
two ``TestClient``s open at once against apps built by two concurrent
``create_app()`` calls) -- tests must not overlap two open MCP-enabled
clients.

Result-size caps
-----------------
``_MAX_RESULT_ITEMS``/``_MAX_RESULT_BYTES`` mirror the reference's
``_MAX_GEOJSON_FEATURES``/``_MAX_GEOJSON_BYTES`` (5000 features / ~450 KB),
sized there to stay under the MCP transport's ~1 MB tool-result limit even
if a client double-counts (structured + text echo). The reference only
needed this for the (dropped) per-feature GeoJSON tool; kept here as a
general safety net applied to every list-shaped tool result (``search_catalog``
results, a bundle's member items in ``get_catalog_record``), since a facet-
heavy or member-heavy catalog could in principle still produce a large body.

Error handling
--------------
Unlike the reference (which ``raise``s ``ValueError`` for an unknown id, relying
on the MCP dispatch layer -- ``MCPServer._handle_call_tool`` -- to catch it and
convert it to an error ``CallToolResult``), these tool functions return a
``{"error": "..."}`` dict on a lookup failure instead of raising. They are
plain, directly-callable coroutines (``@mcp.tool()`` returns the undecorated
function -- see ``MCPServer.tool``'s implementation), and this project's unit
tests call them directly rather than through the MCP protocol, so a clean
error payload is preferable to an exception that only the protocol layer
would otherwise catch.
"""

from __future__ import annotations

import json
import re
from typing import Any

from mcp.server.mcpserver import MCPServer
from mcp.server.transport_security import TransportSecuritySettings

from catalog.errors import ApiError
from catalog.routers.params import SearchQuery
from catalog.services.aggregations import (
    aggregation_names,
    facet_params,
    run_aggregations,
)
from catalog.services.search import (
    SearchParams,
    resolve_id,
    safe_query,
    search_collections,
)
from catalog.services.stac_build import (
    collection_from_row,
    collection_to_stac,
    item_from_row,
    record_to_item,
)
from catalog.store import CatalogStore

_MAX_RESULT_ITEMS = 5000
_MAX_RESULT_BYTES = 450_000


def build_transport_security(allowed_hosts: list[str]) -> TransportSecuritySettings:
    """Build the Streamable HTTP transport's Host/Origin-validation settings
    from ``CatalogSettings.mcp_allowed_hosts`` -- see ``catalog.app``'s mount
    call, which passes this as ``streamable_http_app(transport_security=...)``.

    ``streamable_http_app()``'s own default (``host="127.0.0.1"``)
    auto-enables DNS-rebinding protection scoped to
    ``127.0.0.1``/``localhost``/``::1`` only, which 421s every request not
    literally addressed to localhost -- i.e. every real request once this is
    deployed behind an actual hostname (confirmed via a raw smoke test: even
    ``TestClient``'s default ``Host: testserver`` header fails that check).

    ``allowed_hosts == ["*"]`` (``CatalogSettings``'s default) disables the
    protection entirely, for local/dev use before the real ingress hostname
    is known. Any other value enables it, scoped to exactly those Host
    header values (``"host:*"`` matches any port on that host -- see
    ``mcp.server.transport_security.TransportSecurityMiddleware._validate_host``).
    Origin is left unrestricted (``allowed_origins=[]``): that same
    middleware treats an absent ``Origin`` header as a pass, which is the
    normal case for a non-browser MCP client.
    """
    if allowed_hosts == ["*"]:
        return TransportSecuritySettings(enable_dns_rebinding_protection=False)
    return TransportSecuritySettings(
        enable_dns_rebinding_protection=True,
        allowed_hosts=allowed_hosts,
        allowed_origins=[],
    )


# The MCP server has no per-request URL context (see the module docstring),
# so links embedded in returned records use this fixed relative base rather
# than a real request's host. Clients use the returned ids with
# get_catalog_record, not these links.
_STAC_BASE = "/stac"

_store: CatalogStore | None = None


def set_store(store: CatalogStore) -> None:
    """Bind the current app's ``CatalogStore`` for the tools below to query.

    Called once from ``catalog.app``'s lifespan, right after that app builds
    its store -- see the module docstring for why this is a module-level
    accessor rather than request-scoped DI.
    """
    global _store
    _store = store


def _require_store() -> CatalogStore:
    if _store is None:
        raise RuntimeError(
            "MCP catalog store not initialized -- set_store() must be "
            "called from the app lifespan before any tool call reaches "
            "this module"
        )
    return _store


def _cap_items(items: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], bool]:
    """Trim ``items`` to at most ``_MAX_RESULT_ITEMS`` entries and roughly
    ``_MAX_RESULT_BYTES`` of serialized JSON. Returns ``(kept, truncated)``."""
    truncated = len(items) > _MAX_RESULT_ITEMS
    kept: list[dict[str, Any]] = []
    size = 64
    for item in items[:_MAX_RESULT_ITEMS]:
        item_bytes = len(json.dumps(item, separators=(",", ":"), default=str)) + 1
        if kept and size + item_bytes > _MAX_RESULT_BYTES:
            truncated = True
            break
        kept.append(item)
        size += item_bytes
    return kept, truncated


def _trim_dataset(collection: dict[str, Any]) -> dict[str, Any]:
    """Reduce a full STAC Collection to the fields useful for an LLM.

    A dataset *is* a Collection, so this reads the dataset's own metadata rather
    than a designated layer's. The previous version trimmed an Item and took
    dataset identity off whichever layer the mirror had marked representative,
    which meant a bundle answered with one member's title (92 of 3,834 differ,
    60 carrying a format suffix like "SHP EPSG:31259").
    """
    contacts = [c for c in collection.get("contacts") or [] if isinstance(c, dict)]
    publisher = next(
        (c for c in contacts if "publisher" in (c.get("roles") or [])),
        contacts[0] if contacts else None,
    )
    providers = [p for p in collection.get("providers") or [] if isinstance(p, dict)]
    producer = next(
        (p for p in providers if "producer" in (p.get("roles") or [])),
        providers[0] if providers else None,
    )
    keywords = [
        kw.get("value") if isinstance(kw, dict) else kw
        for kw in collection.get("keywords") or []
    ]
    themes = [
        concept.get("id")
        for theme in collection.get("themes") or []
        for concept in (theme.get("concepts") or [])
        if isinstance(concept, dict)
    ]
    language = collection.get("language")
    if isinstance(language, dict):
        language = language.get("code")
    spatial = ((collection.get("extent") or {}).get("spatial") or {}).get("bbox") or []
    return {
        "id": collection.get("id"),
        "title": collection.get("title"),
        "description": collection.get("description"),
        "type": collection.get("goat:layerType"),
        "publisher": (publisher or producer or {}).get("name") or None,
        "license": collection.get("license"),
        "keywords": [kw for kw in keywords if kw] or None,
        "themes": themes or None,
        "language": language,
        "updated": collection.get("updated"),
        "bbox": spatial[0] if spatial else None,
        "member_count": collection.get("goat:member_count") or 1,
    }


mcp = MCPServer("goat-catalog")


@mcp.tool()
async def search_catalog(
    q: str | None = None,
    bbox: str | None = None,
    bbox_boost: str | None = None,
    themes: str | None = None,
    language: str | None = None,
    year: int | None = None,
    license: str | None = None,
    publisher: str | None = None,
    type: str | None = None,
    geographical_code: str | None = None,
    geometry_type: str | None = None,
    datetime: str | None = None,
    sortby: str | None = None,
    limit: int = 10,
    offset: int = 0,
) -> dict[str, Any]:
    """Search the GOAT data catalog and return matching datasets, plus facet
    counts over the same filters.

    One entry per dataset (``member_count`` > 1 means several layers are bundled
    under it), and the facet counts are in the same unit -- so a count and the
    result set it describes always agree. Results are trimmed for brevity; use
    ``get_catalog_record(id)`` for the full record.

    Args:
        q: Free-text over title, description and keywords. Spaces mean AND
            ('radverkehr dresden' needs both words); commas mean OR
            ('radweg,fahrrad' matches either), and a row matching several
            alternatives ranks first. Matching is substring-based and
            case-insensitive, so 'grünfläche' finds 'Grünflächen' but not the
            reverse -- prefer the shorter stem. If a query returns nothing, drop
            a word or switch to commas rather than rephrasing wholesale, and use
            suggest_terms() to check a word exists. Prefer the structured
            filters below when describe_catalog() shows a matching value.
        bbox: Spatial filter 'west,south,east,north' in WGS84 lon/lat.
        bbox_boost: 'west,south,east,north' for ranking only (nothing excluded).
        themes: Comma-separated categories, e.g. transportation, landuse,
            environment, places, people, imagery, boundary, basemap, other.
        language: ISO 639-1 code, e.g. 'de' or 'en'.
        year: Data reference year, e.g. 2023.
        license: Comma-separated licenses.
        publisher: Comma-separated publisher/distributor names.
        type: Comma-separated layer types, e.g. feature, raster, table, bundle
            (describe_catalog() lists what this catalog holds).
        geographical_code: Comma-separated ISO 3166-1 alpha-2 codes.
        geometry_type: Comma-separated geometry types, e.g. point, line, polygon.
            Matches a dataset when ANY of its layers has that geometry.
        datetime: RFC 3339 instant or interval, e.g. '2023-01-01/2024-12-31'.
        sortby: e.g. '-updated'; prefix '-' for descending.
        limit: Max datasets to return (1-100, default 10).
        offset: Number of datasets to skip for pagination.
    """
    store = _require_store()
    limit = max(1, min(limit, 100))
    offset = max(offset, 0)

    try:
        # Parsed and assembled by the same request model the HTTP endpoints use
        # (`catalog.routers.params`), so an MCP search means exactly what the
        # equivalent `GET /stac/search` means -- including which filter
        # parameters exist at all, which the registry decides.
        #
        # Validated from a mapping (rather than by keyword) because these
        # arguments are the *unparsed* strings an MCP client sends -- the same
        # shape a query string carries, which is what the model's validators
        # take as input.
        params = SearchQuery.model_validate(
            {
                "bbox": bbox,
                "bbox_boost": bbox_boost,
                "datetime": datetime,
                "q": q,
                "sortby": sortby,
                "themes": themes,
                "language": language,
                "year": year,
                "license": license,
                "publisher": publisher,
                "type": type,
                "geographical_code": geographical_code,
                "geometry_type": geometry_type,
                "limit": limit,
                "offset": offset,
            }
        ).to_search_params(
            store.collection_registry, default_filter_lang="cql2-text", limit=limit
        )
        # Collection Search, not Item Search: one row per dataset, filtered as a
        # dataset. Item-level filters (`geometry_type`) become semi-joins, so
        # "datasets with a polygon layer" matches on *any* layer -- asking Item
        # Search for one designated layer per dataset missed 228 of the 1,886
        # datasets that contain one.
        rows, matched = search_collections(store, params)
        facets = run_aggregations(store, params, None, "collections")["aggregations"]
    except ApiError as exc:
        return {"error": exc.detail}

    trimmed = [
        _trim_dataset(
            collection_to_stac(collection_from_row(row), stac_base=_STAC_BASE)
        )
        for row in rows
    ]
    trimmed, truncated = _cap_items(trimmed)

    return {
        "numberMatched": matched,
        "numberReturned": len(trimmed),
        "truncated": truncated,
        "results": trimmed,
        "facets": facets,
    }


@mcp.tool()
async def describe_catalog(max_values: int = 40) -> dict[str, Any]:
    """List the filters this catalog accepts and the values they actually hold.

    Call this before searching. It answers the question a free-text query can
    only guess at -- what vocabulary this catalog uses -- so a request phrased
    in the user's words can be mapped onto real filter values instead of hoping
    a keyword matches. Every value returned here is a valid argument to the
    correspondingly named ``search_catalog`` parameter.

    Args:
        max_values: Maximum distinct values to list per filter (most frequent
            first). Filters with more values than this are marked truncated.
    """
    store = _require_store()
    # Counted through the aggregation service, in DATASETS, because that is what
    # `search_catalog` returns. Hand-rolled SQL over the items relation counted
    # layers and — worse — could advertise a value that the dataset search then
    # matched nothing for, because a collection-level filter reads the
    # collection's own column, not its members'. Going through the same code path
    # as `/aggregate` makes "every value here is a valid argument" true by
    # construction rather than by coincidence.
    # `{aggregation name: the parameter that narrows it}` -- never the name minus
    # "_count": `category_count` is narrowed with `?themes=`.
    params_for_aggregation = facet_params(store)
    aggregated = run_aggregations(store, SearchParams(), None, "collections")
    dataset_facets = {
        params_for_aggregation[a["name"]]: a["buckets"]
        for a in aggregated["aggregations"]
        if a.get("buckets") is not None and a["name"] in params_for_aggregation
    }
    filters: list[dict[str, Any]] = []
    for param, queryable in sorted(store.registry.filter_params().items()):
        entry: dict[str, Any] = {
            "parameter": param,
            "description": queryable.definition.get("description"),
            "type": queryable.json_type,
        }
        buckets = dataset_facets.get(param)
        if queryable.facetable and buckets is not None:
            entry["values"] = [
                {"value": bucket["key"], "count": bucket["frequency"]}
                for bucket in buckets[:max_values]
            ]
            entry["truncated"] = len(buckets) > max_values
        filters.append(entry)

    totals = safe_query(
        store,
        f"SELECT (SELECT count(*) FROM {CatalogStore.ITEMS}), "
        f"(SELECT count(*) FROM {CatalogStore.COLLECTIONS})",
    )
    items, collections = totals[0] if totals else (0, 0)
    return {
        "items": int(items or 0),
        "collections": int(collections or 0),
        "filters": filters,
        "sortable": sorted(store.registry.sortable()),
        "aggregations": aggregation_names(store),
    }


@mcp.tool()
async def suggest_terms(prefix: str, limit: int = 10) -> dict[str, Any]:
    """Test whether a word appears in this catalog, before searching on it.

    Returns dataset titles whose text contains a word starting with ``prefix``.
    Use it to check a term cheaply -- German catalogues compound words
    ('Radverkehrsnetz', 'Radwegekataster'), so a guessed keyword often needs a
    shorter stem than the one a user typed.

    Args:
        prefix: Word beginning, e.g. 'radver'. Case-insensitive.
        limit: Maximum titles to return.
    """
    store = _require_store()
    cleaned = prefix.strip().lower()
    if not cleaned:
        return {"prefix": prefix, "matches": []}
    # \b anchors at a word start, so 'art' does not match 'Karte'; the pattern
    # is a parameter, never interpolated, and RE2 has no catastrophic
    # backtracking to worry about.
    rows = safe_query(
        store,
        f"SELECT title, id FROM {CatalogStore.ITEMS} "
        f"WHERE regexp_matches(search_text, ?) "
        f"ORDER BY member_count DESC, updated DESC LIMIT ?",
        [rf"\b{re.escape(cleaned)}", max(1, min(limit, 50))],
    )
    return {
        "prefix": cleaned,
        "matches": [{"title": title, "id": item_id} for title, item_id in rows],
    }


@mcp.tool()
async def get_catalog_record(item_id: str) -> dict[str, Any]:
    """Return the full catalog record for an id (from search_catalog results).

    Resolves either a single dataset (STAC Item) or a bundle (STAC Collection
    whose member layers ride under ``goat:items``, each a STAC Item).
    """
    store = _require_store()
    resolved = resolve_id(store, item_id)
    if resolved is None:
        return {"error": f"Catalog record {item_id!r} not found"}

    if resolved["kind"] == "collection":
        collection = collection_to_stac(
            collection_from_row(resolved["collection_row"]), stac_base=_STAC_BASE
        )
        member_items = [
            record_to_item(
                item_from_row(row), stac_base=_STAC_BASE, collection_id=item_id
            )
            for row in resolved["member_rows"]
        ]
        member_count = resolved["member_count"]
        # Two independent sources of truncation can both apply: `resolve_id`
        # already caps the member rows it fetches (its own `_MEMBER_LIMIT`),
        # and `_cap_items` caps again for MCP transport size -- either one
        # dropping rows means the client is not seeing every member.
        resolve_truncated = member_count > len(resolved["member_rows"])
        member_items, cap_truncated = _cap_items(member_items)
        collection["goat:items"] = member_items
        collection["goat:items_truncated"] = resolve_truncated or cap_truncated
        collection["goat:member_count"] = member_count
        return collection

    return record_to_item(
        item_from_row(resolved["row"]),
        stac_base=_STAC_BASE,
        collection_id=resolved["collection_id"],
    )
