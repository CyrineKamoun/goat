"""Tests for the MCP server port (Task 11): app-level mount gating, auth
gating, session-manager pairing across multiple apps, and tool-logic unit
tests calling the tool coroutines directly (not over the MCP wire protocol)
against the fixture store.
"""

import json
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

import catalog.routers.mcp as mcp_module
from catalog.app import create_app
from catalog.config import CatalogSettings
from catalog.store import CatalogStore

from .fixtures.gen_catalog import write_catalog, write_nuts

_MCP_HEADERS = {
    "Accept": "application/json, text/event-stream",
    "Content-Type": "application/json",
}

# --------------------------------------------------------------------------
# App-level mount gating
# --------------------------------------------------------------------------


def _has_mount(app_routes: Sequence[object], path: str) -> bool:
    return any(getattr(route, "path", None) == path for route in app_routes)


def _handshake_list_tools(client: TestClient, headers: dict[str, str]) -> Any:
    """A real Streamable HTTP handshake (initialize -> notifications/initialized
    -> tools/list), returning the ``tools/list`` response.

    Typed ``Any`` rather than a concrete response class: ``starlette
    .testclient.TestClient`` returns an ``httpx2.Response`` on this install
    (``starlette.testclient`` now requires the ``httpx2`` package, only a
    transitive dependency here via ``mcp`` -> ``httpx2``, not one this
    project declares directly), so pinning a response type in test code
    would couple it to that transitive dependency's continued presence.
    """
    init = client.post(
        "/mcp",
        json={
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "test", "version": "0"},
            },
        },
        headers=headers,
    )
    assert init.status_code == 200, init.text
    session_headers = {**headers, "mcp-session-id": init.headers["mcp-session-id"]}
    client.post(
        "/mcp",
        json={"jsonrpc": "2.0", "method": "notifications/initialized"},
        headers=session_headers,
    )
    return client.post(
        "/mcp",
        json={"jsonrpc": "2.0", "id": 2, "method": "tools/list"},
        headers=session_headers,
    )


def _tool_names(tools_list_response: Any) -> set[str]:
    body = tools_list_response.text.split("data: ", 1)[1]
    result: set[str] = {tool["name"] for tool in json.loads(body)["result"]["tools"]}
    return result


def test_mcp_mounted_when_enabled(catalog_dir: Path) -> None:
    app = create_app(CatalogSettings(data_dir=catalog_dir, auth=False, enable_mcp=True))
    assert _has_mount(app.routes, "/mcp")
    with TestClient(app) as client:
        # A bare GET is not a valid Streamable HTTP request, but a mounted
        # route must never 404 -- that would be indistinguishable from "no
        # /mcp at all". It also must never 421 (Misdirected Request): that
        # was a real bug caught during self-review -- streamable_http_app()'s
        # own default `host="127.0.0.1"` auto-enables a DNS-rebinding Host
        # header allowlist, which TestClient's default "testserver" Host
        # header (and every real production hostname) fails, 421ing every
        # request. `!= 404` alone would NOT have caught that regression, so
        # this explicitly rules it out too.
        r = client.get("/mcp")
        assert r.status_code not in (404, 421)


def test_mcp_protocol_round_trip_lists_tools(catalog_dir: Path) -> None:
    """A real Streamable HTTP handshake, not just a route-table probe --
    confirms both kept tools are actually reachable over the MCP wire
    protocol, end to end."""
    app = create_app(CatalogSettings(data_dir=catalog_dir, auth=False, enable_mcp=True))
    with TestClient(app) as client:
        listed = _handshake_list_tools(client, _MCP_HEADERS)
    assert listed.status_code == 200
    assert _tool_names(listed) == {
        "search_catalog",
        "get_catalog_record",
        "describe_catalog",
        "suggest_terms",
    }


def test_mcp_not_mounted_when_disabled(catalog_dir: Path) -> None:
    app = create_app(
        CatalogSettings(data_dir=catalog_dir, auth=False, enable_mcp=False)
    )
    assert not _has_mount(app.routes, "/mcp")
    with TestClient(app) as client:
        r = client.get("/mcp")
        assert r.status_code == 404


def test_mcp_import_failure_leaves_app_bootable(
    catalog_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Simulate ``import catalog.routers.mcp`` failing (e.g. the ``mcp``
    dependency being broken/missing) by setting its ``sys.modules`` entry to
    ``None`` -- Python's import machinery raises ``ImportError`` immediately
    for a module name mapped to ``None`` -- and confirm ``create_app`` still
    produces a working app with no ``/mcp`` route, rather than raising.
    """
    monkeypatch.setitem(sys.modules, "catalog.routers.mcp", None)

    app = create_app(CatalogSettings(data_dir=catalog_dir, auth=False, enable_mcp=True))
    assert not _has_mount(app.routes, "/mcp")
    with TestClient(app) as client:
        r = client.get("/healthz")
        assert r.status_code == 200


# --------------------------------------------------------------------------
# Auth gating (api spec §1: the whole service requires an authenticated
# GOAT session when AUTH=True -- /stac and /nuts get this via
# Depends(require_auth); /mcp is a raw ASGI Mount that DI can't reach, so
# catalog.auth.BearerAuthASGIMiddleware wraps it instead).
# --------------------------------------------------------------------------


def test_mcp_requires_auth_when_auth_enabled(catalog_dir: Path) -> None:
    app = create_app(CatalogSettings(data_dir=catalog_dir, auth=True, enable_mcp=True))
    with TestClient(app) as client:
        r = client.get("/mcp")
        assert r.status_code == 401
        assert r.headers["www-authenticate"] == "Bearer"
        assert r.json()["code"] == 401


def test_mcp_auth_open_access_when_disabled(catalog_dir: Path) -> None:
    # AUTH=False -- covered end to end by test_mcp_protocol_round_trip_lists_tools
    # above; this asserts the specific "no auth gate at all" behavior in
    # isolation (no Authorization header, still not a 401).
    app = create_app(CatalogSettings(data_dir=catalog_dir, auth=False, enable_mcp=True))
    with TestClient(app) as client:
        r = client.get("/mcp")
        assert r.status_code != 401


def test_mcp_auth_valid_token_allows_handshake(
    catalog_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        "catalog.auth.validate_token",
        lambda settings, token: {"sub": "test-user"},
    )
    app = create_app(CatalogSettings(data_dir=catalog_dir, auth=True, enable_mcp=True))
    headers = {**_MCP_HEADERS, "Authorization": "Bearer sometoken"}
    with TestClient(app) as client:
        listed = _handshake_list_tools(client, headers)
    assert listed.status_code == 200
    assert _tool_names(listed) == {
        "search_catalog",
        "get_catalog_record",
        "describe_catalog",
        "suggest_terms",
    }


def test_mcp_auth_invalid_token_is_401(
    catalog_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from goatlib.auth import JOSEError

    def _raise(settings: CatalogSettings, token: str) -> dict[str, object]:
        raise JOSEError("bad signature")

    monkeypatch.setattr("catalog.auth.validate_token", _raise)
    app = create_app(CatalogSettings(data_dir=catalog_dir, auth=True, enable_mcp=True))
    with TestClient(app) as client:
        r = client.get("/mcp", headers={"Authorization": "Bearer sometoken"})
    assert r.status_code == 401


# --------------------------------------------------------------------------
# mcp_allowed_hosts narrowing (Host-header DNS-rebinding protection)
# --------------------------------------------------------------------------


def test_mcp_allowed_hosts_wildcard_default_disables_protection(
    catalog_dir: Path,
) -> None:
    # The default (["*"]) is what every other test in this file relies on --
    # TestClient's Host header is "testserver", which a narrowed allowlist
    # would reject.
    app = create_app(CatalogSettings(data_dir=catalog_dir, auth=False, enable_mcp=True))
    with TestClient(app) as client:
        r = client.get("/mcp")
    assert r.status_code != 421


def test_mcp_allowed_hosts_narrowed_to_matching_host_still_works(
    catalog_dir: Path,
) -> None:
    app = create_app(
        CatalogSettings(
            data_dir=catalog_dir,
            auth=False,
            enable_mcp=True,
            mcp_allowed_hosts=["testserver"],
        )
    )
    with TestClient(app) as client:
        r = client.get("/mcp")
    assert r.status_code != 421


def test_mcp_allowed_hosts_narrowed_to_other_host_is_421(catalog_dir: Path) -> None:
    app = create_app(
        CatalogSettings(
            data_dir=catalog_dir,
            auth=False,
            enable_mcp=True,
            mcp_allowed_hosts=["api.goat.example.com"],
        )
    )
    with TestClient(app) as client:
        r = client.get("/mcp")
    assert r.status_code == 421


# --------------------------------------------------------------------------
# Session-manager pairing across multiple create_app() calls
# --------------------------------------------------------------------------


def test_mcp_two_sequential_apps_each_handshake_correctly(
    catalog_dir: Path, tmp_path: Path
) -> None:
    """Guards against session-manager mispairing: the module-level ``mcp``
    singleton's ``session_manager`` attribute is rebuilt by EVERY
    ``streamable_http_app()`` call (one per ``create_app()``), so a lifespan
    that re-reads ``mcp_module.mcp.session_manager`` lazily (instead of a
    reference captured at mount time) would pick up whichever app was built
    LAST, not the one whose lifespan is actually running. Building both
    apps' mounts before opening either ``TestClient`` reproduces exactly
    that ordering.
    """
    dir2 = tmp_path / "catalog2"
    write_catalog(dir2)
    write_nuts(dir2)

    app1 = create_app(
        CatalogSettings(data_dir=catalog_dir, auth=False, enable_mcp=True)
    )
    app2 = create_app(CatalogSettings(data_dir=dir2, auth=False, enable_mcp=True))

    for app in (app1, app2):
        with TestClient(app) as client:
            listed = _handshake_list_tools(client, _MCP_HEADERS)
        assert listed.status_code == 200
        assert _tool_names(listed) == {
            "search_catalog",
            "get_catalog_record",
            "describe_catalog",
            "suggest_terms",
        }


# --------------------------------------------------------------------------
# Tool-logic unit tests (direct coroutine calls, not over the MCP protocol)
# --------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _bind_store(store: CatalogStore) -> None:
    mcp_module.set_store(store)


async def test_search_catalog_returns_items() -> None:
    result = await mcp_module.search_catalog(q="Radverkehrsnetz", limit=5)
    assert "error" not in result
    assert result["numberReturned"] > 0
    assert result["numberReturned"] <= 5
    assert all("id" in item for item in result["results"])
    assert "facets" in result


async def test_search_catalog_limit_is_clamped(store: CatalogStore) -> None:
    result = await mcp_module.search_catalog(limit=99999)
    assert "error" not in result
    # The fixture's ~196 datasets exceed the 100 clamp -- proves the
    # crafted large `limit` never reaches the query unclamped.
    assert result["numberMatched"] > 100
    assert result["numberReturned"] == 100


async def test_search_catalog_invalid_bbox_returns_clean_error() -> None:
    result = await mcp_module.search_catalog(bbox="not,a,bbox")
    assert "error" in result
    assert isinstance(result["error"], str)


async def test_get_catalog_record_item() -> None:
    record = await mcp_module.get_catalog_record("radverkehrsnetz-dresden-0")
    assert "error" not in record
    assert record["id"] == "radverkehrsnetz-dresden-0"
    assert record["type"] == "Feature"


async def test_get_catalog_record_collection_bundle() -> None:
    record = await mcp_module.get_catalog_record("src-1")
    assert "error" not in record
    assert record["id"] == "src-1"
    assert record["type"] == "Collection"
    assert len(record["goat:items"]) == 4
    assert record["goat:items_truncated"] is False


async def test_get_catalog_record_unknown_id_returns_clean_error_not_exception() -> (
    None
):
    record = await mcp_module.get_catalog_record("does-not-exist")
    assert record == {"error": "Catalog record 'does-not-exist' not found"}


class TestDiscoveryTools:
    """The tools that let an agent learn the catalog instead of guessing.

    A model translating "cycling infrastructure in Saxony" into a query needs
    the catalog's own vocabulary; these return it, which is what makes the
    structured filters usable without semantic search.
    """

    async def test_describe_catalog_lists_filters_with_real_values(self) -> None:
        result = await mcp_module.describe_catalog()

        assert result["items"] > 0
        assert result["collections"] > 0
        by_param = {f["parameter"]: f for f in result["filters"]}
        # Every scalar filter search_catalog accepts must be described.
        assert {"themes", "license", "publisher", "type"} <= set(by_param)

        licenses = by_param["license"]["values"]
        assert licenses, "a facetable filter must list its values"
        assert all({"value", "count"} == set(v) for v in licenses)
        # Ordered most frequent first, and every value is really usable.
        counts = [v["count"] for v in licenses]
        assert counts == sorted(counts, reverse=True)

        search = await mcp_module.search_catalog(license=licenses[0]["value"], limit=1)
        assert search["numberMatched"] > 0

    async def test_describe_catalog_reports_sortable_and_aggregations(self) -> None:
        result = await mcp_module.describe_catalog()
        assert "updated" in result["sortable"]
        assert "total_count" in result["aggregations"]

    async def test_suggest_terms_finds_a_word_by_its_stem(self) -> None:
        result = await mcp_module.suggest_terms("radver")
        assert result["matches"], "the fixture has Radverkehrsnetz titles"
        assert any("Radverkehrsnetz" in m["title"] for m in result["matches"])

    async def test_suggest_terms_anchors_at_word_boundaries(self) -> None:
        """'kataster' must not match 'Liegenschaftskataster' mid-word.

        Substring matching is generous by design in `q`, but a term *probe*
        that reported a hit for any infix would tell the model a word exists
        when it does not.
        """
        infix = await mcp_module.suggest_terms("erkehrsnetz")
        assert infix["matches"] == []

    async def test_suggest_terms_blank_prefix_is_empty_not_everything(self) -> None:
        assert await mcp_module.suggest_terms("   ") == {
            "prefix": "   ",
            "matches": [],
        }
