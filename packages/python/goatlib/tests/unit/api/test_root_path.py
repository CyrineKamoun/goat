"""Serving a GOAT service under a path prefix (goatlib.api.root_path)."""

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from goatlib.api import RootPathMiddleware, mount_api_docs, normalize_root_path


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (None, ""),
        ("", ""),
        ("/", ""),
        ("  ", ""),
        ("geoapi", "/geoapi"),
        ("/geoapi", "/geoapi"),
        ("/geoapi/", "/geoapi"),
        (" geoapi/ ", "/geoapi"),
        ("/gw/geoapi/", "/gw/geoapi"),
    ],
)
def test_normalize_root_path(raw: str | None, expected: str) -> None:
    assert normalize_root_path(raw) == expected


def _docs_app(root_path: str) -> FastAPI:
    app = FastAPI(
        title="t",
        openapi_url="/api/openapi.json",
        docs_url=None,
        redoc_url=None,
        root_path=root_path,
    )
    mount_api_docs(app)
    return app


@pytest.mark.parametrize("path", ["/api/docs", "/api/redoc"])
def test_docs_pages_reference_prefixed_openapi_and_favicon(path: str) -> None:
    client = TestClient(_docs_app("/geoapi"))
    html = client.get(path).text
    assert "/geoapi/api/openapi.json" in html
    assert "/geoapi/static/" in html


@pytest.mark.parametrize("path", ["/api/docs", "/api/redoc"])
def test_docs_pages_unchanged_without_root_path(path: str) -> None:
    html = TestClient(_docs_app("")).get(path).text
    assert "'/api/openapi.json'" in html or '"/api/openapi.json"' in html
    assert "//api/openapi.json" not in html


def _redirect_app(root_path: str) -> FastAPI:
    app = FastAPI(root_path=root_path)
    app.add_middleware(RootPathMiddleware)

    @app.get("/collections")
    def collections() -> dict[str, bool]:
        return {"ok": True}

    return app


# The gateway either strips the prefix before forwarding or passes the full
# path through; the trailing-slash redirect must keep the prefix either way.
@pytest.mark.parametrize("path", ["/collections/", "/geoapi/collections/"])
def test_redirect_keeps_prefix_whether_or_not_it_was_stripped(path: str) -> None:
    client = TestClient(_redirect_app("/geoapi"), follow_redirects=False)
    r = client.get(path)
    assert r.status_code == 307
    assert r.headers["location"] == "http://testserver/geoapi/collections"


@pytest.mark.parametrize("path", ["/collections", "/geoapi/collections"])
def test_routes_match_whether_or_not_prefix_was_stripped(path: str) -> None:
    assert TestClient(_redirect_app("/geoapi")).get(path).json() == {"ok": True}


def test_middleware_is_a_no_op_without_root_path() -> None:
    client = TestClient(_redirect_app(""), follow_redirects=False)
    assert (
        client.get("/collections/").headers["location"]
        == "http://testserver/collections"
    )
