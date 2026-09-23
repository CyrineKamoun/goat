"""Serving a GOAT service under a path prefix (goatlib.api.root_path)."""

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from goatlib.api import mount_api_docs, normalize_root_path


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
