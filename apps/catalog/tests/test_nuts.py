"""HTTP-layer tests for the ``/stac/nuts`` spatial-filter helper endpoints
(Task 10): search (``q``/``level``/``limit``) and per-region GeoJSON geometry,
backed by the deterministic ``nuts.parquet`` fixture
(``tests/fixtures/gen_catalog.py::NUTS_REGIONS`` -- 1 level-0 country row +
16 level-1 Bundesland rows, all ``country == "DE"``).
"""

from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from goatlib.auth import JOSEError

from catalog.app import create_app
from catalog.config import CatalogSettings

# --------------------------------------------------------------------------
# GET /stac/nuts (search)
# --------------------------------------------------------------------------


def test_nuts_search_all(client: TestClient) -> None:
    r = client.get("/stac/nuts")
    assert r.status_code == 200
    body = r.json()
    assert isinstance(body, list)
    assert len(body) == 17  # 1 country row + 16 Bundesländer


def test_nuts_search_q_substring_matches_name(client: TestClient) -> None:
    r = client.get("/stac/nuts", params={"q": "Bay"})
    assert r.status_code == 200
    body = r.json()
    assert len(body) == 1
    assert body[0]["nuts_id"] == "DE2"
    assert body[0]["nuts_name"] == "Bayern"


def test_nuts_search_q_substring_matches_id(client: TestClient) -> None:
    r = client.get("/stac/nuts", params={"q": "DEG"})
    assert r.status_code == 200
    body = r.json()
    assert len(body) == 1
    assert body[0]["nuts_id"] == "DEG"


def test_nuts_search_q_case_insensitive(client: TestClient) -> None:
    r = client.get("/stac/nuts", params={"q": "bayern"})
    assert r.status_code == 200
    assert len(r.json()) == 1


def test_nuts_search_level_filter(client: TestClient) -> None:
    r0 = client.get("/stac/nuts", params={"level": 0})
    assert r0.status_code == 200
    body0 = r0.json()
    assert len(body0) == 1
    assert body0[0]["nuts_id"] == "DE"
    assert body0[0]["level"] == 0

    r1 = client.get("/stac/nuts", params={"level": 1})
    assert r1.status_code == 200
    body1 = r1.json()
    assert len(body1) == 16
    assert all(row["level"] == 1 for row in body1)


def test_nuts_search_limit_respected(client: TestClient) -> None:
    r = client.get("/stac/nuts", params={"limit": 3})
    assert r.status_code == 200
    assert len(r.json()) == 3


def test_nuts_search_limit_default_is_20(client: TestClient) -> None:
    r = client.get("/stac/nuts")
    assert r.status_code == 200
    assert len(r.json()) <= 20


def test_nuts_search_limit_capped_at_100(client: TestClient) -> None:
    """An oversized limit is served as the maximum, like every other endpoint
    (catalog.limits) -- not rejected."""
    r = client.get("/stac/nuts", params={"limit": 500})
    assert r.status_code == 200
    assert len(r.json()) <= 100


def test_nuts_search_limit_below_one_is_400(client: TestClient) -> None:
    r = client.get("/stac/nuts", params={"limit": 0})
    assert r.status_code == 400
    assert r.json()["code"] == 400


def test_nuts_search_bbox_shape(client: TestClient) -> None:
    r = client.get("/stac/nuts", params={"q": "Bayern"})
    assert r.status_code == 200
    row = r.json()[0]
    assert set(row) == {"nuts_id", "nuts_name", "level", "country", "bbox"}
    bbox = row["bbox"]
    assert len(bbox) == 4
    assert all(isinstance(v, float) for v in bbox)
    w, s, e, n = bbox
    assert w < e
    assert s < n


def test_nuts_search_country_field(client: TestClient) -> None:
    r = client.get("/stac/nuts", params={"level": 0})
    assert r.json()[0]["country"] == "DE"


# --------------------------------------------------------------------------
# GET /stac/nuts/{nuts_id}/geometry
# --------------------------------------------------------------------------


def test_nuts_geometry_valid_feature(client: TestClient) -> None:
    r = client.get("/stac/nuts/DE2/geometry")
    assert r.status_code == 200
    body = r.json()
    assert body["type"] == "Feature"
    assert body["properties"] == {
        "nuts_id": "DE2",
        "nuts_name": "Bayern",
        "level": 1,
        "country": "DE",
    }
    geometry = body["geometry"]
    assert geometry["type"] == "Polygon"
    assert len(geometry["coordinates"]) > 0
    assert len(geometry["coordinates"][0]) > 0


def test_nuts_geometry_not_found_404(client: TestClient) -> None:
    r = client.get("/stac/nuts/does-not-exist/geometry")
    assert r.status_code == 404
    assert r.json()["code"] == 404


# --------------------------------------------------------------------------
# Missing nuts.parquet -> empty table, never an error
# --------------------------------------------------------------------------


def test_nuts_search_absent_parquet_returns_empty_list(tmp_path: Path) -> None:
    app = create_app(CatalogSettings(data_dir=tmp_path, auth=False))
    with TestClient(app) as c:
        r = c.get("/stac/nuts")
    assert r.status_code == 200
    assert r.json() == []


def test_nuts_geometry_absent_parquet_404(tmp_path: Path) -> None:
    app = create_app(CatalogSettings(data_dir=tmp_path, auth=False))
    with TestClient(app) as c:
        r = c.get("/stac/nuts/DE2/geometry")
    assert r.status_code == 404


# --------------------------------------------------------------------------
# Auth (mirrors test_endpoints.py's Task 9 pattern)
# --------------------------------------------------------------------------


def test_nuts_reads_are_public_even_with_auth_enabled(
    catalog_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The spatial-filter helpers are public reads like the rest of ``/stac``.

    The catalog page's region typeahead calls these, and that page serves
    anonymous visitors (design S1/S14) -- if these 401'd, the public filter
    would be dead while the rest of the page worked.
    """
    monkeypatch.setattr(
        "catalog.auth.validate_token",
        lambda settings, token: {"sub": "test-user"},
    )
    app = create_app(CatalogSettings(data_dir=catalog_dir, auth=True))
    with TestClient(app) as authed_client:
        assert authed_client.get("/stac/nuts").status_code == 200
        assert authed_client.get("/stac/nuts/DE2/geometry").status_code == 200

        r = authed_client.get(
            "/stac/nuts", headers={"Authorization": "Bearer sometoken"}
        )
        assert r.status_code == 200


def test_nuts_invalid_token_is_rejected(
    catalog_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def _reject(settings: object, token: str) -> dict[str, str]:
        raise JOSEError("expired")

    monkeypatch.setattr("catalog.auth.validate_token", _reject)
    app = create_app(CatalogSettings(data_dir=catalog_dir, auth=True))
    with TestClient(app) as authed_client:
        r = authed_client.get("/stac/nuts", headers={"Authorization": "Bearer bad"})
        assert r.status_code == 401
