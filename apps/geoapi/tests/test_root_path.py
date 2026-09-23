"""geoapi served under a path prefix (ROOT_PATH)."""

from collections.abc import Generator

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def prefixed_client(
    mock_ducklake_manager, mock_layer_service, mock_schema_cache, monkeypatch
) -> Generator[TestClient, None, None]:
    from geoapi.main import app

    # What FastAPI(root_path=settings.ROOT_PATH) does when ROOT_PATH is set.
    monkeypatch.setattr(app, "root_path", "/geoapi")
    with TestClient(app, raise_server_exceptions=True) as client:
        yield client


# The gateway either strips the prefix before forwarding (nginx rewrite,
# Traefik stripPrefix) or passes the full path through; both must work.
@pytest.mark.parametrize("path", ["/", "/geoapi/"])
def test_landing_links_carry_the_prefix(prefixed_client: TestClient, path: str) -> None:
    response = prefixed_client.get(path)
    assert response.status_code == 200
    links = {link["rel"]: link["href"] for link in response.json()["links"]}
    assert links["self"] == "http://testserver/geoapi"
    assert links["conformance"] == "http://testserver/geoapi/conformance"


def test_landing_links_unprefixed_by_default(test_client: TestClient) -> None:
    links = {link["rel"]: link["href"] for link in test_client.get("/").json()["links"]}
    assert links["self"] == "http://testserver"
    assert links["conformance"] == "http://testserver/conformance"
