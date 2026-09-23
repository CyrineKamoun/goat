"""processes served under a path prefix (ROOT_PATH)."""

import pytest
from fastapi.testclient import TestClient

from processes.main import app

CONFORMANCE = "http://www.opengis.net/def/rel/ogc/1.0/conformance"


@pytest.fixture
def prefixed_client(monkeypatch) -> TestClient:
    # What FastAPI(root_path=settings.ROOT_PATH) does when ROOT_PATH is set.
    monkeypatch.setattr(app, "root_path", "/processes")
    return TestClient(app)


# The gateway either strips the prefix before forwarding or passes the full
# path through; both must work.
@pytest.mark.parametrize("path", ["/", "/processes/"])
def test_landing_links_carry_the_prefix(prefixed_client: TestClient, path: str) -> None:
    response = prefixed_client.get(path)
    assert response.status_code == 200
    links = {link["rel"]: link["href"] for link in response.json()["links"]}
    assert links["self"] == "http://testserver/processes/"
    assert links[CONFORMANCE] == "http://testserver/processes/conformance"
    assert links["service-desc"] == "http://testserver/processes/api/openapi.json"


def test_landing_links_keep_forwarded_host_and_prefix(
    prefixed_client: TestClient,
) -> None:
    response = prefixed_client.get(
        "/",
        headers={"x-forwarded-proto": "https", "x-forwarded-host": "goat.example.org"},
    )
    links = {link["rel"]: link["href"] for link in response.json()["links"]}
    assert links["self"] == "https://goat.example.org/processes/"


def test_landing_links_unprefixed_by_default() -> None:
    links = {
        link["rel"]: link["href"] for link in TestClient(app).get("/").json()["links"]
    }
    assert links["self"] == "http://testserver/"
    assert links[CONFORMANCE] == "http://testserver/conformance"
