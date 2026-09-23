"""catalog served under a path prefix (ROOT_PATH / CATALOG_ROOT_PATH)."""

from collections.abc import Iterator
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from catalog.app import create_app
from catalog.config import CatalogSettings


@pytest.fixture()
def prefixed_client(catalog_dir: Path) -> Iterator[TestClient]:
    settings = CatalogSettings(data_dir=catalog_dir, auth=False, root_path="catalog/")
    assert settings.root_path == "/catalog"
    with TestClient(create_app(settings)) as c:
        yield c


# The gateway either strips the prefix before forwarding or passes the full
# path through; both must work.
@pytest.mark.parametrize("path", ["/stac", "/catalog/stac"])
def test_stac_links_carry_the_prefix(prefixed_client: TestClient, path: str) -> None:
    r = prefixed_client.get(path)
    assert r.status_code == 200
    links = {link["rel"]: link["href"] for link in r.json()["links"]}
    assert links["self"].startswith("http://testserver/catalog/stac")
    assert links["conformance"].startswith("http://testserver/catalog/")
    assert links["search"].startswith("http://testserver/catalog/")


def test_collection_item_links_carry_the_prefix(prefixed_client: TestClient) -> None:
    collections = prefixed_client.get("/stac/collections").json()["collections"]
    assert collections
    hrefs = [link["href"] for c in collections for link in c["links"]]
    stac_hrefs = [h for h in hrefs if "/stac" in h]
    assert stac_hrefs and all(
        h.startswith("http://testserver/catalog/stac") for h in stac_hrefs
    )


def test_stac_links_unprefixed_by_default(client: TestClient) -> None:
    links = {link["rel"]: link["href"] for link in client.get("/stac").json()["links"]}
    assert links["self"].startswith("http://testserver/stac")
