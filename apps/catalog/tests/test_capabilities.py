"""Conformance and response documentation are derived, not asserted by hand.

``catalog.services.capabilities`` declares each capability with the conformance
URIs it implies and the landing links it contributes; ``/conformance``, the
landing page and the OpenAPI document all read from that one declaration.
"""

from fastapi.testclient import TestClient

from catalog.services import capabilities
from catalog.services.registry import QueryableRegistry, build_registry
from catalog.store import CatalogStore


class TestDerivedConformance:
    def test_endpoint_matches_the_capability_declaration(
        self, client: TestClient, store: CatalogStore
    ) -> None:
        served = client.get("/stac/conformance").json()["conformsTo"]
        assert served == capabilities.conformance_classes(store.registry)

    def test_landing_and_conformance_agree(self, client: TestClient) -> None:
        """Two endpoints, one source -- they cannot drift apart."""
        landing = client.get("/stac").json()["conformsTo"]
        conformance = client.get("/stac/conformance").json()["conformsTo"]
        assert landing == conformance

    def test_the_audited_uris_are_all_declared(
        self, registry: QueryableRegistry
    ) -> None:
        """Regrouping the list by capability must not have dropped a URI.

        These are the classes audited in ``docs/goat-catalog-api.md`` §2 for the
        fixture catalog, which has every kind of column.
        """
        served = set(capabilities.conformance_classes(registry))
        assert served == {
            "https://api.stacspec.org/v1.0.0/core",
            "https://api.stacspec.org/v1.0.0/collections",
            "https://api.stacspec.org/v1.0.0/ogcapi-features",
            "https://api.stacspec.org/v1.0.0/item-search",
            "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/core",
            "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/oas30",
            "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/geojson",
            "https://api.stacspec.org/v1.0.0/item-search#filter",
            "http://www.opengis.net/spec/ogcapi-features-3/1.0/conf/filter",
            "http://www.opengis.net/spec/cql2/1.0/conf/basic-cql2",
            "http://www.opengis.net/spec/cql2/1.0/conf/cql2-text",
            "http://www.opengis.net/spec/cql2/1.0/conf/cql2-json",
            "https://api.stacspec.org/v1.0.0/collection-search",
            "http://www.opengis.net/spec/ogcapi-common-2/1.0/conf/simple-query",
            "https://api.stacspec.org/v1.0.0/collection-search#free-text",
            "https://api.stacspec.org/v1.0.0-rc.1/item-search#free-text",
            "https://api.stacspec.org/v1.1.0/item-search#sort",
            "https://api.stacspec.org/v1.1.0/collection-search#sort",
            "https://api.stacspec.org/v0.3.0/aggregation",
        }

    def test_a_catalog_with_no_queryables_claims_less(self) -> None:
        """The point of deriving it: an unfilterable catalog stops claiming
        the Filter and Sort extensions instead of 400-ing every filter it
        advertised."""
        empty = build_registry({})
        served = capabilities.conformance_classes(empty)
        assert "https://api.stacspec.org/v1.0.0/core" in served
        assert "http://www.opengis.net/spec/cql2/1.0/conf/cql2-text" not in served
        assert "https://api.stacspec.org/v1.1.0/item-search#sort" not in served
        # Aggregation survives: `total_count` needs no column.
        assert "https://api.stacspec.org/v0.3.0/aggregation" in served

    def test_geometry_only_catalog_has_nothing_to_sort_by(self) -> None:
        sortless = build_registry({"geometry": "GEOMETRY", "tags": "VARCHAR[]"})
        served = capabilities.conformance_classes(sortless)
        assert "https://api.stacspec.org/v1.1.0/item-search#sort" not in served
        # `geometry` is still a queryable, so filtering remains available.
        assert "http://www.opengis.net/spec/cql2/1.0/conf/cql2-text" in served


class TestDocumentedResponses:
    """Every endpoint declares its response schema in the OpenAPI document."""

    def test_operations_reference_a_response_schema(self, client: TestClient) -> None:
        schema = client.get("/api/openapi.json").json()
        expected = {
            ("/stac", "get"): "application/json",
            ("/stac/conformance", "get"): "application/json",
            ("/stac/queryables", "get"): "application/schema+json",
            ("/stac/collections", "get"): "application/json",
            ("/stac/collections/{cid}", "get"): "application/json",
            ("/stac/collections/{cid}/items", "get"): "application/geo+json",
            ("/stac/search", "get"): "application/geo+json",
            ("/stac/search", "post"): "application/geo+json",
            ("/stac/aggregate", "get"): "application/json",
            ("/stac/aggregations", "get"): "application/json",
        }
        for (path, method), media_type in expected.items():
            content = schema["paths"][path][method]["responses"]["200"]["content"]
            assert media_type in content, f"{method} {path} is missing {media_type}"
            assert (
                "$ref" in content[media_type]["schema"]
            ), f"{method} {path} documents no response schema"

    def test_search_params_are_documented(self, client: TestClient) -> None:
        """The shared request model must surface as real query parameters."""
        schema = client.get("/api/openapi.json").json()
        names = {
            p["name"] for p in schema["paths"]["/stac/search"]["get"]["parameters"]
        }
        assert {
            "bbox",
            "datetime",
            "q",
            "filter",
            "filter-lang",
            "sortby",
            "themes",
            "license",
            "geometry_type",
            "year",
            "limit",
            "offset",
        } <= names
