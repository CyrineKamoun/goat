"""What this server can do, and the conformance/links that follow from it.

``conformsTo`` used to be one flat list of URIs, and the landing page listed
its ``queryables``/``aggregate`` links unconditionally. Both were accurate but
unconditional: nothing connected the claim to the capability. A file whose
columns the harvester renamed away can leave the service with nothing to
filter on, and it would still have advertised the Filter extension and a
``queryables`` link.

So each capability is declared once, together with the conformance URIs it
implies, the landing-page links it contributes, and the condition under which
it is actually available -- and the ``/conformance`` endpoint and landing page
are both derived from that. This is what stac-fastapi gets from registering
extension objects on the app (``BaseCoreClient.conformance_classes`` is base +
each registered extension, and its landing page adds the queryables link only
``if self.extension_is_enabled("FilterExtension")``); here the "registration"
is the loaded file itself.

The URI list is unchanged from the audited one in ``docs/goat-catalog-api.md``
§2 -- this only regroups it by the capability each URI belongs to.
"""

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from catalog.services.registry import QueryableRegistry

_SCHEMA_JSON = "application/schema+json"
_JSON = "application/json"


@dataclass(frozen=True)
class Link:
    """A landing-page link a capability contributes."""

    rel: str
    path: str
    type: str = _JSON

    def to_dict(self, stac_base: str) -> dict[str, Any]:
        return {"rel": self.rel, "type": self.type, "href": f"{stac_base}{self.path}"}


@dataclass(frozen=True)
class Capability:
    """One advertised capability of the API.

    ``available`` answers "can the currently loaded catalog actually serve
    this?". It defaults to yes: a capability that depends on nothing but the
    code being deployed (STAC core, OGC API - Features, Item Search) is always
    available, and only the data-dependent ones need a predicate.
    """

    name: str
    conformance: tuple[str, ...]
    links: tuple[Link, ...] = ()
    available: Callable[[QueryableRegistry], bool] = field(
        default=lambda registry: True
    )


CAPABILITIES: tuple[Capability, ...] = (
    Capability(
        name="core",
        conformance=(
            "https://api.stacspec.org/v1.0.0/core",
            "https://api.stacspec.org/v1.0.0/collections",
            "https://api.stacspec.org/v1.0.0/ogcapi-features",
            "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/core",
            "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/oas30",
            "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/geojson",
        ),
    ),
    Capability(
        name="item-search",
        conformance=("https://api.stacspec.org/v1.0.0/item-search",),
    ),
    Capability(
        name="collection-search",
        conformance=(
            "https://api.stacspec.org/v1.0.0/collection-search",
            "http://www.opengis.net/spec/ogcapi-common-2/1.0/conf/simple-query",
        ),
    ),
    Capability(
        name="free-text",
        conformance=(
            "https://api.stacspec.org/v1.0.0/collection-search#free-text",
            "https://api.stacspec.org/v1.0.0-rc.1/item-search#free-text",
        ),
    ),
    Capability(
        name="filter",
        conformance=(
            "https://api.stacspec.org/v1.0.0/item-search#filter",
            "http://www.opengis.net/spec/ogcapi-features-3/1.0/conf/filter",
            "http://www.opengis.net/spec/cql2/1.0/conf/basic-cql2",
            "http://www.opengis.net/spec/cql2/1.0/conf/cql2-text",
            "http://www.opengis.net/spec/cql2/1.0/conf/cql2-json",
        ),
        links=(
            Link(
                rel="http://www.opengis.net/def/rel/ogc/1.0/queryables",
                path="/queryables",
                type=_SCHEMA_JSON,
            ),
        ),
        # A filter can only be honoured against properties the loaded file
        # has; with no queryables at all, every filter would be a 400.
        available=lambda registry: len(registry) > 0,
    ),
    Capability(
        name="sort",
        conformance=(
            "https://api.stacspec.org/v1.1.0/item-search#sort",
            "https://api.stacspec.org/v1.1.0/collection-search#sort",
        ),
        # Geometry, list and struct columns have no total order, so a file
        # carrying only those leaves nothing to sort by.
        available=lambda registry: bool(registry.sortable()),
    ),
    Capability(
        name="aggregation",
        conformance=("https://api.stacspec.org/v0.3.0/aggregation",),
        links=(
            Link(rel="aggregate", path="/aggregate"),
            Link(rel="aggregations", path="/aggregations"),
        ),
        # `total_count` needs no particular column, so this holds even when
        # every facet column is gone.
    ),
)


def enabled(registry: QueryableRegistry) -> tuple[Capability, ...]:
    """The capabilities the currently loaded catalog can actually serve."""
    return tuple(cap for cap in CAPABILITIES if cap.available(registry))


def conformance_classes(registry: QueryableRegistry) -> list[str]:
    """``conformsTo`` for this catalog: the URIs of its enabled capabilities.

    Sorted and de-duplicated, so the list is stable across releases rather
    than ordered by however the capabilities happen to be declared.
    """
    uris: set[str] = set()
    for cap in enabled(registry):
        uris.update(cap.conformance)
    return sorted(uris)


def capability_links(
    registry: QueryableRegistry, stac_base: str
) -> list[dict[str, Any]]:
    """The landing-page links contributed by the enabled capabilities."""
    return [link.to_dict(stac_base) for cap in enabled(registry) for link in cap.links]
