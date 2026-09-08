"""STAC Aggregation extension (v0.3.0) support: facet counts for the UI sidebar.

``run_aggregations`` reuses ``catalog.services.search.build_filters`` so every
bucket count is computed under the exact same predicates as the matching
Item Search call (``q`` terms, ``bbox``, ``datetime``, ``cql``, GOAT facet
params, ...) -- and it runs against the items relation, so collections never contribute to a
facet count.

Which facets exist is derived, not fixed: the offered aggregations are the
facetable entries of ``catalog.services.registry`` (built from the loaded
table), each named ``<column>_count``. A facet whose column is absent from the
current file is simply not offered, so a harvester schema change drops the
facet instead of producing SQL against a column that no longer exists.

Facet names are not 1:1 with the query parameter that narrows them (the
``category`` column is filtered via ``?themes=``); the registry holds both
names.
"""

from dataclasses import dataclass
from typing import Any, Literal

import duckdb
from pydantic import BaseModel, ConfigDict, Field

from catalog.errors import ApiError
from catalog.services.search import SearchParams, build_filters, safe_query
from catalog.store import CatalogState, CatalogStore

TOTAL_COUNT = "total_count"


class AggregationBucket(BaseModel):
    """One bucket of a frequency distribution."""

    key: str | None
    data_type: str = "string"
    frequency: int


class Aggregation(BaseModel):
    """One aggregation, in discovery (no value) or executed (with one) form."""

    model_config = ConfigDict(extra="allow")

    name: str
    data_type: str
    frequency_distribution_data_type: str | None = None
    value: int | None = None
    buckets: list[AggregationBucket] | None = None
    #: GOAT extension: the search parameter that narrows this facet, so a
    #: client can build a filter sidebar from discovery instead of hardcoding
    #: a facet-name-to-parameter map (they differ: `category_count` filters
    #: with `?themes=`).
    goat_filter_param: str | None = Field(default=None, alias="goat:filter_param")


class AggregationsDiscovery(BaseModel):
    """``GET /aggregations``: which aggregations this catalog offers."""

    aggregations: list[Aggregation] = Field(default_factory=list)


class AggregationCollection(BaseModel):
    """``GET /aggregate``: the executed aggregations."""

    type: Literal["AggregationCollection"] = "AggregationCollection"
    aggregations: list[Aggregation] = Field(default_factory=list)


#: Suffix that turns a facetable column into its aggregation name, as the
#: Aggregation extension's examples do (``platform`` -> ``platform_count``).
_COUNT_SUFFIX = "_count"


#: What one row of an aggregation counts.
#:
#: ``items`` counts layers, ``collections`` counts datasets. The distinction is
#: not cosmetic: the catalog holds 10,793 layers in 3,834 datasets, so a facet
#: counted over items told a page that lists datasets "8,166 bundles" when
#: selecting that bucket returns 1,207 -- the count and the result set were in
#: different units.
AggregationUnit = Literal["items", "collections"]


def facet_aggregations(
    store: CatalogStore,
    unit: AggregationUnit = "items",
    snap: CatalogState | None = None,
) -> dict[str, str]:
    """``{aggregation name: column expression}`` for the loaded table.

    Counting datasets keeps every facet the item registry offers, not just the
    ones a collection row happens to carry: ``geometry_type`` lives on layers,
    and "how many datasets have a polygon layer" is a question worth answering.
    Those become semi-joins in :func:`run_aggregations`.
    """
    state = snap or store.snapshot()
    facets = {
        f"{q.facet_name}{_COUNT_SUFFIX}": q.expr
        for q in state.registry.facets().values()
    }
    if unit == "items":
        return facets
    # Collection-level expressions win where both relations define the facet:
    # `license` is on the collection, so counting it needs no subquery.
    return {
        **facets,
        **{
            f"{q.facet_name}{_COUNT_SUFFIX}": q.expr
            for q in state.collection_registry.facets().values()
        },
    }


def facet_params(
    store: CatalogStore,
    unit: AggregationUnit = "items",
    snap: CatalogState | None = None,
) -> dict[str, str]:
    """``{aggregation name: the query parameter that narrows it}``.

    Published so a client can build a whole facet sidebar from discovery
    alone. Stripping ``_count`` off the name is *not* enough and quietly
    breaks: ``category_count`` is narrowed with ``?themes=``, and
    ``language_count`` with ``?language=`` over a ``language_code`` column.
    Without this, every consumer hardcodes that map and drifts from the
    server the first time a facet is added.
    """
    state = snap or store.snapshot()
    params = {
        f"{q.facet_name}{_COUNT_SUFFIX}": q.param
        for q in state.registry.facets().values()
    }
    if unit == "collections":
        # Counting datasets offers the collection registry's facets too (see
        # `facet_aggregations`), so a facet only a collection row carries must
        # resolve here as well, or the lookup below is a KeyError -> 500 the
        # first time the harvester's item schema drifts.
        for q in state.collection_registry.facets().values():
            params.setdefault(f"{q.facet_name}{_COUNT_SUFFIX}", q.param)
    return params


def aggregation_names(store: CatalogStore) -> list[str]:
    """Discovery/execution order: ``total_count`` first, then the facets."""
    return [TOTAL_COUNT, *facet_aggregations(store)]


def available_aggregations(
    store: CatalogStore, unit: AggregationUnit = "items"
) -> dict[str, Any]:
    """The ``/aggregations`` discovery document.

    ``unit`` is echoed through so a client discovering facets for a dataset list
    is offered the same set it can then count -- including the item-level facets
    that become semi-joins.
    """
    aggregations: list[dict[str, Any]] = [{"name": TOTAL_COUNT, "data_type": "integer"}]
    params = facet_params(store, unit)
    for name in facet_aggregations(store, unit):
        aggregations.append(
            {
                "name": name,
                "data_type": "frequency_distribution",
                "frequency_distribution_data_type": "string",
                # GOAT extension: the search parameter that narrows this
                # facet. Not in the Aggregation extension, which describes
                # what can be counted but not how to filter by it.
                "goat:filter_param": params.get(name),
            }
        )
    return {"aggregations": aggregations}


@dataclass(frozen=True)
class _GroupedCounts:
    """One scan's worth of counts: the total, and per column its value counts."""

    total: int
    buckets: dict[str, list[tuple[Any, int]]]


def _grouped_counts(
    store: CatalogStore,
    *,
    con: duckdb.DuckDBPyConnection,
    relation: str,
    where_sql: str,
    params: list[Any],
    columns: list[str],
    with_total: bool,
) -> _GroupedCounts:
    """Count every requested facet, and the total, in a single GROUPING SETS scan.

    Each grouping set contributes rows for one column; a row's set is identified
    by its per-column ``GROUPING`` flag (0 = grouped by, 1 = aggregated away)
    rather than by decoding a combined bitmask, so the mapping does not depend on
    argument order. The empty grouping set is the total, which therefore costs
    nothing extra once any facet is requested.

    NULL keys are dropped here, as the per-facet queries did: a facet bucket
    describes rows that *have* the value.
    """
    if not columns:
        if not with_total:
            return _GroupedCounts(0, {})
        rows = safe_query(
            store,
            f"SELECT count(*) FROM {relation} WHERE {where_sql}",
            params,
            con=con,
        )
        return _GroupedCounts(int(rows[0][0]) if rows else 0, {})

    keys = ", ".join(f"{c} AS key{i}" for i, c in enumerate(columns))
    flags = ", ".join(f"GROUPING({c}) AS grouped{i}" for i, c in enumerate(columns))
    sets = ", ".join(f"({c})" for c in columns)
    if with_total:
        sets = f"{sets}, ()"
    rows = safe_query(
        store,
        f"""
        SELECT {keys}, {flags}, count(*) AS frequency
        FROM {relation}
        WHERE {where_sql}
        GROUP BY GROUPING SETS ({sets})
        """,
        params,
        con=con,
    )

    width = len(columns)
    total = 0
    counts: dict[str, list[tuple[Any, int]]] = {c: [] for c in columns}
    for row in rows:
        values, grouping, frequency = row[:width], row[width : width * 2], row[-1]
        grouped_by = [i for i, flag in enumerate(grouping) if not flag]
        if not grouped_by:
            total = int(frequency)
            continue
        index = grouped_by[0]
        key = values[index]
        if key is None:
            continue
        counts[columns[index]].append((key, int(frequency)))

    for column, buckets in counts.items():
        # Ordered here rather than in SQL: one ORDER BY cannot order the sets
        # independently, and a page's worth of buckets is cheap to sort.
        buckets.sort(key=lambda bucket: (-bucket[1], str(bucket[0])))
    return _GroupedCounts(total, counts)


def run_aggregations(
    store: CatalogStore,
    p: SearchParams,
    names: list[str] | None,
    unit: AggregationUnit = "items",
) -> dict[str, Any]:
    """Execute the requested aggregations; returns an ``AggregationCollection``.

    ``names=None`` runs all known aggregations (``total_count`` + every
    facet). An unknown name anywhere in an explicit list is a 400 naming it,
    before any query runs.

    ``unit`` decides what a count counts -- see :data:`AggregationUnit`. A client
    listing datasets must ask for ``collections``, or its facet counts describe a
    different set of things than its results do.
    """
    # One generation for the whole call: which facets exist, the SQL built from
    # them and the connection it runs on must all describe the same file.
    snap = store.snapshot()
    facets = facet_aggregations(store, unit, snap)
    known = [TOTAL_COUNT, *facets]
    if names is None:
        requested = known
    else:
        for name in names:
            if name not in known:
                raise ApiError(400, f"unknown aggregation name: {name!r}")
        requested = names

    if unit == "collections":
        relation = CatalogStore.COLLECTIONS
        where_sql, params = build_filters(
            p,
            registry=snap.collection_registry,
            relation="collections",
            item_registry=snap.registry,
        )
        collection_facets = {
            f"{q.facet_name}{_COUNT_SUFFIX}"
            for q in snap.collection_registry.facets().values()
        }
    else:
        relation = CatalogStore.ITEMS
        where_sql, params = build_filters(p, registry=snap.registry)
        collection_facets = set()

    params_by_name = facet_params(store, unit, snap)

    # Everything countable in one pass over `relation`: the total and every facet
    # that groups a column of that same relation. One scan instead of N+1 -- 26 ms
    # against the 38k-row mirror where the loop took 166 ms.
    grouped = _grouped_counts(
        store,
        con=snap.con,
        relation=relation,
        where_sql=where_sql,
        params=params,
        columns=[
            facets[name]
            for name in requested
            if name != TOTAL_COUNT
            and not (unit == "collections" and name not in collection_facets)
        ],
        with_total=TOTAL_COUNT in requested,
    )

    result: list[dict[str, Any]] = []
    for name in requested:
        if name == TOTAL_COUNT:
            result.append(
                {
                    "name": TOTAL_COUNT,
                    "data_type": "integer",
                    "value": grouped.total,
                }
            )
            continue

        column = facets[name]
        if unit == "collections" and name not in collection_facets:
            # An item-level facet counted in datasets: group the layers by their
            # value and count DISTINCT collections, so a bundle with four polygon
            # layers counts once. `count(*)` here is what reported 8,166 bundles
            # for 1,207 datasets.
            rows = safe_query(
                store,
                f"""
                SELECT {column} AS key, count(DISTINCT {CatalogStore.ITEMS}.collection) AS frequency
                FROM {CatalogStore.ITEMS}
                WHERE {column} IS NOT NULL AND EXISTS (
                    SELECT 1 FROM {CatalogStore.COLLECTIONS}
                    WHERE {CatalogStore.COLLECTIONS}.id = {CatalogStore.ITEMS}.collection
                      AND {where_sql}
                )
                GROUP BY {column}
                ORDER BY frequency DESC, key ASC
                """,
                params,
                con=snap.con,
            )
            buckets = [
                {"key": key, "data_type": "string", "frequency": int(frequency)}
                for key, frequency in rows
            ]
            result.append(
                {
                    "name": name,
                    "data_type": "frequency_distribution",
                    "frequency_distribution_data_type": "string",
                    "buckets": buckets,
                    "goat:filter_param": params_by_name.get(name),
                }
            )
            continue

        buckets = [
            {"key": key, "data_type": "string", "frequency": frequency}
            for key, frequency in grouped.buckets[column]
        ]
        result.append(
            {
                "name": name,
                "data_type": "frequency_distribution",
                "buckets": buckets,
                "goat:filter_param": params_by_name.get(name),
            }
        )

    return {"type": "AggregationCollection", "aggregations": result}
