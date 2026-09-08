"""Real published documents must survive JSON -> columns -> JSON.

``catalog.parquet``'s ``document`` column is a *cache* of a transformation: the
converter folds the published columns into STAC JSON at sync time, and the
service parses that JSON, rewrites links and re-serialises it per request. This
module asserts the precondition for ever removing that cache -- that the
published columns carry enough to rebuild the document -- using the real
harvester output captured in ``tests/fixtures/real``.

It is also the regression guard for the converter's tolerance of published
schema drift: the fixtures here carry a ``summaries`` struct with no ``updated``
member, which used to abort the whole mirror build with a bind error.
"""

import json
import re
from pathlib import Path
from typing import Any

import duckdb
import pytest
from goatlib.tasks.catalog_mirror import build_mirror

from catalog.services.stac_build import collection_from_row, item_from_row

REAL = Path(__file__).parent / "fixtures" / "real"

#: STAC Item members that are NOT properties; everything else on an item is a
#: ``properties.*`` member. Mirrors the converter's own ``_ITEM_STRUCTURAL``.
_STRUCTURAL = {
    "id",
    "stac_version",
    "stac_extensions",
    "collection",
    "bbox",
    "assets",
    "links",
}
_TIMESTAMPS = ("datetime", "created", "updated")


def _norm_timestamp(value: Any) -> Any:
    """``2026-04-27Z`` -> ``2026-04-27T00:00:00Z``.

    The captured JSON tree carries date-with-Z values, which are not RFC 3339
    and cannot be a timestamp column. The published *parquet* has real
    timestamps, so normalising here reproduces the parquet the converter
    actually reads rather than testing a shape that never reaches it.
    """
    if isinstance(value, str) and re.fullmatch(r"\d{4}-\d{2}-\d{2}Z", value):
        return f"{value[:-1]}T00:00:00Z"
    return value


def _hoist(doc: dict[str, Any]) -> dict[str, Any]:
    """A STAC document -> the flat row shape stac-geoparquet publishes."""
    if doc.get("type") == "Collection":
        # A Collection has no `properties`: its fields are already top level.
        return {k: _norm_timestamp(v) for k, v in doc.items() if k != "type"}
    row: dict[str, Any] = {k: v for k, v in doc.items() if k in _STRUCTURAL}
    if doc.get("bbox"):
        w, s, e, n = doc["bbox"][:4]
        row["bbox"] = {"xmin": w, "ymin": s, "xmax": e, "ymax": n}
    if doc.get("geometry") is not None:
        row["__geom"] = json.dumps(doc["geometry"])
    for key, value in doc.get("properties", {}).items():
        row[key] = _norm_timestamp(value)
    return row


def _publish(
    con: duckdb.DuckDBPyConnection,
    docs: list[dict[str, Any]],
    out: Path,
    name: str,
    tmp_path: Path,
) -> None:
    """Write ``docs`` as a published-shaped parquet.

    Built via ``read_json_auto`` so DuckDB infers the STRUCT/LIST/VARCHAR shapes
    the harvester publishes, rather than hand-written casts that could quietly
    differ from the real file.
    """
    nd = tmp_path / f"{name}.ndjson"
    nd.write_text("\n".join(json.dumps(_hoist(d)) for d in docs))
    con.execute(
        f"CREATE OR REPLACE TABLE {name} AS SELECT * FROM read_json_auto('{nd}')"
    )
    cols = [row[0] for row in con.execute(f"DESCRIBE {name}").fetchall()]
    geometry = "ST_GeomFromGeoJSON(__geom) AS geometry," if "__geom" in cols else ""
    kept = [f'"{c}"' for c in cols if c not in _TIMESTAMPS and c != "__geom"]
    stamps = [f'"{c}"::TIMESTAMPTZ AS "{c}"' for c in cols if c in _TIMESTAMPS]
    con.execute(
        f"COPY (SELECT {geometry} {', '.join(kept + stamps)} FROM {name}) "
        f"TO '{out.as_posix()}' (FORMAT PARQUET)"
    )


def _norm_nested_timestamps(value: Any) -> Any:
    """``2026-07-28 09:56:45`` -> ``2026-07-28T09:56:45Z`` inside passthrough structs.

    The converter formats the *promoted* timestamp columns as RFC 3339, but
    columns it passes through whole (``extent``) are rendered by DuckDB's
    ``to_json``, which prints a TIMESTAMP as ``YYYY-MM-DD HH:MM:SS`` -- not
    RFC 3339. Here that is a harness artifact: ``read_json_auto`` infers
    ``extent.temporal.interval`` as a timestamp, whereas the published file
    types it as text, which is why production output is correct today.

    It is a latent risk rather than a live bug, and worth stating: if the
    harvester ever publishes nested temporal values as real timestamps, every
    served Collection would carry a non-RFC-3339 ``extent.temporal.interval``.
    """
    if isinstance(value, dict):
        return {k: _norm_nested_timestamps(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_norm_nested_timestamps(v) for v in value]
    if isinstance(value, str) and re.fullmatch(
        r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", value
    ):
        return value.replace(" ", "T") + "Z"
    return value


def _strip_nulls(value: Any) -> Any:
    """Drop null members, which parquet STRUCTs pad in and JSON did not have.

    A published ``table:columns`` entry is ``{"name": "OGC_FID"}``; as a parquet
    STRUCT every row carries every member of the unified type, so the value
    round-trips as ``{"name": "OGC_FID", "description": null, "type": null}``.
    That padding is already in what the service serves today (a real item has 30
    null members out of 57 in ``table:columns``), so it is tolerated here rather
    than treated as data loss -- but it is the reason a Python-side rebuild
    could serve *smaller* documents than the cached ones.
    """
    if isinstance(value, dict):
        return {k: _strip_nulls(v) for k, v in value.items() if v is not None}
    if isinstance(value, list):
        return [_strip_nulls(v) for v in value]
    return value


@pytest.fixture()
def rebuilt(tmp_path: Path) -> dict[str, dict[str, Any]]:
    """Every captured real document, run through the real converter."""
    items = [
        json.loads((REAL / f).read_text())
        for f in (
            "bundle_item_1.json",
            "bundle_item_2.json",
            "bundle_item_3.json",
            "single_item.json",
        )
    ]
    collections = [
        json.loads((REAL / f).read_text())
        for f in ("bundle_collection.json", "single_collection.json")
    ]

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; INSTALL json; LOAD json;")
    items_path, collections_path = (
        tmp_path / "items.parquet",
        tmp_path / "colls.parquet",
    )
    _publish(con, items, items_path, "items_pub", tmp_path)
    _publish(con, collections, collections_path, "colls_pub", tmp_path)

    out_items = tmp_path / "mirror_items.parquet"
    out_collections = tmp_path / "mirror_collections.parquet"
    build_mirror(items_path, collections_path, out_items, out_collections, con)

    rebuilt: dict[str, dict[str, Any]] = {}
    for path, assemble in (
        (out_items, item_from_row),
        (out_collections, collection_from_row),
    ):
        result = con.execute(
            f"SELECT * REPLACE (ST_AsGeoJSON(geometry) AS geometry) "
            f"FROM read_parquet('{path.as_posix()}')"
        )
        names = [d[0] for d in result.description]
        for values in result.fetchall():
            doc = assemble(dict(zip(names, values, strict=True)))
            rebuilt[doc["id"]] = doc
    con.close()
    return rebuilt


def _originals() -> list[dict[str, Any]]:
    return [
        json.loads((REAL / f).read_text())
        for f in (
            "bundle_item_1.json",
            "bundle_item_2.json",
            "bundle_item_3.json",
            "single_item.json",
            "bundle_collection.json",
            "single_collection.json",
        )
    ]


#: Properties the mirror *adds* to an item on purpose, so the rebuilt document
#: states them where the published item did not:
#:
#: * ``goat:layerType`` is published on the Collection and denormalised onto
#:   every item so a search can facet by it.
#: * ``goat:member_count`` is how many layers share the item's bundle --
#:   precomputed at build time, and the number a dataset card exists to
#:   show ("74 layers").
#: * ``goat:publisher`` is denormalised from the Collection's ``providers``, so
#:   a card can name the publisher without fetching the parent.
_ADDED_PROPERTIES = {"goat:layerType", "goat:member_count", "goat:publisher"}

#: Added to a **Collection** only: its members' geometry type, where they all
#: agree on one. A Collection publishes none of its own (it is a property of a
#: layer, and the published ``summaries`` carry only ``updated``), and a dataset
#: card needs to know what shape the data is.
#:
#: Kept separate from ``_ADDED_PROPERTIES`` on purpose. That set is discounted at
#: both levels, so putting this in it would also stop checking that an *Item's*
#: published ``goat:geometryType`` survives the round trip -- which it must, and
#: which is what the texture on 10,685 layer cards is drawn from.
_ADDED_COLLECTION_MEMBERS = {"goat:geometryType"}

#: `goat:style` carries a heterogeneous nested array (``[[["label"], "#rgb"]]``)
#: which parquet cannot type, so ``read_json_auto`` in this harness turns the
#: inner values into JSON strings. The real published ``items.parquet`` has no
#: ``goat:style`` column at all, so this is a harness artifact -- but it is the
#: shape to watch if the harvester ever starts publishing it.
_UNTYPEABLE_PROPERTIES = {"goat:style"}


@pytest.mark.parametrize("original", _originals(), ids=lambda d: d["id"][:32])
def test_real_document_survives_the_column_round_trip(
    original: dict[str, Any], rebuilt: dict[str, dict[str, Any]]
) -> None:
    """Rebuilding from published columns reproduces the published document."""
    got = rebuilt.get(original["id"])
    assert got is not None, f"{original['id']} missing from the mirror"

    expected = _strip_nulls(
        {k: _norm_timestamp(v) if k in _TIMESTAMPS else v for k, v in original.items()}
    )
    if "properties" in expected:
        expected["properties"] = _strip_nulls(
            {k: _norm_timestamp(v) for k, v in original["properties"].items()}
        )
    rebuilt = _norm_nested_timestamps(_strip_nulls(got))
    # An Item carries these under `properties`; a Collection has no
    # `properties`, so the same derived members sit at its top level.
    for container in (
        rebuilt,
        expected,
        rebuilt.get("properties"),
        expected.get("properties"),
    ):
        if isinstance(container, dict):
            for name in _ADDED_PROPERTIES | _UNTYPEABLE_PROPERTIES:
                container.pop(name, None)
    # Top level only, so an Item's own `properties.goat:geometryType` stays under
    # assertion.
    if "properties" not in rebuilt:
        for name in _ADDED_COLLECTION_MEMBERS:
            rebuilt.pop(name, None)
            expected.pop(name, None)
    assert rebuilt == expected


def test_summaries_without_updated_does_not_break_the_build(
    rebuilt: dict[str, dict[str, Any]],
) -> None:
    """Published STRUCTs whose members differ must degrade to NULL, not abort.

    These fixtures' collections carry ``summaries`` with no ``updated`` member.
    The converter reads ``summaries.updated.maximum``, which is a *bind* error
    rather than a NULL, so before the expression prober this took the entire
    mirror build down -- the catalog would simply stop updating.
    """
    collections = [d for d in rebuilt.values() if d.get("type") == "Collection"]
    assert collections, "the fixture collections should be in the mirror"
