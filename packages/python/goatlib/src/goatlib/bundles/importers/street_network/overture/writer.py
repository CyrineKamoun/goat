"""Write flattened records as typed GeoParquet member layers.

The schema is declared rather than inferred. Handing the runner GeoJSON instead
would let column types follow the data: a network with no ``level_rules`` yields
an all-null column, which infers as VARCHAR, while a network that has them yields
INTEGER — so two imports of the same bundle type would disagree on their layer
schema, and anything reading the layer (the routing artifact, CQL2 filters) would
have to cope with both.

Geometry goes through DuckDB so the output carries GeoParquet metadata — the
runner's ingest detects the geometry column by DuckDB type, and a plain WKB blob
would not be recognised. Writing via ``write_optimized_parquet`` also picks up the
bbox struct, Hilbert ordering and Parquet V2 that every other layer gets.
"""

import logging
from typing import Any, Dict, List, Sequence

import duckdb
import pyarrow as pa
from shapely.geometry import LineString, Point

from goatlib.computed_columns import COMPUTED_KIND_REGISTRY
from goatlib.io.parquet import write_optimized_parquet

logger = logging.getLogger(__name__)

# WKB on the way in; DuckDB turns it into a real geometry column on write.
_GEOMETRY = ("geometry", pa.binary())

EDGE_SCHEMA = pa.schema(
    [
        ("id", pa.string()),
        ("name", pa.string()),
        ("class", pa.string()),
        ("subclass", pa.string()),
        # float64, not float32: the value is written by the `length` computed
        # kind's own SQL, which is DOUBLE, and `SELECT * REPLACE (<expr> AS
        # length_m)` takes the expression's type — so a narrower declaration
        # here would simply be false about the file. Widening the declaration
        # rather than narrowing the expression, because DOUBLE is also what the
        # routing artifact reads the column as and what a later recompute
        # produces: one width the whole way through, and no rounding step that
        # would make the length a user sees differ from the length the engine
        # routes on.
        ("length_m", pa.float64()),
        ("surface", pa.string()),
        ("speed_limit_kph_forward", pa.int32()),
        ("speed_limit_kph_backward", pa.int32()),
        ("source_node", pa.string()),
        ("target_node", pa.string()),
        ("other", pa.string()),
        ("original_id", pa.string()),
        _GEOMETRY,
    ]
)

NODE_SCHEMA = pa.schema(
    [
        ("id", pa.string()),
        ("is_synthetic", pa.bool_()),
        _GEOMETRY,
    ]
)


#: Edge columns filled from the geometry at write time rather than carried on the
#: flattened record. The routing artifact reads `length_m` from the layer instead
#: of deriving its own, so a layer written without it cannot be built from.
EDGE_COMPUTED = {"length_m": COMPUTED_KIND_REGISTRY["length"]}


def write_edges(records: Sequence[Dict[str, Any]], path: str) -> str:
    """Write edge records, converting ``coordinates`` to LineString geometry.

    ``length_m`` is filled here rather than left to the import: the routing
    artifact reads the column instead of deriving its own, so a layer written
    without it cannot be built from. The expression is the computed kind's own,
    so the value is identical to what a later recompute produces.
    """
    return _write(
        records,
        path,
        EDGE_SCHEMA,
        "coordinates",
        _line_wkb,
        computed=EDGE_COMPUTED,
    )


def write_nodes(records: Sequence[Dict[str, Any]], path: str) -> str:
    """Write node records, converting ``coordinate`` to Point geometry."""
    return _write(records, path, NODE_SCHEMA, "coordinate", _point_wkb)


def _write(
    records: Sequence[Dict[str, Any]],
    path: str,
    schema: pa.Schema,
    geometry_key: str,
    to_wkb: Any,
    computed: Dict[str, Any] | None = None,
) -> str:
    rows: List[Dict[str, Any]] = []
    for record in records:
        geometry = record.get(geometry_key)
        if geometry is None:
            continue
        row = {name: record.get(name) for name in schema.names if name != "geometry"}
        row["geometry"] = to_wkb(geometry)
        rows.append(row)

    table = pa.Table.from_pylist(rows, schema=schema)
    con = duckdb.connect()
    try:
        con.execute("INSTALL spatial; LOAD spatial")
        con.register("records", table)
        # Same optimiser every other layer goes through — bbox struct for
        # row-group pruning, Hilbert ordering, Parquet V2 — so a bundle's layers
        # aren't second-class for tile and feature queries.
        #
        # REPLACE keeps the declared column *order*. It does not keep types: a
        # replaced column takes the type of the expression that replaces it,
        # which is why geometry comes out a geometry and why a computed column
        # comes out at its kind's own width. The declared schema has to agree
        # with those expressions — `test_writer_schema_types_match_the_file`
        # is what holds it — since the declaration is what anything reading the
        # layer trusts.
        #
        # Geometry first, then anything computed from it: a computed expression
        # needs a real geometry, not the WKB the records carry.
        geom_query = (
            "SELECT * REPLACE (ST_GeomFromWKB(geometry) AS geometry) FROM records"
        )
        if computed:
            fills = ", ".join(
                f"{kind.compute_sql()} AS {column}" for column, kind in computed.items()
            )
            query = f"SELECT * REPLACE ({fills}) FROM ({geom_query})"
        else:
            query = geom_query
        write_optimized_parquet(
            con,
            query,
            path,
            geometry_column="geometry",
        )
    finally:
        con.close()
    logger.debug("Wrote %d row(s) to %s", len(rows), path)
    return path


def _line_wkb(coordinates: Sequence[Any]) -> bytes:
    return bytes(LineString([tuple(c) for c in coordinates]).wkb)


def _point_wkb(coordinate: Sequence[float]) -> bytes:
    return bytes(Point(tuple(coordinate)).wkb)
