"""Read Overture transportation GeoParquet into plain records.

The splitter works on dicts with decoded coordinates rather than Arrow structs:
the networks are small, and nested-struct manipulation in Arrow is far harder to
read than the dict form. This module is the boundary where WKB becomes
coordinates and back.
"""

import logging
from typing import Any, Dict, List, Tuple

import pyarrow.parquet as pq
from shapely import wkb
from shapely.geometry import LineString, Point

logger = logging.getLogger(__name__)

Coord = Tuple[float, float]

# Overture's geometry column name in the official distribution.
GEOMETRY_COLUMN = "geometry"

# Only road segments are routable; rail and water share the segment type.
ROUTABLE_SUBTYPES = frozenset({"road"})


class OvertureReadError(Exception):
    """Raised when a file is not a usable Overture transportation extract."""


def read_segments(path: str) -> List[Dict[str, Any]]:
    """Read segments, decoding geometry to a ``coordinates`` list.

    Non-road subtypes are dropped — they are part of the transportation theme but
    are not routable, and carrying them would put unusable rows in the layer.
    """
    table = pq.read_table(path)
    _require_columns(path, table.column_names, ("id", GEOMETRY_COLUMN, "connectors"))

    records: List[Dict[str, Any]] = []
    skipped_subtype = 0
    for record in table.to_pylist():
        subtype = record.get("subtype")
        if subtype is not None and subtype not in ROUTABLE_SUBTYPES:
            skipped_subtype += 1
            continue
        geometry = _decode(record.pop(GEOMETRY_COLUMN, None))
        if not isinstance(geometry, LineString):
            raise OvertureReadError(
                f"Segment {record.get('id')} geometry is "
                f"{type(geometry).__name__}, expected LineString"
            )
        record["coordinates"] = [(x, y) for x, y in geometry.coords]
        records.append(record)

    if skipped_subtype:
        logger.info("Skipped %d non-road segment(s)", skipped_subtype)
    return records


def read_connectors(path: str) -> List[Dict[str, Any]]:
    """Read connectors, decoding geometry to a single ``coordinate`` tuple."""
    table = pq.read_table(path)
    _require_columns(path, table.column_names, ("id", GEOMETRY_COLUMN))

    records: List[Dict[str, Any]] = []
    for record in table.to_pylist():
        geometry = _decode(record.pop(GEOMETRY_COLUMN, None))
        if not isinstance(geometry, Point):
            raise OvertureReadError(
                f"Connector {record.get('id')} geometry is "
                f"{type(geometry).__name__}, expected Point"
            )
        record["coordinate"] = (geometry.x, geometry.y)
        records.append(record)
    return records


def _decode(value: Any) -> Any:
    if value is None:
        raise OvertureReadError("Record has no geometry")
    if isinstance(value, (bytes, bytearray, memoryview)):
        return wkb.loads(bytes(value))
    raise OvertureReadError(
        f"Geometry column holds {type(value).__name__}, expected WKB bytes"
    )


def _require_columns(path: str, present: List[str], required: Tuple[str, ...]) -> None:
    missing = [c for c in required if c not in present]
    if missing:
        raise OvertureReadError(
            f"{path} is missing required column(s): {', '.join(missing)}"
        )
