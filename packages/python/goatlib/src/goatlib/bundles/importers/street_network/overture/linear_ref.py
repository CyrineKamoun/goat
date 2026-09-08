"""Geodetic linear referencing along an Overture segment geometry.

Overture defines a linear reference as a fraction of the segment's *geodetic*
length on the WGS84 ellipsoid:

    lr = geodetic_distance_along_segment_from_start / total_geodetic_length

Planar distance over lon/lat treated as x/y is explicitly not the definition —
it underestimates an east-west length by roughly half at 60°N — so every measure
here goes through ``pyproj.Geod``, matching Overture's reference implementation.
"""

from typing import List, Optional, Sequence, Tuple

from pyproj import Geod

Coord = Tuple[float, float]

_GEOD = Geod(ellps="WGS84")


def cumulative_lengths(coords: Sequence[Coord]) -> List[float]:
    """Geodetic distance from the first vertex to each vertex, in metres."""
    if len(coords) < 2:
        return [0.0] * len(coords)
    lons = [c[0] for c in coords]
    lats = [c[1] for c in coords]
    # line_lengths returns the per-leg distances, one fewer than the vertex count.
    legs = _GEOD.line_lengths(lons, lats)
    cumulative = [0.0]
    for leg in legs:
        cumulative.append(cumulative[-1] + leg)
    return cumulative


def total_length(coords: Sequence[Coord]) -> float:
    """Geodetic length of a linestring in metres."""
    cum = cumulative_lengths(coords)
    return cum[-1] if cum else 0.0


def interpolate(
    coords: Sequence[Coord],
    fraction: float,
    cumulative: Optional[Sequence[float]] = None,
) -> Coord:
    """The coordinate at ``fraction`` of the geometry's geodetic length.

    Walks to the leg containing the target distance, then steps the remainder
    along that leg's geodesic — rather than interpolating lon/lat linearly, which
    would drift from the ellipsoid the fractions are defined on.

    ``cumulative`` lets a caller that already measured the geometry pass it in;
    every measure is a pyproj call, and splitting interpolates the same parent
    repeatedly.
    """
    if not coords:
        raise ValueError("Cannot interpolate an empty geometry")
    if len(coords) == 1:
        return coords[0]

    cum = list(cumulative) if cumulative is not None else cumulative_lengths(coords)
    length = cum[-1]
    if length == 0.0:
        return coords[0]

    target = _clamp(fraction, 0.0, 1.0) * length
    # Exact endpoints must return the original vertices untouched, so that a
    # split at 0 or 1 reproduces the parent geometry bit-for-bit.
    if target <= 0.0:
        return coords[0]
    if target >= length:
        return coords[-1]

    index = _leg_index(cum, target)
    lon1, lat1 = coords[index]
    lon2, lat2 = coords[index + 1]
    remainder = target - cum[index]
    if remainder <= 0.0:
        return coords[index]

    azimuth, _, _ = _GEOD.inv(lon1, lat1, lon2, lat2)
    lon, lat, _ = _GEOD.fwd(lon1, lat1, azimuth, remainder)
    return (lon, lat)


def substring(coords: Sequence[Coord], start: float, end: float) -> List[Coord]:
    """The portion of the geometry between two linear references.

    Interior vertices strictly inside the range are kept, so the piece follows
    the parent's shape; the cut points are interpolated onto the geodesic.
    """
    if end < start:
        raise ValueError(f"end ({end}) precedes start ({start})")
    if not coords:
        raise ValueError("Cannot cut an empty geometry")

    cum = cumulative_lengths(coords)
    length = cum[-1]
    if length == 0.0:
        return [coords[0], coords[-1]]

    start = _clamp(start, 0.0, 1.0)
    end = _clamp(end, 0.0, 1.0)
    start_m = start * length
    end_m = end * length

    # Measured once and reused for both cuts.
    result: List[Coord] = [interpolate(coords, start, cum)]
    for vertex, distance in zip(coords, cum):
        if start_m < distance < end_m:
            result.append(vertex)
    result.append(interpolate(coords, end, cum))

    # A cut point landing exactly on a kept vertex would duplicate it.
    return _dedupe_consecutive(result)


def _leg_index(cumulative: Sequence[float], target: float) -> int:
    """Index of the leg start whose leg contains ``target``."""
    for i in range(len(cumulative) - 1):
        if cumulative[i] <= target <= cumulative[i + 1]:
            return i
    return len(cumulative) - 2


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _dedupe_consecutive(coords: List[Coord], tolerance: float = 1e-12) -> List[Coord]:
    out = [coords[0]]
    for coord in coords[1:]:
        prev = out[-1]
        if abs(coord[0] - prev[0]) > tolerance or abs(coord[1] - prev[1]) > tolerance:
            out.append(coord)
    # A degenerate range still has to be a valid two-point linestring.
    if len(out) == 1:
        out.append(out[0])
    return out
