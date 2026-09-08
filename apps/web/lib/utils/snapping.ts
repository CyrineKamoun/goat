/**
 * Where a drawn endpoint should land, given what is already on the map.
 *
 * Purely an editing aid: the server re-derives the topology on save and is the
 * authority. Its precedence is mirrored here on purpose — a vertex beats a line
 * interior, exactly as reusing a node beats splitting an edge — so what the user
 * sees while drawing is what the save will do.
 *
 * Distances are in the coordinate units of the geometry handed in (degrees for
 * 4326). The caller converts a pixel radius into that unit, because only the map
 * knows the current scale.
 */

/**
 * The furthest an endpoint may be pulled, in ground metres.
 *
 * A pixel radius is the right feel at close zoom but grows with the scale: 12 px
 * is about 10 m of ground at z16 and 38 m by z14. Without a ceiling, zooming out
 * turns snapping from "connect to what I am pointing at" into "connect to
 * whatever is on this block".
 */
export const MAX_SNAP_DISTANCE_M = 10;

export type SnapKind = "vertex" | "edge";

export interface SnapTarget {
  position: [number, number];
  kind: SnapKind;
}

type Position = [number, number];

const distance = (a: Position, b: Position): number =>
  Math.hypot(a[0] - b[0], a[1] - b[1]);

/** Closest point to `p` on the segment `a`–`b`, and how far away it is. */
function nearestOnSegment(
  p: Position,
  a: Position,
  b: Position
): { position: Position; distance: number } | null {
  const dx = b[0] - a[0];
  const dy = b[1] - a[1];
  const lengthSquared = dx * dx + dy * dy;
  // A zero-length segment has no interior to project onto, and dividing by its
  // length would produce NaN and decide the comparison by accident.
  if (lengthSquared === 0) return null;
  let t = ((p[0] - a[0]) * dx + (p[1] - a[1]) * dy) / lengthSquared;
  t = Math.max(0, Math.min(1, t));
  const position: Position = [a[0] + t * dx, a[1] + t * dy];
  return { position, distance: distance(p, position) };
}

function* lineStrings(feature: GeoJSON.Feature): Generator<Position[]> {
  const geometry = feature.geometry;
  if (!geometry) return;
  if (geometry.type === "LineString") {
    yield geometry.coordinates as Position[];
  } else if (geometry.type === "MultiLineString") {
    for (const part of geometry.coordinates) yield part as Position[];
  }
}

function* vertices(feature: GeoJSON.Feature): Generator<Position> {
  const geometry = feature.geometry;
  if (!geometry) return;
  if (geometry.type === "Point") {
    yield geometry.coordinates as Position;
    return;
  }
  if (geometry.type === "MultiPoint") {
    for (const coordinate of geometry.coordinates) yield coordinate as Position;
    return;
  }
  for (const line of lineStrings(feature)) {
    // Only the ends: a mid-line vertex is not a node in the network sense, so
    // snapping to it would connect to something the server would treat as a
    // split anyway.
    if (line.length) {
      yield line[0];
      yield line[line.length - 1];
    }
  }
}

export function findSnapTarget(
  point: Position,
  candidates: GeoJSON.Feature[],
  tolerance: number
): SnapTarget | null {
  let bestVertex: { position: Position; distance: number } | null = null;
  for (const feature of candidates) {
    for (const vertex of vertices(feature)) {
      const d = distance(point, vertex);
      if (d <= tolerance && (!bestVertex || d < bestVertex.distance)) {
        bestVertex = { position: vertex, distance: d };
      }
    }
  }
  if (bestVertex) return { position: bestVertex.position, kind: "vertex" };

  let bestEdge: { position: Position; distance: number } | null = null;
  for (const feature of candidates) {
    for (const line of lineStrings(feature)) {
      for (let i = 0; i + 1 < line.length; i++) {
        const hit = nearestOnSegment(point, line[i], line[i + 1]);
        if (!hit) continue;
        if (hit.distance <= tolerance && (!bestEdge || hit.distance < bestEdge.distance)) {
          bestEdge = hit;
        }
      }
    }
  }
  if (bestEdge) return { position: bestEdge.position, kind: "edge" };

  return null;
}

export interface SnappedLine {
  coordinates: Position[];
  snapped: number;
}

/**
 * Move a drawn line's first and last vertex onto their snap targets.
 *
 * Only the ends: the middle of an edge is the user's chosen route, and the
 * server only resolves endpoints.
 */
export function snapLineEndpoints(
  coordinates: Position[],
  candidates: GeoJSON.Feature[],
  tolerance: number
): SnappedLine {
  if (coordinates.length < 2) return { coordinates, snapped: 0 };
  const result = coordinates.map((c) => [...c] as Position);
  let snapped = 0;
  for (const index of [0, result.length - 1]) {
    const target = findSnapTarget(result[index], candidates, tolerance);
    if (target) {
      result[index] = target.position;
      snapped += 1;
    }
  }
  return { coordinates: result, snapped };
}


/**
 * The snap tolerance in degrees, clamped so it never exceeds a ground distance.
 *
 * Takes the tolerance a pixel radius implies (which only the map can measure)
 * and caps it. At close zoom the pixel radius is already inside the ceiling and
 * nothing changes; zoomed out, the radius stops growing and the user simply has
 * to be geometrically closer.
 */
export function clampToleranceToGround(
  degreesFromPixels: number,
  latitude: number,
  maxMetres: number = MAX_SNAP_DISTANCE_M
): number {
  const metresPerDegree = 111320 * Math.cos((latitude * Math.PI) / 180);
  if (metresPerDegree <= 0) return degreesFromPixels;
  const maxDegrees = maxMetres / metresPerDegree;
  return Math.min(degreesFromPixels, maxDegrees);
}
