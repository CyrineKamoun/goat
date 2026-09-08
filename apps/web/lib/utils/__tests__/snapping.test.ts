import { describe, expect, it } from "vitest";

import {
  MAX_SNAP_DISTANCE_M,
  clampToleranceToGround,
  findSnapTarget,
  snapLineEndpoints,
} from "@/lib/utils/snapping";

const line = (coordinates: [number, number][]): GeoJSON.Feature => ({
  type: "Feature",
  properties: {},
  geometry: { type: "LineString", coordinates },
});

const point = (coordinates: [number, number]): GeoJSON.Feature => ({
  type: "Feature",
  properties: {},
  geometry: { type: "Point", coordinates },
});

const ROAD = line([
  [0, 0],
  [10, 0],
]);
const TOL = 1;

describe("findSnapTarget", () => {
  // The precedence mirrors the server's — a vertex beats a line interior, as
  // reusing a node beats splitting an edge — so the indicator predicts the save.
  const cases: {
    name: string;
    point: [number, number];
    candidates: GeoJSON.Feature[];
    expected: ReturnType<typeof findSnapTarget>;
  }[] = [
    { name: "a line's own end is a vertex", point: [0.4, 0], candidates: [ROAD], expected: { position: [0, 0], kind: "vertex" } },
    { name: "a standalone point is a vertex", point: [5, 0.5], candidates: [point([5, 0])], expected: { position: [5, 0], kind: "vertex" } },
    { name: "a line interior is an edge hit", point: [5, 0.5], candidates: [ROAD], expected: { position: [5, 0], kind: "edge" } },
    { name: "a vertex wins over an equally close interior", point: [5, 0.5], candidates: [ROAD, point([5, 0.4])], expected: { position: [5, 0.4], kind: "vertex" } },
    { name: "nothing within tolerance", point: [5, 50], candidates: [ROAD], expected: null },
    {
      name: "the nearer of two lines",
      point: [5, 0],
      candidates: [line([[0, 5], [10, 5]]), line([[0, 0.2], [10, 0.2]])],
      expected: { position: [5, 0.2], kind: "edge" },
    },
    // Projecting onto a zero-length segment would divide by its length.
    { name: "a zero-length line is ignored", point: [0.5, 0], candidates: [line([[9, 9], [9, 9]])], expected: null },
  ];

  it.each(cases)("$name", ({ point: p, candidates, expected }) => {
    expect(findSnapTarget(p, candidates, TOL)).toEqual(expected);
  });

  it("snaps to the closest segment of a multi-segment line", () => {
    const target = findSnapTarget([5, 4.6], [line([[0, 0], [5, 0], [5, 10]])], TOL);
    // Float arithmetic, so compared numerically.
    expect(target?.kind).toBe("edge");
    expect(target?.position[0]).toBeCloseTo(5, 10);
    expect(target?.position[1]).toBeCloseTo(4.6, 10);
  });
});

describe("snapLineEndpoints", () => {
  it("moves only the ends, and only those with a target", () => {
    // The middle of the line is the user's chosen route; the server resolves
    // endpoints alone.
    const result = snapLineEndpoints(
      [
        [0.3, 0],
        [3, 3],
        [40, 40],
      ],
      [ROAD],
      TOL
    );
    expect(result.coordinates[0]).toEqual([0, 0]);
    expect(result.coordinates[1]).toEqual([3, 3]);
    expect(result.coordinates[2]).toEqual([40, 40]);
    expect(result.snapped).toBe(1);
  });

  it("snaps both ends when both have targets", () => {
    const result = snapLineEndpoints(
      [
        [0.3, 0],
        [9.8, 0.1],
      ],
      [ROAD],
      TOL
    );
    expect(result.coordinates).toEqual([
      [0, 0],
      [10, 0],
    ]);
    expect(result.snapped).toBe(2);
  });

  it("leaves a line with fewer than two points alone", () => {
    expect(snapLineEndpoints([[0.3, 0]], [ROAD], TOL)).toEqual({
      coordinates: [[0.3, 0]],
      snapped: 0,
    });
  });
});

describe("clampToleranceToGround", () => {
  const LAT = 48.26;
  const metresPerDegree = 111320 * Math.cos((LAT * Math.PI) / 180);
  const degreesFor = (metres: number) => metres / metresPerDegree;

  it("leaves a close-zoom tolerance alone", () => {
    // ~9.5 m, what 12 px covers at z16 — already inside the ceiling.
    const asked = degreesFor(9.5);
    expect(clampToleranceToGround(asked, LAT)).toBeCloseTo(asked, 12);
  });

  it("caps a zoomed-out tolerance at the ground limit", () => {
    // ~38 m, what 12 px covers at z14: without the cap a cursor would grab
    // across the street.
    const capped = clampToleranceToGround(degreesFor(38), LAT);
    expect(capped * metresPerDegree).toBeCloseTo(MAX_SNAP_DISTANCE_M, 6);
  });

  it("converts the ceiling per latitude, not per degree", () => {
    // A degree of longitude is shorter further from the equator, so the same
    // ground limit is a larger angle there.
    const north = clampToleranceToGround(1, 60);
    const equator = clampToleranceToGround(1, 0);
    expect(north).toBeGreaterThan(equator);
  });
});
