import { describe, expect, it } from "vitest";

import {
  type CatalogSpatialFilter,
  decodeSpatial,
  encodeSpatial,
  formatBuffer,
  spatialFeatures,
  spatialGeometry,
} from "@/lib/catalog/spatial";

/**
 * The spatial filter's URL codec and geometry derivation.
 *
 * Worth testing because the URL is the only state: a filter that encodes but does
 * not decode back to itself silently changes what a shared link searches, and the
 * geometry is what actually reaches the API.
 */
describe("spatial filter encoding", () => {
  it("round-trips a buffered point without losing the radius", () => {
    const filter: CatalogSpatialFilter = { kind: "point", lng: 16.3738, lat: 48.2082, km: 7.5 };
    const decoded = decodeSpatial(encodeSpatial(filter));
    expect(decoded).toEqual(filter);
  });

  it("round-trips a drawn ring", () => {
    const filter: CatalogSpatialFilter = {
      kind: "polygon",
      ring: [
        [16.2, 48.1],
        [16.4, 48.1],
        [16.4, 48.3],
      ],
    };
    expect(decodeSpatial(encodeSpatial(filter))).toEqual(filter);
  });

  it("round-trips several regions", () => {
    const filter: CatalogSpatialFilter = { kind: "region", nutsIds: ["AT12", "AT13"] };
    // Names are in-session only, so they are deliberately not round-tripped.
    expect(decodeSpatial(encodeSpatial({ ...filter, names: { AT12: "Niederösterreich" } }))).toEqual(
      filter
    );
  });

  it("clears every parameter for an empty filter", () => {
    expect(encodeSpatial(null)).toEqual({ nuts: null, pt: null, poly: null });
  });

  it("reads a hand-edited URL as unfiltered rather than throwing", () => {
    expect(decodeSpatial({ pt: "not,a,point" })).toBeNull();
    expect(decodeSpatial({ pt: "16.3" })).toBeNull();
    // Two corners cannot be an area.
    expect(decodeSpatial({ poly: "16.2 48.1;16.4 48.1" })).toBeNull();
    expect(decodeSpatial({ nuts: "" })).toBeNull();
    expect(decodeSpatial({})).toBeNull();
  });

  it("prefers the more specific shape when the URL carries several", () => {
    // Hand-edited URLs can hold more than one; the drawn shapes win over a region
    // so the result is never a silent combination of two different questions.
    const decoded = decodeSpatial({ nuts: "AT12", pt: "16.3,48.2,3", poly: "1 1;2 2;3 3" });
    expect(decoded).toEqual({ kind: "point", lng: 16.3, lat: 48.2, km: 3 });
  });
});

describe("spatial filter geometry", () => {
  it("buffers a point into a closed polygon", () => {
    const geometry = spatialGeometry({ kind: "point", lng: 16.37, lat: 48.2, km: 5 });
    expect(geometry?.type).toBe("Polygon");
    const ring = geometry!.coordinates[0];
    expect(ring.length).toBeGreaterThan(16);
    expect(ring[0]).toEqual(ring[ring.length - 1]);
  });

  it("closes a drawn ring, which the map hands over open", () => {
    const geometry = spatialGeometry({
      kind: "polygon",
      ring: [
        [16.2, 48.1],
        [16.4, 48.1],
        [16.4, 48.3],
      ],
    });
    const ring = geometry!.coordinates[0];
    expect(ring).toHaveLength(4);
    expect(ring[0]).toEqual(ring[3]);
  });

  it("sends no geometry for a region, which is filtered by id", () => {
    expect(spatialGeometry({ kind: "region", nutsIds: ["AT12"] })).toBeUndefined();
    expect(spatialGeometry(null)).toBeUndefined();
  });

  it("draws only the region outlines that have been fetched", () => {
    const filter: CatalogSpatialFilter = { kind: "region", nutsIds: ["AT12", "AT13"] };
    const outline: GeoJSON.Geometry = {
      type: "Polygon",
      coordinates: [
        [
          [16, 48],
          [17, 48],
          [17, 49],
          [16, 48],
        ],
      ],
    };
    const features = spatialFeatures(filter, { AT12: outline, AT13: undefined });
    expect(features.features).toHaveLength(1);
    expect(features.features[0].properties).toEqual({ id: "AT12" });
  });
});

describe("formatBuffer", () => {
  it("uses metres below a kilometre", () => {
    expect(formatBuffer(0.5)).toBe("500 m");
    expect(formatBuffer(1)).toBe("1 km");
    expect(formatBuffer(12.5)).toBe("12.5 km");
  });
});
