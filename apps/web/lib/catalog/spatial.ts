import circle from "@turf/circle";

/** The catalog's spatial filter: what it can be, how it travels in the URL, and the geometry the API is asked with. */

export type CatalogSpatialFilter =
  /** One or more NUTS regions; a dataset matching any of them is included. */
  | { kind: "region"; nutsIds: string[]; names?: Record<string, string> }
  | { kind: "point"; lng: number; lat: number; km: number; label?: string }
  | { kind: "polygon"; ring: [number, number][] };

/** Buffer bounds: 500 m to 25 km in 500 m steps. */
export const MIN_BUFFER_KM = 0.5;
export const MAX_BUFFER_KM = 25;
export const BUFFER_STEP_KM = 0.5;
export const DEFAULT_BUFFER_KM = 3;

/** Enough segments that a circle reads as a circle at any usable zoom. */
const CIRCLE_STEPS = 64;

const round = (value: number, places = 5) => Number(value.toFixed(places));

/** The URL parameters a filter occupies. Absent keys are cleared. */
export type SpatialParams = {
  nuts: string | null;
  pt: string | null;
  poly: string | null;
};

export const encodeSpatial = (filter: CatalogSpatialFilter | null): SpatialParams => {
  if (!filter) return { nuts: null, pt: null, poly: null };
  switch (filter.kind) {
    case "region":
      return {
        nuts: filter.nutsIds.length ? filter.nutsIds.join(",") : null,
        pt: null,
        poly: null,
      };
    case "point":
      return {
        nuts: null,
        pt: [round(filter.lng), round(filter.lat), filter.km].join(","),
        poly: null,
      };
    case "polygon":
      return {
        nuts: null,
        pt: null,
        poly: filter.ring.map(([lng, lat]) => `${round(lng)} ${round(lat)}`).join(";"),
      };
  }
};

/** Read a filter back out of the URL. */
export const decodeSpatial = (params: {
  nuts?: string | null;
  pt?: string | null;
  poly?: string | null;
}): CatalogSpatialFilter | null => {
  if (params.pt) {
    const [lng, lat, km] = params.pt.split(",").map(Number);
    if ([lng, lat, km].every(Number.isFinite)) return { kind: "point", lng, lat, km };
    return null;
  }
  if (params.poly) {
    const ring = params.poly
      .split(";")
      .map((pair) => pair.trim().split(/\s+/).map(Number))
      .filter((pair) => pair.length === 2 && pair.every(Number.isFinite))
      .map(([lng, lat]) => [lng, lat] as [number, number]);
    return ring.length >= 3 ? { kind: "polygon", ring } : null;
  }
  if (params.nuts) {
    const nutsIds = params.nuts.split(",").map((id) => id.trim()).filter(Boolean);
    return nutsIds.length ? { kind: "region", nutsIds } : null;
  }
  return null;
};

/** The polygon to send as `intersects`, or `undefined` for a region — which is filtered by id (`?nuts=`) so the geometry never has to travel. */
export const spatialGeometry = (
  filter: CatalogSpatialFilter | null
): GeoJSON.Polygon | undefined => {
  if (!filter) return undefined;
  if (filter.kind === "point") {
    const drawn = circle([filter.lng, filter.lat], filter.km, {
      steps: CIRCLE_STEPS,
      units: "kilometers",
    });
    return drawn.geometry;
  }
  if (filter.kind === "polygon") {
    const ring = [...filter.ring];
    const [first] = ring;
    const last = ring[ring.length - 1];
    if (first[0] !== last[0] || first[1] !== last[1]) ring.push(first);
    return { type: "Polygon", coordinates: [ring] };
  }
  return undefined;
};

export const formatBuffer = (km: number) =>
  km < 1 ? `${Math.round(km * 1000)} m` : `${km} km`;

/** What to draw for a filter: its own geometry, plus any region outlines that have been fetched. */
export const spatialFeatures = (
  filter: CatalogSpatialFilter | null,
  regionGeometries?: Record<string, GeoJSON.Geometry | undefined>
): GeoJSON.FeatureCollection => {
  const features: GeoJSON.Feature[] = [];
  if (filter?.kind === "region") {
    for (const id of filter.nutsIds) {
      const geometry = regionGeometries?.[id];
      if (geometry) features.push({ type: "Feature", geometry, properties: { id } });
    }
  } else {
    const geometry = spatialGeometry(filter);
    if (geometry) features.push({ type: "Feature", geometry, properties: {} });
  }
  return { type: "FeatureCollection", features };
};
