/**
 * Client-side CRS suggestion engine.
 *
 * Given a WKT extent (EPSG:4326), returns a list of CRS options:
 *  - Global CRS (always shown)
 *  - UTM zone (computed from centroid, only if extent ≤ 6° longitude)
 *  - Regional/national CRS whose area of use intersects the data extent
 */

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface CRSSuggestion {
  /** EPSG code as string, e.g. "25832" */
  code: string;
  /** Human-readable label, e.g. "ETRS89 / UTM zone 32N" */
  label: string;
  /** Grouping key for the UI */
  group: "global" | "utm" | "regional";
}

interface BBox {
  xmin: number;
  ymin: number;
  xmax: number;
  ymax: number;
}

// ---------------------------------------------------------------------------
// Global CRS (always shown)
// ---------------------------------------------------------------------------

const GLOBAL_CRS: CRSSuggestion[] = [
  { code: "4326", label: "WGS 84 (EPSG:4326)", group: "global" },
  { code: "3857", label: "Web Mercator (EPSG:3857)", group: "global" },
];

// ---------------------------------------------------------------------------
// Curated regional / national CRS with their area of use (lon/lat bbox)
// ---------------------------------------------------------------------------

interface RegionalCRS {
  code: string;
  label: string;
  /** [xmin, ymin, xmax, ymax] in EPSG:4326 */
  bbox: [number, number, number, number];
}

const REGIONAL_CRS: RegionalCRS[] = [
  // ── Europe-wide ──
  { code: "4258", label: "ETRS89 (EPSG:4258)", bbox: [-16.1, 32.88, 40.18, 84.73] },
  { code: "3035", label: "ETRS89 / LAEA Europe (EPSG:3035)", bbox: [-16.1, 32.88, 40.18, 84.73] },

  // ── Germany ──
  { code: "25832", label: "ETRS89 / UTM zone 32N (EPSG:25832)", bbox: [5.87, 47.27, 13.84, 55.09] },
  { code: "25833", label: "ETRS89 / UTM zone 33N (EPSG:25833)", bbox: [5.87, 47.27, 15.04, 55.09] },
  { code: "4839", label: "ETRS89 / LCC Germany (EPSG:4839)", bbox: [5.87, 47.27, 15.04, 55.09] },

  // ── Austria ──
  { code: "31287", label: "MGI / Austria Lambert (EPSG:31287)", bbox: [9.53, 46.41, 17.17, 49.02] },
  { code: "31255", label: "MGI / Austria GK M28 (EPSG:31255)", bbox: [9.53, 46.41, 17.17, 49.02] },

  // ── Switzerland ──
  { code: "2056", label: "CH1903+ / LV95 (EPSG:2056)", bbox: [5.96, 45.82, 10.49, 47.81] },

  // ── France ──
  { code: "2154", label: "RGF93 v1 / Lambert-93 (EPSG:2154)", bbox: [-9.86, 41.15, 10.38, 51.56] },

  // ── Spain ──
  { code: "25830", label: "ETRS89 / UTM zone 30N (EPSG:25830)", bbox: [-9.37, 35.95, 4.33, 43.82] },

  // ── Italy ──
  { code: "6875", label: "RDN2008 / Italy zone (EPSG:6875)", bbox: [6.62, 36.62, 18.52, 47.09] },

  // ── Netherlands ──
  { code: "28992", label: "Amersfoort / RD New (EPSG:28992)", bbox: [3.37, 50.75, 7.21, 53.47] },

  // ── Belgium ──
  { code: "31370", label: "Belge 1972 / Belgian Lambert 72 (EPSG:31370)", bbox: [2.5, 49.5, 6.4, 51.5] },

  // ── United Kingdom ──
  { code: "27700", label: "OSGB 1936 / British National Grid (EPSG:27700)", bbox: [-8.82, 49.79, 1.92, 60.94] },

  // ── Ireland ──
  { code: "2157", label: "IRENET95 / Irish Transverse Mercator (EPSG:2157)", bbox: [-10.56, 51.39, -5.34, 55.43] },

  // ── Nordic ──
  { code: "25835", label: "ETRS89 / UTM zone 35N (EPSG:25835)", bbox: [24.0, 59.0, 32.0, 71.0] },
  { code: "3006", label: "SWEREF99 TM (EPSG:3006)", bbox: [10.57, 55.28, 24.17, 69.07] },
  { code: "25833", label: "ETRS89 / UTM zone 33N (EPSG:25833)", bbox: [4.68, 57.93, 31.17, 71.19] },

  // ── Poland ──
  { code: "2180", label: "ETRS89 / Poland CS92 (EPSG:2180)", bbox: [14.12, 49.0, 24.15, 54.84] },

  // ── Czech Republic ──
  { code: "5514", label: "S-JTSK / Krovak East North (EPSG:5514)", bbox: [12.09, 48.55, 18.86, 51.06] },

  // ── Portugal ──
  { code: "3763", label: "ETRS89 / Portugal TM06 (EPSG:3763)", bbox: [-9.52, 36.96, -6.19, 42.15] },

  // ── Greece ──
  { code: "2100", label: "GGRS87 / Greek Grid (EPSG:2100)", bbox: [19.57, 34.80, 29.65, 41.75] },

  // ── United States ──
  { code: "5070", label: "NAD83 / Conus Albers (EPSG:5070)", bbox: [-124.79, 24.41, -66.91, 49.38] },
  { code: "6350", label: "NAD83(2011) / Conus Albers (EPSG:6350)", bbox: [-124.79, 24.41, -66.91, 49.38] },

  // ── Canada ──
  { code: "3979", label: "NAD83(CSRS) / Canada Atlas Lambert (EPSG:3979)", bbox: [-141.01, 40.04, -47.74, 86.46] },

  // ── Australia ──
  { code: "7844", label: "GDA2020 (EPSG:7844)", bbox: [93.41, -60.56, 173.35, -8.47] },
  { code: "3112", label: "GDA94 / Geoscience Australia Lambert (EPSG:3112)", bbox: [93.41, -60.56, 173.35, -8.47] },

  // ── Brazil ──
  { code: "5880", label: "SIRGAS 2000 / Brazil Polyconic (EPSG:5880)", bbox: [-73.98, -33.77, -28.85, 5.27] },

  // ── South Africa ──
  { code: "2048", label: "Hartebeesthoek94 / Lo19 (EPSG:2048)", bbox: [16.45, -34.88, 32.95, -22.13] },

  // ── Japan ──
  { code: "6668", label: "JGD2011 (EPSG:6668)", bbox: [122.38, 17.09, 157.65, 46.05] },

  // ── South Korea ──
  { code: "5186", label: "Korean 1985 / Central Belt (EPSG:5186)", bbox: [124.53, 33.06, 131.87, 38.64] },

  // ── India ──
  { code: "7755", label: "WGS 84 / India NSF LCC (EPSG:7755)", bbox: [68.08, 6.75, 97.42, 37.08] },

  // ── China ──
  { code: "4490", label: "CGCS2000 (EPSG:4490)", bbox: [73.62, 3.86, 134.77, 53.56] },

  // ── New Zealand ──
  { code: "2193", label: "NZGD2000 / NZTM 2000 (EPSG:2193)", bbox: [166.37, -47.33, 178.63, -34.10] },
];

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Parse a WKT POLYGON or MULTIPOLYGON string into a bounding box. */
export function wktToBBox(wkt: string): BBox | null {
  if (!wkt) return null;

  const coordPattern = /-?\d+\.?\d*\s+-?\d+\.?\d*/g;
  const matches = wkt.match(coordPattern);
  if (!matches || matches.length === 0) return null;

  let xmin = Infinity,
    ymin = Infinity,
    xmax = -Infinity,
    ymax = -Infinity;

  for (const pair of matches) {
    const [x, y] = pair.split(/\s+/).map(Number);
    if (isNaN(x) || isNaN(y)) continue;
    if (x < xmin) xmin = x;
    if (x > xmax) xmax = x;
    if (y < ymin) ymin = y;
    if (y > ymax) ymax = y;
  }

  if (!isFinite(xmin) || !isFinite(ymin) || !isFinite(xmax) || !isFinite(ymax)) return null;

  return { xmin, ymin, xmax, ymax };
}

/** Check if two bboxes intersect. */
function bboxIntersects(a: BBox, b: BBox): boolean {
  return a.xmin <= b.xmax && a.xmax >= b.xmin && a.ymin <= b.ymax && a.ymax >= b.ymin;
}

/** Returns true if the extent covers essentially the whole world (default extent). */
function isGlobalExtent(bbox: BBox): boolean {
  return bbox.xmax - bbox.xmin > 350 && bbox.ymax - bbox.ymin > 170;
}

// ---------------------------------------------------------------------------
// UTM zone detection
// ---------------------------------------------------------------------------

/** ETRS89 area of use (approximate) */
const ETRS89_BBOX: BBox = { xmin: -16.1, ymin: 32.88, xmax: 40.18, ymax: 84.73 };

/**
 * Check if a bbox is fully contained within another bbox.
 */
function bboxContains(outer: BBox, inner: BBox): boolean {
  return inner.xmin >= outer.xmin && inner.xmax <= outer.xmax &&
    inner.ymin >= outer.ymin && inner.ymax <= outer.ymax;
}

/**
 * Compute the UTM CRS for the centroid of a bbox.
 * Uses ETRS89 UTM (EPSG:258xx) for European data, WGS 84 UTM (EPSG:326xx/327xx) otherwise.
 * Returns null if the extent spans more than 12 degrees of longitude.
 */
function getUtmSuggestion(bbox: BBox): CRSSuggestion | null {
  const lonSpan = bbox.xmax - bbox.xmin;
  if (lonSpan > 12) return null;

  const centroidLon = (bbox.xmin + bbox.xmax) / 2;
  const centroidLat = (bbox.ymin + bbox.ymax) / 2;

  const zoneNumber = Math.floor((centroidLon + 180) / 6) + 1;
  const isNorth = centroidLat >= 0;

  // Use ETRS89 UTM for data within Europe (northern hemisphere, zones 28-38)
  if (isNorth && bboxContains(ETRS89_BBOX, bbox)) {
    const epsgCode = 25800 + zoneNumber;
    return {
      code: String(epsgCode),
      label: `ETRS89 / UTM zone ${zoneNumber}N (EPSG:${epsgCode})`,
      group: "utm",
    };
  }

  // WGS 84 / UTM zone for the rest of the world
  const epsgCode = isNorth ? 32600 + zoneNumber : 32700 + zoneNumber;
  const hemisphere = isNorth ? "N" : "S";

  return {
    code: String(epsgCode),
    label: `WGS 84 / UTM zone ${zoneNumber}${hemisphere} (EPSG:${epsgCode})`,
    group: "utm",
  };
}

// ---------------------------------------------------------------------------
// Main API
// ---------------------------------------------------------------------------

/**
 * Given a WKT extent string (in EPSG:4326), return suggested CRS options
 * grouped as global, utm, and regional.
 */
export function getSuggestedCRS(extentWkt: string): CRSSuggestion[] {
  const bbox = wktToBBox(extentWkt);

  // If we can't parse or it's the global default extent, only return globals
  if (!bbox || isGlobalExtent(bbox)) {
    return [...GLOBAL_CRS];
  }

  const suggestions: CRSSuggestion[] = [...GLOBAL_CRS];
  const seen = new Set(suggestions.map((s) => s.code));

  // UTM suggestion (always first in recommended)
  const utm = getUtmSuggestion(bbox);
  if (utm && !seen.has(utm.code)) {
    suggestions.push(utm);
    seen.add(utm.code);
  }

  // Regional CRS matching
  for (const rcrs of REGIONAL_CRS) {
    if (seen.has(rcrs.code)) continue;
    const regionBBox: BBox = {
      xmin: rcrs.bbox[0],
      ymin: rcrs.bbox[1],
      xmax: rcrs.bbox[2],
      ymax: rcrs.bbox[3],
    };
    if (bboxIntersects(bbox, regionBBox)) {
      suggestions.push({ code: rcrs.code, label: rcrs.label, group: "regional" });
      seen.add(rcrs.code);
    }
  }

  return suggestions;
}
