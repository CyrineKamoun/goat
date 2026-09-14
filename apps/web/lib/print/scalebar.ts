/**
 * Scalebar geometry shared by the layout editor, the print page and the
 * scalebar renderer.
 *
 * The bar works in "fit segment width" mode: the distance a rectangle
 * represents is rounded down to a round number (1, 2 or 5 × 10ⁿ), and the
 * bar is drawn exactly as long as that distance. The rectangle the user drags
 * is therefore only a maximum; `snapScalebarWidths` shrinks each scalebar's
 * rectangle to its bar so that the element frame always fits the bar, and
 * grows it back up to the dragged maximum when the map scale allows.
 */
import { mmToPx, pxToMm, SCREEN_DPI } from "@/lib/print/units";
import type { ReportElement } from "@/lib/validations/reportLayout";

import type { ScalebarUnit } from "@/components/reports/elements/config/ScalebarElementConfig";

/** Space between the frame and the bar, in CSS pixels at screen DPI. */
export const SCALEBAR_PADDING_PX = 8;

/** Metres per pixel used when the scalebar has no connected map. */
export const DEFAULT_METERS_PER_PIXEL = 100;

/** Tolerance for treating a distance as already round. */
const ROUND_TOLERANCE = 1e-6;

export interface ScalebarViewState {
  zoom?: number;
  latitude?: number;
}

/** Convert metres to the scalebar's unit. */
export const metersToUnit = (meters: number, unit: ScalebarUnit): number => {
  switch (unit) {
    case "kilometers":
      return meters / 1000;
    case "meters":
      return meters;
    case "feet":
      return meters * 3.28084;
    case "yards":
      return meters * 1.09361;
    case "miles":
      return meters / 1609.344;
    case "nautical_miles":
      return meters / 1852;
    case "centimeters":
      return meters * 100;
    case "millimeters":
      return meters * 1000;
    case "inches":
      return meters * 39.3701;
    case "map_units":
    default:
      return meters;
  }
};

/**
 * Largest round number (1, 2 or 5 × 10ⁿ) that is at most `value`. A value
 * within floating-point noise of a round number counts as that number.
 */
export const getNiceNumber = (value: number): number => {
  if (value <= 0) return 1;
  const tolerant = value * (1 + ROUND_TOLERANCE);
  const magnitude = Math.pow(10, Math.floor(Math.log10(tolerant)));
  const normalized = tolerant / magnitude;
  if (normalized < 2) return magnitude;
  if (normalized < 5) return 2 * magnitude;
  return 5 * magnitude;
};

/**
 * Ground resolution of a map element's view, in metres per CSS pixel.
 * MapLibre uses 512px tiles, so the equator constant is 40075016.686 / 512.
 */
export const metersPerPixelFromViewState = (viewState: ScalebarViewState | undefined): number => {
  if (!viewState) return DEFAULT_METERS_PER_PIXEL;
  const zoom = viewState.zoom ?? 10;
  const latitude = viewState.latitude ?? 48;
  return (78271.51696 * Math.cos((latitude * Math.PI) / 180)) / Math.pow(2, zoom);
};

export interface ScaleBarParams {
  niceTotalUnits: number;
  segmentValueRight: number;
  segmentValueLeft: number;
  segmentsRight: number;
  segmentsLeft: number;
  /** Fraction of `maxWidthPx` the bar occupies (0 < ratio <= 1). */
  widthRatio: number;
}

/**
 * Round the distance `maxWidthPx` represents down to a round number and
 * report how much of that width the bar then uses. Left segments subdivide
 * the first right segment.
 */
export const calculateScaleBarParams = (
  metersPerPixel: number,
  maxWidthPx: number,
  unit: ScalebarUnit,
  segmentsRight: number,
  segmentsLeft: number
): ScaleBarParams => {
  const effectiveSegmentsRight = Math.max(1, segmentsRight);
  const rawTotalUnits = metersToUnit(metersPerPixel * Math.max(1, maxWidthPx), unit);
  const niceTotalUnits = getNiceNumber(rawTotalUnits);
  const widthRatio = Math.min(niceTotalUnits / rawTotalUnits, 1);
  const segmentValueRight = niceTotalUnits / effectiveSegmentsRight;
  const effectiveSegmentsLeft = Math.max(0, segmentsLeft);
  const segmentValueLeft = effectiveSegmentsLeft > 0 ? segmentValueRight / effectiveSegmentsLeft : 0;
  return {
    niceTotalUnits,
    segmentValueRight,
    segmentValueLeft,
    segmentsRight: effectiveSegmentsRight,
    segmentsLeft: effectiveSegmentsLeft,
    widthRatio,
  };
};

interface ScalebarSizing {
  maxWidthMm: number;
  unit: ScalebarUnit;
  segmentsRight: number;
  segmentsLeft: number;
}

const sizingOf = (element: ReportElement): ScalebarSizing => {
  const config = (element.config ?? {}) as Record<string, unknown>;
  return {
    maxWidthMm: typeof config.maxWidthMm === "number" ? config.maxWidthMm : element.position.width,
    unit: (config.unit as ScalebarUnit | undefined) ?? "kilometers",
    segmentsRight: (config.segmentsRight as number | undefined) ?? 2,
    segmentsLeft: (config.segmentsLeft as number | undefined) ?? 0,
  };
};

/** The bar's inner space at the dragged maximum width, in CSS pixels. */
const maxInnerWidthPx = (maxWidthMm: number): number =>
  Math.max(1, mmToPx(maxWidthMm, SCREEN_DPI) - 2 * SCALEBAR_PADDING_PX);

/** Parameters of a scalebar element's bar for the given ground resolution. */
export const scalebarParamsFor = (element: ReportElement, metersPerPixel: number): ScaleBarParams => {
  const s = sizingOf(element);
  return calculateScaleBarParams(metersPerPixel, maxInnerWidthPx(s.maxWidthMm), s.unit, s.segmentsRight, s.segmentsLeft);
};

/** Length of the bar itself, in CSS pixels at screen DPI. */
export const scalebarBarWidthPx = (element: ReportElement, metersPerPixel: number): number => {
  const s = sizingOf(element);
  return scalebarParamsFor(element, metersPerPixel).widthRatio * maxInnerWidthPx(s.maxWidthMm);
};

/** Ground resolution of the map a scalebar is connected to. */
export const metersPerPixelForScalebar = (element: ReportElement, elements: ReportElement[]): number => {
  const mapElementId = (element.config as Record<string, unknown> | undefined)?.mapElementId;
  const connected = mapElementId ? elements.find((el) => el.id === mapElementId && el.type === "map") : undefined;
  const viewState = connected?.config?.viewState as ScalebarViewState | undefined;
  return metersPerPixelFromViewState(viewState);
};

/**
 * Fit every scalebar's rectangle to its bar: width = bar + padding, never
 * wider than the dragged maximum, which is remembered in `config.maxWidthMm`.
 * Returns the same array when no element needs to change.
 */
export const snapScalebarWidths = (elements: ReportElement[]): ReportElement[] => {
  let changed = false;
  const next = elements.map((element) => {
    if (element.type !== "scalebar") return element;
    const config = (element.config ?? {}) as Record<string, unknown>;
    if (config.style === "numeric") return element;

    const s = sizingOf(element);
    const barPx = scalebarBarWidthPx(element, metersPerPixelForScalebar(element, elements));
    const widthMm = Math.min(s.maxWidthMm, pxToMm(barPx + 2 * SCALEBAR_PADDING_PX, SCREEN_DPI));
    const widthChanged = Math.abs(widthMm - element.position.width) > 1e-6;
    const maxMissing = config.maxWidthMm !== s.maxWidthMm;
    if (!widthChanged && !maxMissing) return element;

    changed = true;
    return {
      ...element,
      position: { ...element.position, width: widthMm },
      config: { ...config, maxWidthMm: s.maxWidthMm },
    };
  });
  return changed ? next : elements;
};
