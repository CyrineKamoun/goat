/**
 * Unit conversion utilities for print layouts
 * Converts between millimeters (print units) and pixels (screen units)
 */

// Standard DPI for screen displays
export const SCREEN_DPI = 96;

// Standard DPI for print quality
export const PRINT_DPI = 300;

// Conversion constant: 1 inch = 25.4 millimeters
export const MM_PER_INCH = 25.4;

/**
 * Convert millimeters to pixels
 * @param mm - Value in millimeters
 * @param dpi - Dots per inch (default: 96 for screen)
 * @returns Value in pixels
 */
export function mmToPx(mm: number, dpi: number = SCREEN_DPI): number {
  return (mm / MM_PER_INCH) * dpi;
}

/**
 * Convert pixels to millimeters
 * @param px - Value in pixels
 * @param dpi - Dots per inch (default: 96 for screen)
 * @returns Value in millimeters
 */
export function pxToMm(px: number, dpi: number = SCREEN_DPI): number {
  return (px / dpi) * MM_PER_INCH;
}

/**
 * Convert inches to millimeters
 */
export function inchesToMm(inches: number): number {
  return inches * MM_PER_INCH;
}

/**
 * Convert millimeters to inches
 */
export function mmToInches(mm: number): number {
  return mm / MM_PER_INCH;
}

/**
 * Standard page sizes in millimeters
 */
export const PAGE_SIZES = {
  A4: { width: 210, height: 297 },
  A3: { width: 297, height: 420 },
  A2: { width: 420, height: 594 },
  A1: { width: 594, height: 841 },
  Letter: { width: inchesToMm(8.5), height: inchesToMm(11) },
  Legal: { width: inchesToMm(8.5), height: inchesToMm(14) },
  Tabloid: { width: inchesToMm(11), height: inchesToMm(17) },
} as const;

export type PageSize = keyof typeof PAGE_SIZES;
export type PageOrientation = "portrait" | "landscape";

/** A page's printable extent in millimetres, the way the sheet is held. */
export interface PageMm {
  width: number;
  height: number;
}

/** The part of a layout's page config that decides the sheet's size. */
export interface PageSizeConfig {
  size: PageSize | "Custom";
  orientation: PageOrientation;
  /** A Custom page's own width and height in millimetres. */
  width?: number;
  height?: number;
}

/** Bounds for one side of a Custom page, in millimetres. */
export const CUSTOM_PAGE_MIN_MM = 50;
export const CUSTOM_PAGE_MAX_MM = 1500;

export function isCustomSideValid(mm: number): boolean {
  return Number.isFinite(mm) && mm >= CUSTOM_PAGE_MIN_MM && mm <= CUSTOM_PAGE_MAX_MM;
}

/**
 * The sheet a page config describes, in millimetres and already turned the
 * way it prints. A named size is looked up and rotated by its orientation; a
 * Custom page is its stored width and height as typed, so orientation has no
 * say there. Anything unresolvable is an A4 sheet in the requested
 * orientation, which is what the canvas has always drawn.
 */
export function resolvePageMm(page: PageSizeConfig): PageMm {
  if (
    page.size === "Custom" &&
    isCustomSideValid(page.width ?? NaN) &&
    isCustomSideValid(page.height ?? NaN)
  ) {
    return { width: page.width as number, height: page.height as number };
  }
  const named =
    page.size !== "Custom" && Object.prototype.hasOwnProperty.call(PAGE_SIZES, page.size)
      ? PAGE_SIZES[page.size]
      : undefined;
  const dims = named ?? PAGE_SIZES.A4;
  return page.orientation === "landscape"
    ? { width: dims.height, height: dims.width }
    : { width: dims.width, height: dims.height };
}

/**
 * The longest side an export may have, in output pixels. Headless Chromium
 * and WebGL refuse canvases past 16384 px, and the map frame is such a
 * canvas; this leaves headroom under that.
 */
export const MAX_RENDER_SIDE_PX = 14000;

/** The DPI options the page settings offer, lowest first. */
export const EXPORT_DPI_OPTIONS = [72, 150, 300, 600] as const;

/** The pixel size an export of `page` comes out at, at `dpi`. */
export function outputPixels(page: PageMm, dpi: number): { width: number; height: number } {
  return { width: Math.round(mmToPx(page.width, dpi)), height: Math.round(mmToPx(page.height, dpi)) };
}

/** Whether `page` still renders at `dpi` without exceeding the render cap. */
export function isDpiAllowed(page: PageMm, dpi: number): boolean {
  const out = outputPixels(page, dpi);
  return Math.max(out.width, out.height) <= MAX_RENDER_SIDE_PX;
}

/**
 * The highest offered DPI at or below `dpi` that `page` renders at. The
 * lowest option is returned even when it too is over the cap, so a caller
 * always gets a usable number back.
 */
export function clampDpi(page: PageMm, dpi: number): number {
  const allowed = EXPORT_DPI_OPTIONS.filter((option) => option <= dpi && isDpiAllowed(page, option));
  return allowed.length ? allowed[allowed.length - 1] : EXPORT_DPI_OPTIONS[0];
}

/**
 * Get page dimensions in millimeters
 * @param size - Page size name
 * @param orientation - Page orientation
 * @returns Width and height in millimeters
 */
export function getPageDimensions(
  size: PageSize,
  orientation: PageOrientation = "portrait"
): { width: number; height: number } {
  const dims = PAGE_SIZES[size];

  if (orientation === "landscape") {
    return { width: dims.height, height: dims.width };
  }

  return { width: dims.width, height: dims.height };
}

/**
 * Calculate scale factor to fit content to page
 * @param contentWidth - Content width in mm
 * @param contentHeight - Content height in mm
 * @param pageSize - Target page size
 * @param orientation - Page orientation
 * @param margins - Page margins in mm
 * @returns Scale factor (1.0 = 100%)
 */
export function calculateScaleToFit(
  contentWidth: number,
  contentHeight: number,
  pageSize: PageSize,
  orientation: PageOrientation = "portrait",
  margins = { top: 10, right: 10, bottom: 10, left: 10 }
): number {
  const page = getPageDimensions(pageSize, orientation);
  const availableWidth = page.width - margins.left - margins.right;
  const availableHeight = page.height - margins.top - margins.bottom;

  const scaleX = availableWidth / contentWidth;
  const scaleY = availableHeight / contentHeight;

  // Use the smaller scale to ensure content fits
  return Math.min(scaleX, scaleY, 1); // Never scale up beyond 100%
}
