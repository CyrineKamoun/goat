import { describe, expect, it } from "vitest";

import {
  SCALEBAR_PADDING_PX,
  calculateScaleBarParams,
  getNiceNumber,
  metersPerPixelFromViewState,
  scalebarBarWidthPx,
  snapScalebarWidths,
} from "@/lib/print/scalebar";
import { mmToPx, pxToMm } from "@/lib/print/units";
import type { ReportElement } from "@/lib/validations/reportLayout";

const map = (zoom: number): ReportElement =>
  ({
    id: "map-1",
    type: "map",
    position: { x: 0, y: 0, width: 200, height: 150, z_index: 0 },
    config: { viewState: { zoom, latitude: 48, longitude: 11 } },
  }) as unknown as ReportElement;

const scalebar = (overrides: Record<string, unknown> = {}, config: Record<string, unknown> = {}): ReportElement =>
  ({
    id: "scalebar-1",
    type: "scalebar",
    position: { x: 10, y: 10, width: 100, height: 20, z_index: 1 },
    config: { mapElementId: "map-1", unit: "kilometers", segmentsRight: 2, ...config },
    ...overrides,
  }) as unknown as ReportElement;

describe("getNiceNumber", () => {
  it("rounds down to 1, 2 or 5 times a power of ten", () => {
    expect(getNiceNumber(0.87)).toBe(0.5);
    expect(getNiceNumber(4.99)).toBe(2);
    expect(getNiceNumber(3.5)).toBe(2);
    expect(getNiceNumber(12)).toBe(10);
    expect(getNiceNumber(0)).toBe(1);
  });

  it("keeps a value that is a round number within floating-point noise", () => {
    expect(getNiceNumber(0.5)).toBe(0.5);
    expect(getNiceNumber(0.5 * (1 - 1e-9))).toBe(0.5);
    expect(getNiceNumber(2 * (1 - 1e-9))).toBe(2);
    expect(getNiceNumber(1000 * (1 - 1e-9))).toBe(1000);
  });
});

describe("metersPerPixelFromViewState", () => {
  it("uses the 512px web-mercator tile constant", () => {
    const mpp = metersPerPixelFromViewState({ zoom: 12, latitude: 48 });
    expect(mpp).toBeCloseTo((78271.51696 * Math.cos((48 * Math.PI) / 180)) / 4096, 6);
  });

  it("falls back to the default scale without a view state", () => {
    expect(metersPerPixelFromViewState(undefined)).toBe(100);
  });
});

describe("calculateScaleBarParams", () => {
  it("rounds the total and reports the fraction of the width the bar uses", () => {
    // 1 m/px over 870 px = 0.87 km -> 0.5 km, so the bar fills 0.5 / 0.87 of the width
    const p = calculateScaleBarParams(1, 870, "kilometers", 2, 0);
    expect(p.niceTotalUnits).toBe(0.5);
    expect(p.segmentValueRight).toBe(0.25);
    expect(p.widthRatio).toBeCloseTo(0.5 / 0.87, 9);
  });
});

describe("snapScalebarWidths", () => {
  it("shrinks the rectangle to the bar and remembers the dragged width as the maximum", () => {
    const [, snapped] = snapScalebarWidths([map(12), scalebar()]);
    const maxInnerPx = mmToPx(100) - 2 * SCALEBAR_PADDING_PX;
    const mpp = metersPerPixelFromViewState({ zoom: 12, latitude: 48 });
    const { widthRatio } = calculateScaleBarParams(mpp, maxInnerPx, "kilometers", 2, 0);
    expect(widthRatio).toBeLessThan(1);
    expect(snapped.position.width).toBeCloseTo(pxToMm(widthRatio * maxInnerPx + 2 * SCALEBAR_PADDING_PX), 9);
    expect(snapped.position.x).toBe(10);
    expect(snapped.position.height).toBe(20);
    expect(snapped.config?.maxWidthMm).toBe(100);
  });

  it("is idempotent and returns the same array when nothing changes", () => {
    const once = snapScalebarWidths([map(12), scalebar()]);
    const twice = snapScalebarWidths(once);
    expect(twice).toBe(once);
  });

  it("grows back to the maximum when the map returns to its scale (no ratchet)", () => {
    const start = snapScalebarWidths([map(12), scalebar()]);
    const startWidth = start[1].position.width;
    const zoomedIn = snapScalebarWidths([map(12.7), start[1]]);
    expect(zoomedIn[1].position.width).not.toBeCloseTo(startWidth, 6);
    const back = snapScalebarWidths([map(12), zoomedIn[1]]);
    expect(back[1].position.width).toBeCloseTo(startWidth, 9);
  });

  it("never makes the rectangle wider than the dragged maximum", () => {
    for (const zoom of [8, 9.3, 10.6, 12, 13.2, 14.9, 16]) {
      const [, s] = snapScalebarWidths([map(zoom), scalebar()]);
      expect(s.position.width).toBeLessThanOrEqual(100 + 1e-9);
      expect(s.position.width).toBeGreaterThan(30);
    }
  });

  it("leaves the numeric style and other element types alone", () => {
    const elements = [map(12), scalebar({}, { style: "numeric" })];
    expect(snapScalebarWidths(elements)).toBe(elements);
    const noScalebar = [map(12)];
    expect(snapScalebarWidths(noScalebar)).toBe(noScalebar);
  });

  it("uses the default scale for a scalebar without a connected map", () => {
    const [snapped] = snapScalebarWidths([scalebar({}, { mapElementId: null })]);
    expect(snapped.position.width).toBeLessThanOrEqual(100);
    expect(snapped.config?.maxWidthMm).toBe(100);
  });

  it("keeps the bar exactly the labelled distance", () => {
    const [m, s] = snapScalebarWidths([map(12), scalebar()]);
    const mpp = metersPerPixelFromViewState(m.config?.viewState as { zoom: number; latitude: number });
    const barPx = scalebarBarWidthPx(s, mpp);
    const params = calculateScaleBarParams(mpp, mmToPx(100) - 2 * SCALEBAR_PADDING_PX, "kilometers", 2, 0);
    expect(barPx * mpp).toBeCloseTo(params.niceTotalUnits * 1000, 6);
    // and the bar fits the snapped rectangle's inner width
    expect(barPx).toBeCloseTo(mmToPx(s.position.width) - 2 * SCALEBAR_PADDING_PX, 6);
  });
});
