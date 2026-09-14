import { describe, expect, it } from "vitest";

import {
  CUSTOM_PAGE_MAX_MM,
  CUSTOM_PAGE_MIN_MM,
  PAGE_SIZES,
  clampDpi,
  getPageDimensions,
  inchesToMm,
  isCustomSideValid,
  isDpiAllowed,
  mmToInches,
  mmToPx,
  outputPixels,
  pxToMm,
  resolvePageMm,
} from "../units";

describe("Print Unit Conversions", () => {
  describe("mmToPx", () => {
    it("should convert millimeters to pixels at default DPI (96)", () => {
      expect(mmToPx(25.4)).toBeCloseTo(96, 0); // 1 inch = 25.4mm = 96px
      expect(mmToPx(10)).toBeCloseTo(37.8, 1);
    });

    it("should convert millimeters to pixels at custom DPI", () => {
      expect(mmToPx(25.4, 300)).toBeCloseTo(300, 0); // 1 inch at 300 DPI
      expect(mmToPx(10, 300)).toBeCloseTo(118.11, 1);
    });

    it("should handle zero", () => {
      expect(mmToPx(0)).toBe(0);
    });
  });

  describe("pxToMm", () => {
    it("should convert pixels to millimeters at default DPI", () => {
      expect(pxToMm(96)).toBeCloseTo(25.4, 1); // 96px = 1 inch = 25.4mm
      expect(pxToMm(37.8)).toBeCloseTo(10, 0);
    });

    it("should convert pixels to millimeters at custom DPI", () => {
      expect(pxToMm(300, 300)).toBeCloseTo(25.4, 1);
    });

    it("should be inverse of mmToPx", () => {
      const original = 100;
      expect(pxToMm(mmToPx(original))).toBeCloseTo(original, 1);
    });
  });

  describe("inchesToMm and mmToInches", () => {
    it("should convert inches to millimeters", () => {
      expect(inchesToMm(1)).toBeCloseTo(25.4, 1);
      expect(inchesToMm(8.5)).toBeCloseTo(215.9, 1);
    });

    it("should convert millimeters to inches", () => {
      expect(mmToInches(25.4)).toBeCloseTo(1, 2);
      expect(mmToInches(297)).toBeCloseTo(11.69, 2); // A4 height
    });
  });

  describe("getPageDimensions", () => {
    it("should return correct dimensions for A4 portrait", () => {
      const dims = getPageDimensions("A4", "portrait");
      expect(dims.width).toBe(210);
      expect(dims.height).toBe(297);
    });

    it("should return correct dimensions for A4 landscape", () => {
      const dims = getPageDimensions("A4", "landscape");
      expect(dims.width).toBe(297);
      expect(dims.height).toBe(210);
    });

    it("should return correct dimensions for Letter portrait", () => {
      const dims = getPageDimensions("Letter", "portrait");
      expect(dims.width).toBeCloseTo(215.9, 1);
      expect(dims.height).toBeCloseTo(279.4, 1);
    });

    it("should handle all standard page sizes", () => {
      const sizes: Array<"A4" | "A3" | "Letter" | "Legal" | "Tabloid"> = [
        "A4",
        "A3",
        "Letter",
        "Legal",
        "Tabloid",
      ];

      sizes.forEach((size) => {
        const dims = getPageDimensions(size, "portrait");
        expect(dims.width).toBeGreaterThan(0);
        expect(dims.height).toBeGreaterThan(dims.width);
      });
    });
  });
});

describe("Page sizes beyond A3", () => {
  it("knows A2 and A1 in portrait millimetres", () => {
    expect(PAGE_SIZES.A2).toEqual({ width: 420, height: 594 });
    expect(PAGE_SIZES.A1).toEqual({ width: 594, height: 841 });
  });
});

describe("resolvePageMm", () => {
  it("turns a named size plus orientation into width and height", () => {
    expect(resolvePageMm({ size: "A3", orientation: "landscape" })).toEqual({ width: 420, height: 297 });
    expect(resolvePageMm({ size: "A1", orientation: "portrait" })).toEqual({ width: 594, height: 841 });
  });

  it("uses the stored width and height for a Custom page, ignoring orientation", () => {
    expect(resolvePageMm({ size: "Custom", orientation: "portrait", width: 300, height: 200 })).toEqual({
      width: 300,
      height: 200,
    });
  });

  it("falls back to A4 when a Custom page has no dimensions", () => {
    expect(resolvePageMm({ size: "Custom", orientation: "landscape" })).toEqual({ width: 297, height: 210 });
  });

  it("falls back to A4 for an unknown size name", () => {
    expect(resolvePageMm({ size: "B5" as never, orientation: "portrait" })).toEqual({
      width: 210,
      height: 297,
    });
  });
});

describe("resolvePageMm and inherited names", () => {
  it("falls back to A4 for a size named after an inherited object member", () => {
    expect(resolvePageMm({ size: "toString" as never, orientation: "portrait" })).toEqual({
      width: 210,
      height: 297,
    });
  });
});

describe("custom page bounds", () => {
  it("accepts sides between 50 and 1500 mm", () => {
    expect(CUSTOM_PAGE_MIN_MM).toBe(50);
    expect(CUSTOM_PAGE_MAX_MM).toBe(1500);
    expect(isCustomSideValid(50)).toBe(true);
    expect(isCustomSideValid(1500)).toBe(true);
    expect(isCustomSideValid(49.9)).toBe(false);
    expect(isCustomSideValid(1500.1)).toBe(false);
    expect(isCustomSideValid(Number.NaN)).toBe(false);
  });
});

describe("render size cap", () => {
  it("reports the output pixel size of a page at a DPI", () => {
    expect(outputPixels({ width: 210, height: 297 }, 300)).toEqual({ width: 2480, height: 3508 });
  });

  it("allows a DPI only while the longest side stays under the render cap", () => {
    const a1 = { width: 594, height: 841 };
    expect(isDpiAllowed(a1, 300)).toBe(true);
    expect(isDpiAllowed(a1, 600)).toBe(false);
    const a3 = { width: 297, height: 420 };
    expect(isDpiAllowed(a3, 600)).toBe(true);
  });

  it("picks the highest allowed DPI at or below the requested one", () => {
    const a1 = { width: 594, height: 841 };
    expect(clampDpi(a1, 600)).toBe(300);
    expect(clampDpi(a1, 300)).toBe(300);
    expect(clampDpi({ width: 1500, height: 1500 }, 600)).toBe(150);
  });
});
