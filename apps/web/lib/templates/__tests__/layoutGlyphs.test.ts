import { describe, expect, it } from "vitest";

import type { GlyphBox, GlyphShape } from "@/lib/templates/layoutGlyphs";
import { FILLED_ELEMENTS, layoutGlyph } from "@/lib/templates/layoutGlyphs";
import { reportElementTypes } from "@/lib/validations/reportLayout";

const box: GlyphBox = { x: 40, y: 20, w: 160, h: 100 };

const round = (value: number): number => Math.round(value * 100) / 100;

/** Every element type a layout descriptor can carry that the scaffold draws
 * a picture for. The names are `PrintElement["type"]`. */
const DRAWN = [
  "map",
  "legend",
  "text",
  "metadata",
  "image",
  "table",
  "chart",
  "histogram_chart",
  "categories_chart",
  "pie_chart",
  "scalebar",
  "north_arrow",
  "qr_code",
  "divider",
];

/** The box a shape occupies, for the shapes whose extent is read straight
 * off their fields. A path's own extent is not parsed — an arc's parameters
 * are not all coordinates — so a path is checked by being drawn at all. */
const extentOf = (shape: GlyphShape): { x0: number; y0: number; x1: number; y1: number } | null => {
  switch (shape.shape) {
    case "rect":
      return { x0: shape.x, y0: shape.y, x1: shape.x + shape.w, y1: shape.y + shape.h };
    case "line":
      return {
        x0: Math.min(shape.x1, shape.x2),
        y0: Math.min(shape.y1, shape.y2),
        x1: Math.max(shape.x1, shape.x2),
        y1: Math.max(shape.y1, shape.y2),
      };
    case "circle":
      return {
        x0: shape.cx - shape.r,
        y0: shape.cy - shape.r,
        x1: shape.cx + shape.r,
        y1: shape.cy + shape.r,
      };
    default:
      return null;
  }
};

describe("layoutGlyph", () => {
  it("covers every element type the layout builder can place", () => {
    // A new element type reaches a template preview the moment the builder
    // can place it, so this list is the builder's own.
    expect(new Set(DRAWN)).toEqual(new Set(reportElementTypes.options));
  });

  it("draws a picture for every element type a layout carries", () => {
    for (const type of DRAWN) {
      expect(layoutGlyph(type, box).length, type).toBeGreaterThan(0);
    }
  });

  it("keeps every shape inside the element's own box", () => {
    // A hairline is centred on its coordinate, so half of it may fall
    // outside — the tolerance is the widest line any glyph draws.
    const tolerance = 3;
    for (const type of DRAWN) {
      for (const shape of layoutGlyph(type, box)) {
        const extent = extentOf(shape);
        if (!extent) continue;
        expect(extent.x0, type).toBeGreaterThanOrEqual(box.x - tolerance);
        expect(extent.y0, type).toBeGreaterThanOrEqual(box.y - tolerance);
        expect(extent.x1, type).toBeLessThanOrEqual(box.x + box.w + tolerance);
        expect(extent.y1, type).toBeLessThanOrEqual(box.y + box.h + tolerance);
      }
    }
  });

  it("is the same drawing every time, so a preview does not change under the reader", () => {
    for (const type of DRAWN) {
      expect(layoutGlyph(type, box)).toEqual(layoutGlyph(type, box));
    }
  });

  it("scales with the box it is drawn in", () => {
    const small = layoutGlyph("map", box);
    const large = layoutGlyph("map", { x: 0, y: 0, w: 640, h: 400 });

    expect(large).toHaveLength(small.length);
    expect(large).not.toEqual(small);
  });

  it("draws nothing for an element type it knows no picture for", () => {
    expect(layoutGlyph("something_new", box)).toEqual([]);
    expect(layoutGlyph("", box)).toEqual([]);
  });

  it("draws no code in a box too small to hold one", () => {
    // Element sizes carry no minimum, and the QR cells are derived from the
    // box less a padding that never goes below 1.
    expect(layoutGlyph("qr_code", { x: 0, y: 0, w: 1, h: 1 })).toEqual([]);
    expect(layoutGlyph("qr_code", { x: 0, y: 0, w: 40, h: 0.5 })).toEqual([]);
    for (const shape of layoutGlyph("qr_code", { x: 0, y: 0, w: 8, h: 8 })) {
      if (shape.shape === "rect") {
        expect(shape.w).toBeGreaterThan(0);
        expect(shape.h).toBeGreaterThan(0);
      }
    }
  });

  it("draws nothing in a box with no area", () => {
    expect(layoutGlyph("map", { x: 0, y: 0, w: 0, h: 100 })).toEqual([]);
    expect(layoutGlyph("map", { x: 0, y: 0, w: 100, h: -1 })).toEqual([]);
  });

  it("reads a paragraph as a stack of lines whose last one is short", () => {
    const lines = layoutGlyph("text", box);

    expect(lines).toHaveLength(4);
    const widths = lines.map((shape) => (shape.shape === "rect" ? shape.w : 0));
    expect(Math.min(...widths)).toBe(widths[widths.length - 1]);
  });

  it("draws a legend as one swatch and one label rule per row", () => {
    const legend = layoutGlyph("legend", box);

    // Four rows, each a square swatch and the rule beside it.
    expect(legend).toHaveLength(8);
    expect(legend.every((shape) => shape.shape === "rect")).toBe(true);
    const swatches = legend.filter((_shape, index) => index % 2 === 0);
    expect(swatches.every((shape) => shape.shape === "rect" && shape.w === shape.h)).toBe(true);
    // The rows are evenly spaced, and the label rules are not all one length
    // — a column of identical bars does not read as text.
    const tops = swatches.map((shape) => (shape.shape === "rect" ? shape.y : 0));
    const steps = tops.slice(1).map((top, index) => round(top - tops[index]));
    expect(new Set(steps).size).toBe(1);
    const rules = legend.filter((_shape, index) => index % 2 === 1);
    expect(new Set(rules.map((shape) => (shape.shape === "rect" ? shape.w : 0))).size).toBeGreaterThan(1);
  });

  it("draws fewer legend rows in a box too short for them", () => {
    const short = layoutGlyph("legend", { x: 0, y: 0, w: 60, h: 12 });

    expect(short.length).toBeLessThan(8);
    expect(short.length).toBeGreaterThan(0);
    // No sliver rows: what is drawn keeps the swatch square.
    expect(short.every((shape) => shape.shape === "rect" && shape.h > 0)).toBe(true);
  });

  it("draws the three chart types as bars on a baseline", () => {
    for (const type of ["chart", "histogram_chart", "categories_chart"]) {
      const bars = layoutGlyph(type, box);
      expect(
        bars.filter((shape) => shape.shape === "rect"),
        type
      ).toHaveLength(5);
      expect(
        bars.filter((shape) => shape.shape === "line"),
        type
      ).toHaveLength(1);
    }
  });

  it("marks the element types drawn on a ground of their own", () => {
    // The content blocks, as opposed to the page's furniture.
    expect(FILLED_ELEMENTS.has("map")).toBe(true);
    expect(FILLED_ELEMENTS.has("pie_chart")).toBe(true);
    expect(FILLED_ELEMENTS.has("divider")).toBe(false);
    expect(FILLED_ELEMENTS.has("scalebar")).toBe(false);
  });
});
