import { describe, expect, it } from "vitest";

import { hasLegend } from "@/lib/utils/datasets";
import type { Layer } from "@/lib/validations/layer";

const layer = (overrides: Partial<Layer>): Layer => ({ ...overrides }) as Layer;

/** An ordinal fill with `count` classes, the shape the style panel writes. */
const ordinalFill = (count: number) => ({
  filled: true,
  color_field: { name: "kind", type: "string" },
  color_scale: "ordinal",
  color_range: {
    color_map: Array.from({ length: count }, (_, index) => [[`class-${index}`], "#112233"]),
  },
});

describe("hasLegend", () => {
  it("says no for a feature layer painted in one flat colour", () => {
    expect(hasLegend(layer({ type: "feature", properties: { color: [0, 0, 0] } }))).toBe(false);
  });

  it("says yes once a feature layer colours by a field with more than one class", () => {
    expect(hasLegend(layer({ type: "feature", properties: ordinalFill(2) }))).toBe(true);
  });

  it("says no for a single-class ordinal, which the legend panel leaves out", () => {
    expect(hasLegend(layer({ type: "feature", properties: ordinalFill(1) }))).toBe(false);
  });

  it("says no where the fill is off, whatever the colour field says", () => {
    expect(hasLegend(layer({ type: "feature", properties: { ...ordinalFill(3), filled: false } }))).toBe(
      false
    );
  });

  it("says yes for a raster that carries a style", () => {
    expect(hasLegend(layer({ type: "raster", properties: { style: { style_type: "image" } } }))).toBe(true);
  });

  it("says no for a raster without one", () => {
    expect(hasLegend(layer({ type: "raster", properties: {} }))).toBe(false);
  });
});
