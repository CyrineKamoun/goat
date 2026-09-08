import { describe, expect, it } from "vitest";

import { type CatalogStyle, catalogPaint } from "@/lib/catalog/style";

/**
 * The catalog's published styles, turned into what the preview map draws with.
 *
 * The translation itself is the app's own (`transformToMapboxLayerStyleSpec`) —
 * deliberately, so a preview cannot diverge from the map that renders the same
 * dataset once it is added. What is worth pinning here is the boundary: which
 * published objects reach that transformer, what shape comes back for each
 * geometry, and that a malformed style degrades to "no style" rather than
 * throwing inside a map render.
 */

const style = (extra: Record<string, unknown> = {}): CatalogStyle =>
  ({
    color: [102, 194, 165],
    filled: true,
    stroked: false,
    opacity: 0.8,
    visibility: true,
    ...extra,
  }) as CatalogStyle;

describe("catalogPaint", () => {
  it("draws points as circles in the dataset's colour", () => {
    const paint = catalogPaint(style(), "point");
    expect(paint?.type).toBe("circle");
    expect(paint?.paint["circle-color"]).toBe("#66C2A5");
    expect(paint?.paint["circle-opacity"]).toBe(0.8);
  });

  it("draws areas as a fill", () => {
    const paint = catalogPaint(style(), "polygon");
    expect(paint?.type).toBe("fill");
    expect(paint?.paint["fill-color"]).toBe("#66C2A5");
  });

  it("draws linework as a line", () => {
    const paint = catalogPaint(style({ stroke_color: [217, 25, 85], stroke_width: 3 }), "line");
    expect(paint?.type).toBe("line");
    expect(paint?.paint["line-color"]).toBe("#D91955");
  });

  it("carries a data-driven colour ramp through as an expression", () => {
    // The published styles really do this — `color_field` + `color_range` is a
    // choropleth — and the preview sample carries the attributes it needs, so
    // the ramp works on the sample rather than being flattened to one colour.
    const paint = catalogPaint(
      style({
        color_field: { name: "measure", type: "number" },
        color_scale: "quantile",
        color_range: {
          name: "YlGnBu",
          type: "sequential",
          colors: ["#ffffcc", "#c7e9b4", "#7fcdbb"],
        },
        color_scale_breaks: { breaks: [10, 20], min: 0, max: 30 },
      }),
      "polygon"
    );
    expect(Array.isArray(paint?.paint["fill-color"])).toBe(true);
    expect(JSON.stringify(paint?.paint["fill-color"])).toContain("measure");
  });

  it("has nothing to draw without a style or a geometry type", () => {
    expect(catalogPaint(undefined, "point")).toBeUndefined();
    expect(catalogPaint(style(), undefined)).toBeUndefined();
  });

  it("declines a geometry the map has no shape for", () => {
    // A raster's style is not a vector paint spec, and 108 of 10,793 layers
    // state no geometry type at all.
    expect(catalogPaint(style(), "raster")).toBeUndefined();
    expect(catalogPaint(style(), "")).toBeUndefined();
  });

  it("degrades a malformed style to no style instead of throwing", () => {
    // The object is published upstream, so a field of the wrong type must not be
    // able to take a map render down with it.
    expect(
      catalogPaint({ color: "not-a-colour" } as unknown as CatalogStyle, "point")
    ).toBeDefined();
    expect(
      catalogPaint({ color_range: { colors: null } } as unknown as CatalogStyle, "polygon")
    ).toBeDefined();
  });
});
