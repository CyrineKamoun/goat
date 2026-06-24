import { describe, expect, it } from "vitest";

import {
  classifyBasemapLayers,
  computeStackOrder,
  prettifyLayerId,
  resolveTarget,
} from "../basemapLayers";

describe("resolveTarget", () => {
  const valid = new Set(["10", "20"]);
  it("keeps 'all'", () => expect(resolveTarget("all", valid)).toBe("all"));
  it("keeps a present target", () => expect(resolveTarget("10", valid)).toBe("10"));
  it("falls back to 'all' for a missing target", () =>
    expect(resolveTarget("99", valid)).toBe("all"));
});

describe("computeStackOrder orphaned target", () => {
  const userLayers = [{ id: "10", sublayers: ["10"] }];
  it("excludes a 'below <deleted>' layer from the order (so it stays native)", () => {
    const order = computeStackOrder(userLayers, [
      { id: "water", relation: "below", target: "999" },
    ]);
    expect(order).toEqual(["10"]);
  });
  it("still promotes an 'above <deleted>' layer to the top (fallback above-all)", () => {
    const order = computeStackOrder(userLayers, [
      { id: "labels", relation: "above", target: "999" },
    ]);
    expect(order).toEqual(["labels", "10"]);
  });
});

describe("classifyBasemapLayers", () => {
  it("classifies a symbol layer with text-field as labels", () => {
    const [info] = classifyBasemapLayers([
      { id: "road_label", type: "symbol", "source-layer": "transportation_name", layout: { "text-field": "{name}" } },
    ]);
    expect(info.category).toBe("labels");
  });

  it("classifies a non-label road line as roads", () => {
    const [info] = classifyBasemapLayers([
      { id: "road_primary", type: "line", "source-layer": "transportation" },
    ]);
    expect(info.category).toBe("roads");
  });

  it("classifies water, landuse, buildings, poi, and falls back to other", () => {
    const cats = classifyBasemapLayers([
      { id: "water", type: "fill", "source-layer": "water" },
      { id: "wood", type: "fill", "source-layer": "landcover" },
      { id: "building", type: "fill", "source-layer": "building" },
      { id: "poi_z16", type: "circle", "source-layer": "poi" },
      { id: "background", type: "background" },
    ]).map((c) => c.category);
    expect(cats).toEqual(["water", "landuse", "buildings", "poi", "other"]);
  });
});

describe("prettifyLayerId", () => {
  it("humanizes underscored ids", () => {
    expect(prettifyLayerId("road_label")).toBe("Road label");
    expect(prettifyLayerId("place_label_city")).toBe("Place label city");
  });
});

describe("computeStackOrder", () => {
  const userLayers = [
    { id: "10", sublayers: ["10", "10-stroke"] }, // top
    { id: "20", sublayers: ["20"] }, // bottom
  ];

  it("places 'above all' promoted layers at the very top", () => {
    const order = computeStackOrder(userLayers, [
      { id: "labels", relation: "above", target: "all" },
    ]);
    expect(order).toEqual(["labels", "10", "10-stroke", "20"]);
  });

  it("places 'above <user layer>' just above that layer's group", () => {
    const order = computeStackOrder(userLayers, [
      { id: "labels", relation: "above", target: "20" },
    ]);
    expect(order).toEqual(["10", "10-stroke", "labels", "20"]);
  });

  it("places 'below <user layer>' just below that layer's group", () => {
    const order = computeStackOrder(userLayers, [
      { id: "labels", relation: "below", target: "10" },
    ]);
    expect(order).toEqual(["10", "10-stroke", "labels", "20"]);
  });

  it("excludes 'below all' (default) layers from the list", () => {
    const order = computeStackOrder(userLayers, [
      { id: "water", relation: "below", target: "all" },
    ]);
    expect(order).toEqual(["10", "10-stroke", "20"]);
  });

  it("falls back to 'all' when the target user layer is missing", () => {
    const order = computeStackOrder(userLayers, [
      { id: "labels", relation: "above", target: "999" },
    ]);
    expect(order).toEqual(["labels", "10", "10-stroke", "20"]);
  });

  it("preserves input order among co-anchored promoted layers", () => {
    const order = computeStackOrder(userLayers, [
      { id: "labels", relation: "above", target: "all" },
      { id: "shields", relation: "above", target: "all" },
    ]);
    expect(order).toEqual(["labels", "shields", "10", "10-stroke", "20"]);
  });
});
