import { describe, expect, it } from "vitest";

import { clusterSchema, featureLayerPointPropertiesSchema } from "../layer";

describe("clusterSchema", () => {
  it("provides v1 defaults when given an empty object", () => {
    const parsed = clusterSchema.parse({});
    expect(parsed).toEqual({
      enabled: false,
      radius: 50,
      min_points: 3,
      max_zoom: 14,
      color: [40, 56, 178],
      text_color: [255, 255, 255],
    });
  });

  it("accepts a fully specified cluster object", () => {
    const parsed = clusterSchema.parse({
      enabled: true,
      radius: 30,
      min_points: 5,
      max_zoom: 12,
      color: [200, 0, 0],
      text_color: [0, 0, 0],
    });
    expect(parsed.enabled).toBe(true);
    expect(parsed.radius).toBe(30);
  });

  it("rejects out-of-range numeric fields", () => {
    expect(() => clusterSchema.parse({ radius: 0 })).toThrow();
    expect(() => clusterSchema.parse({ radius: 101 })).toThrow();
    expect(() => clusterSchema.parse({ min_points: 1 })).toThrow();
    expect(() => clusterSchema.parse({ min_points: 21 })).toThrow();
    expect(() => clusterSchema.parse({ max_zoom: -1 })).toThrow();
    expect(() => clusterSchema.parse({ max_zoom: 21 })).toThrow();
  });

  it("rejects color arrays of the wrong length", () => {
    expect(() => clusterSchema.parse({ color: [1, 2] })).toThrow();
    expect(() => clusterSchema.parse({ color: [1, 2, 3, 4] })).toThrow();
  });
});

describe("featureLayerPointPropertiesSchema with cluster", () => {
  it("treats cluster as optional", () => {
    const parsed = featureLayerPointPropertiesSchema.parse({
      visibility: true,
    });
    expect(parsed.cluster).toBeUndefined();
  });

  it("parses a point layer with cluster enabled", () => {
    const parsed = featureLayerPointPropertiesSchema.parse({
      visibility: true,
      cluster: { enabled: true },
    });
    expect(parsed.cluster?.enabled).toBe(true);
    expect(parsed.cluster?.radius).toBe(50);
  });
});
