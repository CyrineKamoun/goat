import { describe, expect, it } from "vitest";

import { formatFeatureProperties } from "../formatFeatureProperties";

describe("formatFeatureProperties", () => {
  const layerFields = [
    { name: "area_m2", type: "number", kind: "area" },
    { name: "status", type: "string", kind: "string" },
  ] as const;

  it("formats area fields from m² to ha", () => {
    const { byColumn } = formatFeatureProperties({
      properties: { area_m2: 50000, status: "aktiv" },
      layerFields: layerFields as never,
      lang: "en",
    });
    expect(byColumn["area_m2"]).toMatch(/5([.,]\d+)?\s?ha/);
    expect(byColumn["status"]).toBe("aktiv");
  });

  it("applies fieldDecorators prefix/suffix", () => {
    const { byColumn } = formatFeatureProperties({
      properties: { count: 5 },
      layerFields: [{ name: "count", type: "number", kind: "number" }] as never,
      fieldDecorators: { count: { prefix: "≈ ", suffix: " items" } },
      lang: "en",
    });
    expect(byColumn["count"]).toBe("≈ 5 items");
  });

  it("returns label-keyed output when fieldLabels provided", () => {
    const { formatted } = formatFeatureProperties({
      properties: { status: "aktiv" },
      layerFields: [{ name: "status", type: "string", kind: "string" }] as never,
      fieldLabels: { status: "State" },
      lang: "en",
    });
    expect(formatted["State"]).toBe("aktiv");
    expect(formatted["status"]).toBeUndefined();
  });

  it("returns empty string for null / undefined values", () => {
    const { byColumn } = formatFeatureProperties({
      properties: { status: null as never },
      layerFields: [{ name: "status", type: "string", kind: "string" }] as never,
      lang: "en",
    });
    expect(byColumn["status"]).toBe("");
  });
});
