import { describe, expect, it } from "vitest";

import { DEFAULT_ATTRIBUTIONS, buildAttributionParts } from "../Attribution";

describe("buildAttributionParts", () => {
  it("puts custom attribution in madeWith and sources in dataSources", () => {
    const parts = buildAttributionParts(
      ['<a href="x">Made with GOAT</a>'],
      null,
      ["© MapTiler © OpenStreetMap contributors"],
    );
    expect(parts.madeWith).toEqual(['<a href="x">Made with GOAT</a>']);
    expect(parts.dataSources).toEqual(["© MapTiler © OpenStreetMap contributors"]);
  });

  it("includes extraAttribution in dataSources and trims/drops empties", () => {
    const parts = buildAttributionParts("GOAT", "  ", ["  ", "OSM"]);
    expect(parts.madeWith).toEqual(["GOAT"]);
    expect(parts.dataSources).toEqual(["OSM"]);
  });

  it("substring-dedupes dataSources keeping the longer superstring", () => {
    const parts = buildAttributionParts("GOAT", null, ["© OSM", "© OSM contributors"]);
    expect(parts.dataSources).toEqual(["© OSM contributors"]);
  });

  it("default attributions no longer include MapLibre", () => {
    expect(DEFAULT_ATTRIBUTIONS.join(" ")).toContain("Made with GOAT");
    expect(DEFAULT_ATTRIBUTIONS.join(" ").toLowerCase()).not.toContain("maplibre");
  });
});
