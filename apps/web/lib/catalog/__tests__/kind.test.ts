import { describe, expect, it } from "vitest";

import { catalogKindOf } from "@/lib/catalog/kind";
import type { CatalogItem, CatalogItemProperties } from "@/lib/validations/catalog";

/**
 * `catalogKindOf` exists because two of the properties it reads are denormalised
 * from the dataset onto each of its layers. These cases are the bug it fixed: a
 * member of a bundle used to render as the bundle itself — badged with the sibling
 * count and offering to expand into the list it was already in.
 */
const item = (properties: Partial<CatalogItemProperties>): CatalogItem => ({
  type: "Feature",
  id: "x",
  properties: { title: "t", "goat:layerType": "feature", ...properties },
  assets: {},
  links: [],
});

describe("catalogKindOf", () => {
  it("reads a dataset standing for several layers as a bundle", () => {
    expect(catalogKindOf(item({ "goat:layerType": "bundle", "goat:member_count": 4 }))).toBe(
      "bundle"
    );
  });

  it("does not let a member of a bundle claim to be one", () => {
    // Same row, now listed inside its own bundle: member_count and layerType are
    // both inherited, and only the caller knows the context.
    const member = item({
      "goat:layerType": "bundle",
      "goat:member_count": 4,
      "goat:geometryType": "polygon",
    });
    expect(catalogKindOf(member, true)).toBe("vector");
  });

  it("settles an inherited type from the item's own fields", () => {
    expect(catalogKindOf(item({ "goat:layerType": "bundle", "goat:geometryType": "point" }), true)).toBe(
      "vector"
    );
    expect(catalogKindOf(item({ "goat:layerType": "bundle", "table:row_count": 12 }), true)).toBe(
      "table"
    );
  });

  it("reports unknown rather than guessing", () => {
    // No own geometry, no row count: could be a table or a raster, and labelling
    // it either would be a guess presented as fact.
    expect(catalogKindOf(item({ "goat:layerType": "bundle" }), true)).toBe("unknown");
  });

  it("passes through an explicit type", () => {
    expect(catalogKindOf(item({ "goat:layerType": "raster" }))).toBe("raster");
    expect(catalogKindOf(item({ "goat:layerType": "table" }))).toBe("table");
    expect(catalogKindOf(item({ "goat:layerType": "feature" }))).toBe("vector");
  });
});
