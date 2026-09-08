import { describe, expect, it } from "vitest";

import { favoriteDatasetId } from "@/lib/utils/catalogFavorites";

const DATASET = { id: "a5b74f88-dataset" };
const BUNDLE = { id: "2aa748de-bundle" };
const LAYER = { id: "b724737d-layer" };

describe("favoriteDatasetId", () => {
  it("saves the dataset a single-layer detail page resolved from", () => {
    // Saving `LAYER.id` here is what the catalog list could never show: it
    // queries collections by id, and a layer id is not one.
    expect(favoriteDatasetId({ collection: DATASET, item: LAYER })).toBe(DATASET.id);
  });

  it("saves the bundle a member belongs to", () => {
    expect(favoriteDatasetId({ parent: BUNDLE, item: LAYER })).toBe(BUNDLE.id);
  });

  it("falls back to the layer when it belongs to no dataset", () => {
    expect(favoriteDatasetId({ item: LAYER })).toBe(LAYER.id);
  });

  it("has nothing to save for an entry that resolved to nothing", () => {
    expect(favoriteDatasetId({})).toBeUndefined();
  });
});
