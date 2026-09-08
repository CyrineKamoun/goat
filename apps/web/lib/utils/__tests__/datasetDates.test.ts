import { describe, expect, it } from "vitest";

import { datasetUpdatedAt } from "@/lib/utils/datasetDates";

const RESTYLED_AT = "2026-08-28T19:26:50.000Z";
const DATASET_CHANGED_AT = "2026-08-20T09:00:00.000Z";
const SOURCE_UPDATED = "2026-08-16T10:55:42.000Z";

const layer = (fields: Record<string, unknown>) =>
  ({ updated_at: RESTYLED_AT, other_properties: {}, ...fields }) as never;

describe("datasetUpdatedAt", () => {
  it("ignores a restyle: an own layer reports when its data last changed", () => {
    // `updated_at` is the later of the dataset's and the project link's,
    // because the map keys its tile source on it — restyling moves it.
    expect(datasetUpdatedAt(layer({ dataset_updated_at: DATASET_CHANGED_AT }))).toBe(
      DATASET_CHANGED_AT
    );
  });

  it("falls back to the layer's own timestamp before the field existed", () => {
    expect(datasetUpdatedAt(layer({}))).toBe(RESTYLED_AT);
  });

  it("reports the provider's date for a catalog layer", () => {
    expect(
      datasetUpdatedAt(
        layer({
          dataset_updated_at: DATASET_CHANGED_AT,
          other_properties: { catalog_item: { updated: SOURCE_UPDATED } },
        })
      )
    ).toBe(SOURCE_UPDATED);
  });

  it("says nothing rather than reporting when GOAT copied the dataset", () => {
    // The row's timestamps describe the copy: promote and re-materialize both
    // move them, so a dataset published in 2022 would read "2 minutes ago".
    expect(
      datasetUpdatedAt(
        layer({
          dataset_updated_at: DATASET_CHANGED_AT,
          other_properties: { catalog_item: { title: "Gebührenzonen" } },
        })
      )
    ).toBeUndefined();
  });
});
