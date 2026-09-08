import { describe, expect, it } from "vitest";

import { datasetPeriod, itemPeriod } from "@/lib/catalog/period";
import type { CatalogCollection, CatalogItem } from "@/lib/validations/catalog";

/**
 * The three ways a catalog row can state when its data is from, and the one
 * failure that motivated this module: reading `properties.datetime` alone shows
 * *no* date for a dataset that covers a period, because STAC requires
 * `datetime` be null exactly when `start_datetime`/`end_datetime` are used.
 */

const item = (properties: Partial<CatalogItem["properties"]>): CatalogItem => ({
  type: "Feature",
  id: "layer-1",
  properties: {
    title: "Layer",
    "goat:layerType": "feature",
    ...properties,
  },
  assets: {},
  links: [],
});

const collection = (interval?: (string | null)[][]): CatalogCollection => ({
  type: "Collection",
  id: "src-1",
  links: [],
  ...(interval ? { extent: { temporal: { interval } } } : {}),
});

describe("itemPeriod", () => {
  it("reads an instant", () => {
    expect(itemPeriod(item({ datetime: "2015-06-01T00:00:00Z" }))).toEqual({
      start: "2015-06-01T00:00:00Z",
      end: "2015-06-01T00:00:00Z",
    });
  });

  it("reads a range whose datetime is null, as the spec requires", () => {
    expect(
      itemPeriod(
        item({
          datetime: null,
          start_datetime: "2014-01-01T00:00:00Z",
          end_datetime: "2021-12-31T00:00:00Z",
        })
      )
    ).toEqual({ start: "2014-01-01T00:00:00Z", end: "2021-12-31T00:00:00Z" });
  });

  it("keeps an open bound open rather than closing it", () => {
    expect(itemPeriod(item({ start_datetime: "2020-01-01T00:00:00Z" }))).toEqual({
      start: "2020-01-01T00:00:00Z",
      end: undefined,
    });
  });

  it("has nothing to say about an undated item", () => {
    expect(itemPeriod(item({}))).toBeUndefined();
    expect(itemPeriod(item({ datetime: null }))).toBeUndefined();
  });
});

describe("datasetPeriod", () => {
  it("prefers the dataset's own extent over its layers", () => {
    const period = datasetPeriod(
      collection([["2000-01-01T00:00:00Z", "2005-12-31T00:00:00Z"]]),
      [item({ datetime: "2019-01-01T00:00:00Z" })]
    );
    expect(period).toEqual({ start: "2000-01-01T00:00:00Z", end: "2005-12-31T00:00:00Z" });
  });

  it("spans the layers when the extent is the harvester's empty one", () => {
    // What today's harvest publishes on all 3,834 collections.
    const period = datasetPeriod(collection([[null, null]]), [
      item({ datetime: "2019-01-01T00:00:00Z" }),
      item({ datetime: "2012-01-01T00:00:00Z" }),
      item({ start_datetime: "2010-01-01T00:00:00Z", end_datetime: "2011-01-01T00:00:00Z" }),
    ]);
    expect(period).toEqual({ start: "2010-01-01T00:00:00Z", end: "2019-01-01T00:00:00Z" });
  });

  it("uses the first interval, which is the whole extent", () => {
    const period = datasetPeriod(
      collection([
        ["2000-01-01T00:00:00Z", "2012-12-31T00:00:00Z"],
        ["2000-01-01T00:00:00Z", "2005-12-31T00:00:00Z"],
      ]),
      []
    );
    expect(period?.end).toBe("2012-12-31T00:00:00Z");
  });

  it("is undefined when neither the dataset nor its layers say", () => {
    expect(datasetPeriod(collection(), [item({})])).toBeUndefined();
    expect(datasetPeriod(undefined)).toBeUndefined();
  });
});
