import { describe, expect, it } from "vitest";

import { buildBundleEditPayload } from "@/lib/api/bundleEdits";

import type { PendingFeature } from "@/lib/store/featureEditor/types";

const line: GeoJSON.Geometry = { type: "LineString", coordinates: [] };

const pending: Record<string, PendingFeature> = {
  a: {
    id: "1",
    drawFeatureId: null,
    geometry: line,
    properties: { class: "residential", _drawId: "x" },
    committed: true,
    action: "create",
  },
  b: {
    id: "2",
    drawFeatureId: null,
    geometry: line,
    properties: { class: "service" },
    committed: true,
    action: "update",
  },
  c: {
    id: "3",
    drawFeatureId: null,
    geometry: null,
    properties: {},
    committed: true,
    action: "delete",
  },
  d: {
    id: "4",
    drawFeatureId: null,
    geometry: line,
    properties: { class: "track" },
    committed: false,
    action: "create",
  },
};

describe("buildBundleEditPayload", () => {
  it("splits committed edits by action", () => {
    const payload = buildBundleEditPayload(pending, 7);
    expect(payload.create).toHaveLength(1);
    expect(payload.update).toHaveLength(1);
    expect(payload.delete).toEqual(["3"]);
  });

  it("carries the base revision so the server can detect a conflict", () => {
    expect(buildBundleEditPayload(pending, 7).base_revision).toBe(7);
  });

  it("ignores uncommitted edits", () => {
    const payload = buildBundleEditPayload(pending, 7);
    expect(payload.create.some((f) => f.properties.class === "track")).toBe(false);
  });

  it("strips the editor's internal underscore properties", () => {
    const payload = buildBundleEditPayload(pending, 7);
    expect(Object.keys(payload.create[0].properties)).toEqual(["class"]);
  });

  it("keeps the feature id on an update so the server can find the row", () => {
    expect(buildBundleEditPayload(pending, 7).update[0].id).toBe("2");
  });

  it("turns an empty string into null rather than sending it", () => {
    const withBlank: Record<string, PendingFeature> = {
      a: { ...pending.a, properties: { class: "residential", name: "" } },
    };
    expect(buildBundleEditPayload(withBlank, 0).create[0].properties.name).toBeNull();
  });
});
