import { describe, expect, it } from "vitest";

import {
  templateInputSchema,
  templatePageSchema,
  templatePreviewDescriptorSchema,
  templatePreviewSchema,
  templateReadSchema,
  templateUseResultSchema,
} from "@/lib/validations/template";

const input = {
  key: "node:1",
  label: "Bike network",
  mode: "ship" as const,
  layer_id: "5b7d2b0e-6c2f-4f0a-9b1a-1f9a8c1d2e3f",
  layer_type: "feature" as const,
  geometry_type: "line",
};

const template = {
  id: "5b7d2b0e-6c2f-4f0a-9b1a-1f9a8c1d2e3f",
  name: "Isochrone starter",
  description: null,
  categories: ["accessibility"],
  thumbnail_url: null,
  space_id: "5b7d2b0e-6c2f-4f0a-9b1a-1f9a8c1d2e40",
  folder_id: "5b7d2b0e-6c2f-4f0a-9b1a-1f9a8c1d2e41",
  created_by: null,
  payload_kind: "workflow",
  kinds: ["workflow"],
  inputs: [input],
  ships_sample_data: true,
  catalog_status: "published",
  source_ref: {},
  my_role: "viewer",
  created_at: "2026-09-01T10:00:00Z",
  updated_at: "2026-09-01T10:00:00Z",
};

describe("templateInputSchema", () => {
  it("parses a ship input, defaulting from_catalog", () => {
    const parsed = templateInputSchema.parse(input);
    expect(parsed.from_catalog).toBe(false);
    expect(parsed.mode).toBe("ship");
  });

  it("parses an ask input with a null layer_id", () => {
    const parsed = templateInputSchema.parse({
      ...input,
      mode: "ask",
      layer_id: null,
      layer_type: null,
      geometry_type: null,
    });
    expect(parsed.layer_id).toBeNull();
    expect(parsed.mode).toBe("ask");
  });
});

describe("templateReadSchema", () => {
  it("parses a full template row", () => {
    const parsed = templateReadSchema.parse(template);
    expect(parsed.kinds).toEqual(["workflow"]);
    expect(parsed.catalog_status).toBe("published");
    expect(parsed.inputs[0].label).toBe("Bike network");
  });

  it("defaults categories and inputs when the server omits them", () => {
    const { categories: _categories, inputs: _inputs, ...rest } = template;
    const parsed = templateReadSchema.parse(rest);
    expect(parsed.categories).toEqual([]);
    expect(parsed.inputs).toEqual([]);
  });

  it("rejects an unknown payload_kind", () => {
    expect(() => templateReadSchema.parse({ ...template, payload_kind: "dashboard" })).toThrow();
  });
});

describe("templatePreviewDescriptorSchema", () => {
  it("parses a workflow descriptor with nodes and index-pair edges", () => {
    const parsed = templatePreviewDescriptorSchema.parse({
      kind: "workflow",
      nodes: [
        { label: "Datasets", type: "dataset", x: 0, y: 0, w: 220, h: 92 },
        { label: "Buffer", type: "tool", x: 320, y: 0, w: 220, h: 108 },
      ],
      edges: [[0, 1]],
    });
    expect(parsed.kind).toBe("workflow");
    if (parsed.kind !== "workflow") throw new Error("expected a workflow descriptor");
    expect(parsed.nodes).toHaveLength(2);
    expect(parsed.edges).toEqual([[0, 1]]);
  });

  it("defaults a workflow descriptor's lists", () => {
    const parsed = templatePreviewDescriptorSchema.parse({ kind: "workflow" });
    if (parsed.kind !== "workflow") throw new Error("expected a workflow descriptor");
    expect(parsed.nodes).toEqual([]);
    expect(parsed.edges).toEqual([]);
  });

  it("parses a layout descriptor with its page and elements", () => {
    const parsed = templatePreviewDescriptorSchema.parse({
      kind: "layout",
      orientation: "landscape",
      page: { width: 297, height: 210 },
      elements: [{ type: "map", x: 10, y: 10, width: 200, height: 150 }],
    });
    if (parsed.kind !== "layout") throw new Error("expected a layout descriptor");
    expect(parsed.orientation).toBe("landscape");
    expect(parsed.elements[0].type).toBe("map");
  });

  it("parses a project descriptor's layer count", () => {
    const parsed = templatePreviewDescriptorSchema.parse({ kind: "project", layers: 4 });
    if (parsed.kind !== "project") throw new Error("expected a project descriptor");
    expect(parsed.layers).toBe(4);
  });

  it("rejects a kind it does not know", () => {
    expect(() => templatePreviewDescriptorSchema.parse({ kind: "builder" })).toThrow();
  });
});

describe("templateReadSchema page", () => {
  it("carries the page a layout template stores", () => {
    const parsed = templateReadSchema.parse({
      ...template,
      page_size: "A3",
      page_orientation: "landscape",
    });
    expect([parsed.page_size, parsed.page_orientation]).toEqual(["A3", "landscape"]);
  });

  it("leaves the page unset where the server sends none", () => {
    const parsed = templateReadSchema.parse(template);
    expect(parsed.page_size).toBeUndefined();
    expect(parsed.page_orientation).toBeUndefined();
  });

  it("reads a template saved with no page at all", () => {
    const parsed = templateReadSchema.parse({ ...template, page_size: null, page_orientation: null });
    expect(parsed.page_size).toBeNull();
  });
});

describe("templatePageSchema", () => {
  it("parses a page of templates", () => {
    const page = templatePageSchema.parse({ items: [template], total: 1 });
    expect(page.total).toBe(1);
    expect(page.items).toHaveLength(1);
  });
});

describe("templatePreviewSchema", () => {
  it("parses a preview with detected inputs and share lines", () => {
    const parsed = templatePreviewSchema.parse({
      detected_inputs: [input],
      kinds: ["workflow"],
      datasets_needing_share: [
        { layer_id: input.layer_id, name: "Bike network", from_catalog: false, current_audience: "team" },
      ],
    });
    expect(parsed.datasets_needing_share).toHaveLength(1);
  });

  it("defaults datasets_needing_share when the server omits it", () => {
    const parsed = templatePreviewSchema.parse({ detected_inputs: [], kinds: [] });
    expect(parsed.datasets_needing_share).toEqual([]);
  });

  it("tolerates an unknown field added by a concurrent backend change", () => {
    const parsed = templatePreviewSchema.parse({
      detected_inputs: [],
      kinds: [],
      datasets_needing_share: [],
      future_field: "unexpected",
    });
    expect(parsed.kinds).toEqual([]);
  });
});

describe("templateUseResultSchema", () => {
  it("parses a use result, defaulting the list fields", () => {
    const parsed = templateUseResultSchema.parse({ project_id: template.id });
    expect(parsed.added_layer_project_ids).toEqual([]);
    expect(parsed.unresolved_inputs).toEqual([]);
  });

  it("carries unresolved inputs through", () => {
    const parsed = templateUseResultSchema.parse({
      project_id: template.id,
      unresolved_inputs: [{ ...input, mode: "ask", layer_id: null }],
    });
    expect(parsed.unresolved_inputs).toHaveLength(1);
  });
});
