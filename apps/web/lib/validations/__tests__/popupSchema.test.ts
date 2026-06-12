import { describe, expect, it } from "vitest";

import { popupBlock, popupProperties } from "../layer";

describe("popupBlock", () => {
  it("parses a text block with defaults", () => {
    const parsed = popupBlock.parse({
      id: "11111111-1111-1111-1111-111111111111",
      type: "text",
    });
    expect(parsed).toEqual({
      id: "11111111-1111-1111-1111-111111111111",
      type: "text",
      html: "",
    });
  });

  it("parses a fieldList block with collapse_after and layout default", () => {
    const parsed = popupBlock.parse({
      id: "11111111-1111-1111-1111-111111111112",
      type: "fieldList",
      attributes: [{ name: "status", type: "string" }],
      collapse_after: 4,
    });
    if (parsed.type !== "fieldList") throw new Error("expected fieldList");
    expect(parsed.layout).toBe("table");
    expect(parsed.collapse_after).toBe(4);
    expect(parsed.attributes).toHaveLength(1);
  });

  it("rejects collapse_after below 1 or above 50", () => {
    const base = {
      id: "11111111-1111-1111-1111-111111111113",
      type: "fieldList" as const,
      attributes: [],
    };
    expect(() => popupBlock.parse({ ...base, collapse_after: 0 })).toThrow();
    expect(() => popupBlock.parse({ ...base, collapse_after: 51 })).toThrow();
    const nullable = popupBlock.parse({ ...base, collapse_after: null });
    if (nullable.type !== "fieldList") throw new Error("expected fieldList");
    expect(nullable.collapse_after).toBeNull();
  });

  it("parses image, button, badge, divider variants", () => {
    expect(
      popupBlock.parse({
        id: "11111111-1111-1111-1111-111111111114",
        type: "image",
      }),
    ).toMatchObject({ source: "field", sizing: "fixed", height: 140, aspect: "16/9" });

    expect(
      popupBlock.parse({
        id: "11111111-1111-1111-1111-111111111115",
        type: "button",
      }),
    ).toMatchObject({ label: "Open", url_template: "", style: "link" });

    expect(
      popupBlock.parse({
        id: "11111111-1111-1111-1111-111111111116",
        type: "badge",
        field: "status",
      }),
    ).toMatchObject({ field: "status", palette: {} });

    expect(
      popupBlock.parse({
        id: "11111111-1111-1111-1111-111111111117",
        type: "divider",
      }),
    ).toEqual({ id: "11111111-1111-1111-1111-111111111117", type: "divider", thickness: 1 });
  });

  it("rejects badge block with empty field", () => {
    expect(() =>
      popupBlock.parse({
        id: "11111111-1111-1111-1111-111111111118",
        type: "badge",
        field: "",
      }),
    ).toThrow();
  });
});

describe("popupProperties", () => {
  it("provides unified-layout defaults for an empty object", () => {
    const parsed = popupProperties.parse({});
    expect(parsed).toMatchObject({
      enabled: true,
      trigger: "click",
      mode: "simple",
      blocks: [],
      html: "",
      layout: "popup",
      header: "standard",
      highlight_active_feature: true,
    });
    // optional fields are omitted when absent
    expect(parsed.anchor).toBeUndefined();
    expect(parsed.width).toBeUndefined();
    expect(parsed.max_height).toBeUndefined();
  });

  it("accepts valid layout/header values and rejects unknown ones", () => {
    expect(popupProperties.parse({ layout: "pinned" }).layout).toBe("pinned");
    expect(popupProperties.parse({ header: "compact" }).header).toBe("compact");
    expect(() => popupProperties.parse({ layout: "drawer" })).toThrow();
    expect(() => popupProperties.parse({ header: "tiny" })).toThrow();
  });

  it("preserves legacy position/show_layer_header so they can be migrated", () => {
    const parsed = popupProperties.parse({
      position: "fixed",
      anchor: "bottom_left",
      show_layer_header: false,
    });
    expect(parsed.position).toBe("fixed");
    expect(parsed.show_layer_header).toBe(false);
    expect(parsed.anchor).toBe("bottom_left");
  });

  it("rejects non-positive width / max_height", () => {
    expect(() => popupProperties.parse({ width: 0 })).toThrow();
    expect(() => popupProperties.parse({ max_height: -10 })).toThrow();
  });
});
