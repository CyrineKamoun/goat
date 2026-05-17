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
    ).toEqual({ id: "11111111-1111-1111-1111-111111111117", type: "divider" });
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
  it("provides defaults for an empty object", () => {
    const parsed = popupProperties.parse({});
    expect(parsed).toEqual({
      enabled: true,
      trigger: "click",
      mode: "simple",
      blocks: [],
      html: "",
      show_layer_header: true,
      position: "in_place",
      anchor: "top_right",
      highlight_active_feature: true,
    });
  });

  it("rejects unknown anchor values", () => {
    expect(() => popupProperties.parse({ anchor: "middle" })).toThrow();
  });
});
