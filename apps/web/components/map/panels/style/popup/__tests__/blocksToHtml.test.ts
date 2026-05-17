import { describe, expect, it } from "vitest";

import { blocksToHtml } from "../blocksToHtml";

describe("blocksToHtml", () => {
  it("returns empty string for no blocks", () => {
    expect(blocksToHtml([])).toBe("");
  });

  it("emits a text block's html verbatim", () => {
    const out = blocksToHtml([
      { id: "1", type: "text", html: "<p>Hello <strong>{{name}}</strong></p>" } as never,
    ]);
    expect(out).toContain("<p>Hello <strong>{{name}}</strong></p>");
  });

  it("emits a fieldList as a <dl> with {{}} tokens", () => {
    const out = blocksToHtml([
      {
        id: "1", type: "fieldList", layout: "list",
        attributes: [{ name: "status", type: "string", label: "Status" }],
        collapse_after: null,
      } as never,
    ]);
    expect(out).toMatch(/<dl[\s\S]*<dt>\s*Status\s*<\/dt>\s*<dd>\s*\{\{status\}\}\s*<\/dd>/);
  });

  it("emits an <img> for an image block (field source)", () => {
    const out = blocksToHtml([
      { id: "1", type: "image", source: "field", field: "img_url", sizing: "fit", height: 140, aspect: "16/9" } as never,
    ]);
    expect(out).toContain('<img src="{{img_url}}"');
  });

  it("emits a <hr> for divider", () => {
    expect(blocksToHtml([{ id: "1", type: "divider" } as never])).toContain("<hr");
  });
});
