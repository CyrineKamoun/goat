import { describe, expect, it } from "vitest";

import { sanitizePopupHtml } from "@/components/map/popover/sanitize";
import { substituteTokens } from "@/components/map/popover/tokens";

import { blocksToHtml } from "../blocksToHtml";

describe("blocksToHtml", () => {
  it("renders a fieldList table with attr-row class hooks and tokens", () => {
    const html = blocksToHtml([
      {
        id: "a",
        type: "fieldList",
        layout: "table",
        attributes: [{ name: "status", label: "Status" } as never],
        collapse_after: null,
      } as never,
    ]);
    expect(html).toContain('<tr class="attr-row">');
    expect(html).toContain('<td class="attr-field">Status</td>');
    expect(html).toContain('<td class="attr-value">{{status}}</td>');
  });

  it("renders badge and button with variant classes", () => {
    const badge = blocksToHtml([{ id: "b", type: "badge", field: "grade", palette: {}, mode: "single", full_width: false } as never]);
    expect(badge).toBe('<span class="badge">{{grade}}</span>');

    const button = blocksToHtml([
      { id: "c", type: "button", label: "Details", url_template: "{{url}}", style: "filled" } as never,
    ]);
    expect(button).toContain('class="btn-filled"');
    expect(button).toContain('href="{{url}}"');
  });

  it("never emits formatter directive classes", () => {
    const html = blocksToHtml([
      { id: "d", type: "fieldList", layout: "table", attributes: [{ name: "n", label: "N" } as never], collapse_after: null } as never,
    ]);
    expect(html).not.toContain("comma-format");
    expect(html).not.toContain("value-money");
  });

  it("produces output that survives token substitution + sanitization", () => {
    const html = blocksToHtml([
      { id: "e", type: "fieldList", layout: "list", attributes: [{ name: "city", label: "City" } as never], collapse_after: null } as never,
    ]);
    const rendered = sanitizePopupHtml(substituteTokens(html, { city: "Berlin" }));
    expect(rendered).toContain("Berlin");
    expect(rendered).toContain("City");
  });

  it("expands an empty-attributes fieldList to one row per layer field", () => {
    const html = blocksToHtml(
      [
        {
          id: "all",
          type: "fieldList",
          layout: "table",
          attributes: [],
          collapse_after: null,
        } as never,
      ],
      [{ name: "status" }, { name: "city" }],
    );
    expect(html).toContain('<td class="attr-value">{{status}}</td>');
    expect(html).toContain('<td class="attr-value">{{city}}</td>');
  });

  it("emits an empty table body when no attributes and no fields provided", () => {
    const html = blocksToHtml([
      { id: "x", type: "fieldList", layout: "table", attributes: [], collapse_after: null } as never,
    ]);
    expect(html).not.toContain("<tr");
  });

  it("returns empty string for no blocks", () => {
    expect(blocksToHtml([])).toBe("");
  });

  it("emits a text block's html verbatim", () => {
    expect(blocksToHtml([{ id: "f", type: "text", html: "<p>Hi {{name}}</p>" } as never])).toBe(
      "<p>Hi {{name}}</p>",
    );
  });

  it("emits an <img> with a field-source token for an image block", () => {
    expect(
      blocksToHtml([{ id: "g", type: "image", source: "field", field: "photo" } as never]),
    ).toBe('<img src="{{photo}}" alt="" />');
  });

  it("emits an <hr> for a divider block", () => {
    expect(blocksToHtml([{ id: "h", type: "divider" } as never])).toBe("<hr />");
  });
});
