import { describe, expect, it } from "vitest";

import { sanitizePopupHtml } from "../sanitize";

describe("sanitizePopupHtml", () => {
  it("strips <script> tags", () => {
    expect(sanitizePopupHtml('<p>ok</p><script>alert(1)</script>'))
      .toBe("<p>ok</p>");
  });

  it("strips on* event handlers", () => {
    const out = sanitizePopupHtml('<img src="x" onerror="alert(1)">');
    expect(out).not.toContain("onerror");
    expect(out).toContain("<img");
    expect(out).toContain('src="x"');
  });

  it("strips javascript: hrefs", () => {
    const out = sanitizePopupHtml('<a href="javascript:alert(1)">x</a>');
    expect(out).not.toContain("javascript:");
  });

  it("preserves common formatting tags", () => {
    const input =
      "<h3>t</h3><p><strong>a</strong> <em>b</em> <u>c</u></p>" +
      '<ul><li>x</li></ul><a href="https://example.com">l</a>';
    const out = sanitizePopupHtml(input);
    expect(out).toContain("<h3>t</h3>");
    expect(out).toContain("<strong>a</strong>");
    expect(out).toContain("<em>b</em>");
    expect(out).toContain("<u>c</u>");
    expect(out).toContain("<li>x</li>");
    expect(out).toContain('href="https://example.com"');
  });

  it("preserves <table>/<dl> for field-list rendering", () => {
    const input =
      "<table><tbody><tr><td>k</td><td>v</td></tr></tbody></table>" +
      "<dl><dt>k</dt><dd>v</dd></dl>";
    const out = sanitizePopupHtml(input);
    expect(out).toContain("<table>");
    expect(out).toContain("<tr>");
    expect(out).toContain("<td>");
    expect(out).toContain("<dt>");
    expect(out).toContain("<dd>");
  });

  it("allows inline style attribute (for rich-text color/size)", () => {
    const out = sanitizePopupHtml('<span style="color:#f00">x</span>');
    expect(out).toContain('style="color:#f00"');
  });
});
