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

  it("preserves an inline SVG icon (shapes/paths/viewBox)", () => {
    const svg =
      '<svg viewBox="0 0 24 24" width="16" height="16" fill="#2BB381">' +
      '<path d="M13 2L4 14h6l-1 8 9-12h-6l1-8z"/></svg>';
    const out = sanitizePopupHtml(svg);
    expect(out).toContain("<svg");
    expect(out).toContain("viewBox=\"0 0 24 24\"");
    expect(out).toContain("<path");
    expect(out).toContain('fill="#2BB381"');
    expect(out).toContain('d="M13 2L4 14h6l-1 8 9-12h-6l1-8z"');
  });

  it("strips <script> inside an <svg>", () => {
    const out = sanitizePopupHtml('<svg><script>alert(1)</script><circle r="5"/></svg>');
    expect(out).not.toContain("script");
    expect(out).not.toContain("alert");
  });

  it("strips SVG event handlers", () => {
    const out = sanitizePopupHtml('<svg onload="alert(1)"><path d="M0 0"/></svg>');
    expect(out).not.toContain("onload");
    expect(out).not.toContain("alert");
  });

  it("strips <foreignObject> (HTML smuggling vector)", () => {
    const out = sanitizePopupHtml(
      '<svg><foreignObject><img src=x onerror="alert(1)"></foreignObject></svg>',
    );
    expect(out.toLowerCase()).not.toContain("foreignobject");
    expect(out).not.toContain("onerror");
  });
});
