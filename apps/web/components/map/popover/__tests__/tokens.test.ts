import { describe, expect, it } from "vitest";

import { escapeHtml, extractTokens, substituteTokens } from "../tokens";

describe("escapeHtml", () => {
  it("escapes <, >, &, \", '", () => {
    expect(escapeHtml(`<script>alert("x" & 'y')</script>`)).toBe(
      "&lt;script&gt;alert(&quot;x&quot; &amp; &#39;y&#39;)&lt;/script&gt;",
    );
  });
  it("returns empty string for null/undefined", () => {
    expect(escapeHtml(null)).toBe("");
    expect(escapeHtml(undefined)).toBe("");
  });
  it("coerces numbers and booleans", () => {
    expect(escapeHtml(42)).toBe("42");
    expect(escapeHtml(false)).toBe("false");
  });
});

describe("substituteTokens", () => {
  it("replaces {{field}} tokens with escaped values", () => {
    expect(
      substituteTokens("Hello {{name}}", { name: "<b>World</b>" }),
    ).toBe("Hello &lt;b&gt;World&lt;/b&gt;");
  });
  it("replaces span[data-field] chips with escaped values", () => {
    expect(
      substituteTokens(
        '<p>Status: <span class="field-chip" data-field="status">{{status}}</span></p>',
        { status: "aktiv" },
      ),
    ).toBe("<p>Status: aktiv</p>");
  });
  it("leaves unknown tokens as empty string", () => {
    expect(substituteTokens("a {{missing}} b", { other: "x" })).toBe("a  b");
  });
  it("handles repeated tokens", () => {
    expect(substituteTokens("{{x}} and {{x}}", { x: "ok" })).toBe("ok and ok");
  });
});

describe("extractTokens", () => {
  it("returns unique field names referenced in template", () => {
    const html =
      '<p>{{a}} <span data-field="b">{{b}}</span> {{a}} {{c}}</p>';
    expect(extractTokens(html).sort()).toEqual(["a", "b", "c"]);
  });
});
