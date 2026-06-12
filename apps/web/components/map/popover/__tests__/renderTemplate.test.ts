import { describe, expect, it } from "vitest";

import { renderTemplate } from "../renderTemplate";

describe("renderTemplate", () => {
  it("interpolates {{field}} tokens", () => {
    expect(renderTemplate("Hi {{name}}", { name: "Bob" })).toBe("Hi Bob");
  });

  it("HTML-escapes interpolated values (XSS-safe, like substituteTokens)", () => {
    expect(renderTemplate("{{v}}", { v: "a & <b>" })).toBe("a &amp; &lt;b&gt;");
  });

  it("renders {% if %} conditionals on truthiness", () => {
    const tpl = "{% if status %}has:{{status}}{% else %}none{% endif %}";
    expect(renderTemplate(tpl, { status: "active" })).toBe("has:active");
    expect(renderTemplate(tpl, { status: "" })).toBe("none");
  });

  it("supports {% for %} loops", () => {
    expect(renderTemplate("{% for i in (1..3) %}x{% endfor %}", {})).toBe("xxx");
  });

  it("supports filters", () => {
    expect(renderTemplate("{{ name | upcase }}", { name: "bob" })).toBe("BOB");
  });

  it("renders unknown variables as empty (non-strict)", () => {
    expect(renderTemplate("[{{missing}}]", {})).toBe("[]");
  });

  it("falls back to plain token substitution on malformed Liquid", () => {
    // `{% if %}` with no matching endif throws at parse time — must not crash;
    // falls back so {{name}} is still substituted and literal tags survive.
    const out = renderTemplate("{{name}} {% if broken %}", { name: "Bob" });
    expect(out).toContain("Bob");
  });
});
