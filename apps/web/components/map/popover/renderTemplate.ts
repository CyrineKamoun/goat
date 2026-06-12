import { Liquid } from "liquidjs";

import { substituteTokens } from "./tokens";

// Single shared engine. Configured to behave like the legacy `{{field}}`
// substituter so existing popups render identically:
//  - outputEscape "escape": `{{ value }}` is HTML-escaped (same as
//    substituteTokens), with DOMPurify still sanitizing the final markup.
//  - strictVariables/strictFilters false: unknown fields/filters render empty
//    instead of throwing, matching the forgiving substituteTokens behaviour.
// No filesystem/include support is configured, so `{% include %}`/`{% render %}`
// simply fail to parse and fall back — browser templates can't read disk.
const engine = new Liquid({
  outputEscape: "escape",
  strictVariables: false,
  strictFilters: false,
  jekyllInclude: false,
});

/**
 * Render a popup HTML template against a feature's (formatted) field values.
 *
 * Supports LiquidJS — `{{ field }}` interpolation plus logic like
 * `{% if %}`, `{% for %}`, and filters (`{{ field | upcase }}`). `{{field}}`
 * is the shared syntax with the legacy substituter, so popups authored before
 * Liquid keep working unchanged.
 *
 * Falls back to plain `substituteTokens` when the template fails to parse —
 * e.g. while the user is mid-typing an `{% if %}` in the editor — so the
 * live preview and on-map popup never break on a half-written tag.
 */
export function renderTemplate(template: string, values: Record<string, unknown>): string {
  try {
    // Normalize empty strings to null so `{% if field %}` reads as "the
    // feature has a value for this field" — the common popup case. In raw
    // Liquid an empty string is truthy (only nil/false are falsy), which
    // would surprise users trying to hide empty rows. `{{field}}` still
    // renders empty for these.
    const scope: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(values)) scope[k] = v === "" ? null : v;
    return engine.parseAndRenderSync(template, scope);
  } catch {
    return substituteTokens(template, values);
  }
}
