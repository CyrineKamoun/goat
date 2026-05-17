const ESCAPE_MAP: Record<string, string> = {
  "<": "&lt;",
  ">": "&gt;",
  "&": "&amp;",
  '"': "&quot;",
  "'": "&#39;",
};

export function escapeHtml(value: unknown): string {
  if (value === null || value === undefined) return "";
  return String(value).replace(/[<>&"']/g, (c) => ESCAPE_MAP[c]);
}

const TOKEN_RE = /\{\{(\w+)\}\}/g;
const CHIP_RE = /<span[^>]*data-field="([^"]+)"[^>]*>[\s\S]*?<\/span>/g;

// Replace data-field chips and {{name}} tokens with HTML-escaped values.
// Chips are replaced first so the `{{name}}` text inside them is consumed
// in the same pass.
export function substituteTokens(
  template: string,
  values: Record<string, unknown>,
): string {
  return template
    .replace(CHIP_RE, (_, name: string) => escapeHtml(values[name]))
    .replace(TOKEN_RE, (_, name: string) => escapeHtml(values[name]));
}

export function extractTokens(template: string): string[] {
  const found = new Set<string>();
  let m: RegExpExecArray | null;
  TOKEN_RE.lastIndex = 0;
  while ((m = TOKEN_RE.exec(template))) found.add(m[1]);
  CHIP_RE.lastIndex = 0;
  while ((m = CHIP_RE.exec(template))) found.add(m[1]);
  return Array.from(found);
}
