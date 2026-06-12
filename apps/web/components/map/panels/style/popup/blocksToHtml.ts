import type { PopupBlock } from "@/lib/validations/layer";

/** Minimal field shape needed to expand an "all fields" field list. */
export interface BlocksToHtmlField {
  name: string;
}

/**
 * Convert popup blocks to HTML for the one-way Simple → Advanced-HTML eject.
 *
 * `fields` is the layer's field list. It's used to expand a `fieldList` block
 * with empty `attributes` (the "show every column" sentinel that the simple
 * renderer honours) into one row per field — otherwise the eject would emit an
 * empty table for the default all-fields popup.
 */
export function blocksToHtml(blocks: PopupBlock[], fields: BlocksToHtmlField[] = []): string {
  return blocks.map((b) => blockToHtml(b, fields)).filter(Boolean).join("\n");
}

function blockToHtml(b: PopupBlock, fields: BlocksToHtmlField[]): string {
  switch (b.type) {
    case "text":
      return b.html;
    case "divider":
      return `<hr />`;
    case "fieldList": {
      // Empty `attributes` means "all columns" (see PopupBlockRenderer). Expand
      // to one entry per layer field so the ejected HTML mirrors what the simple
      // popup renders, instead of an empty table.
      const attrs: Array<{ name: string; label?: string }> =
        b.attributes.length > 0 ? b.attributes : fields.map((f) => ({ name: f.name }));
      const rows = attrs
        .map((a) =>
          b.layout === "table"
            ? `  <tr class="attr-row"><td class="attr-field">${a.label || a.name}</td><td class="attr-value">{{${a.name}}}</td></tr>`
            : `  <dt class="attr-field">${a.label || a.name}</dt><dd class="attr-value">{{${a.name}}}</dd>`,
        )
        .join("\n");
      return b.layout === "table"
        ? `<table class="attr-table">\n  <tbody>\n${rows}\n  </tbody>\n</table>`
        : `<dl class="attr-list">\n${rows}\n</dl>`;
    }
    case "image": {
      const src = b.source === "field" && b.field ? `{{${b.field}}}` : (b.url ?? "");
      if (!src) return "";
      return `<img src="${src}" alt="" />`;
    }
    case "button": {
      const cls =
        b.style === "filled"
          ? "btn-filled"
          : b.style === "outlined"
            ? "btn-outlined"
            : "btn-link";
      return `<a class="${cls}" href="${b.url_template}" target="_blank">${b.label}</a>`;
    }
    case "badge":
      return `<span class="badge">{{${b.field}}}</span>`;
  }
}
