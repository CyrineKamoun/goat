import type { PopupBlock } from "@/lib/validations/layer";

export function blocksToHtml(blocks: PopupBlock[]): string {
  return blocks.map(blockToHtml).filter(Boolean).join("\n");
}

function blockToHtml(b: PopupBlock): string {
  switch (b.type) {
    case "text":
      return b.html;
    case "divider":
      return `<hr />`;
    case "fieldList": {
      const rows = b.attributes
        .map((a) =>
          b.layout === "table"
            ? `  <tr><td>${a.label || a.name}</td><td>{{${a.name}}}</td></tr>`
            : `  <dt>${a.label || a.name}</dt><dd>{{${a.name}}}</dd>`,
        )
        .join("\n");
      return b.layout === "table"
        ? `<table>\n  <tbody>\n${rows}\n  </tbody>\n</table>`
        : `<dl>\n${rows}\n</dl>`;
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
