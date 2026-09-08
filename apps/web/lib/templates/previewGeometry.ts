import { PAGE_SIZES } from "@/lib/print/units";
import type {
  TemplateLayoutPreview,
  TemplatePreviewElementStyle,
  TemplatePreviewViewState,
  TemplateWorkflowPreview,
} from "@/lib/validations/template";

/** A box in the frame the geometry was fitted into. */
export interface PreviewBox {
  x: number;
  y: number;
  w: number;
  h: number;
}

/** A workflow node's box, plus what the drawing writes in it. */
export interface PreviewNodeBox extends PreviewBox {
  label: string;
  type: string;
  /** Which of the node type's icons to draw — see `templatePreviewNodeSchema`. */
  icon?: string | null;
  /** The colour the node is painted in, for the one type that carries one —
   * see `templatePreviewNodeSchema`. */
  color?: string | null;
  /** An annotation's own note markup, sanitised — see
   * `templatePreviewNodeSchema`. */
  html?: string | null;
}

/** A layout element's box, plus the element type the drawing styles it by and
 * the payload that type carries — see `templatePreviewElementSchema`. */
export interface PreviewElementBox extends PreviewBox {
  type: string;
  /** A map frame's frozen camera. */
  viewState?: TemplatePreviewViewState | null;
  /** A text block's own markup, sanitised. */
  html?: string | null;
  /** The size that markup asks for, in the layout canvas's own pixels. */
  fontSize?: number | null;
  /** A legend's own title. */
  title?: string | null;
  /** What the element is framed and filled with. */
  style?: TemplatePreviewElementStyle | null;
}

/** One edge, as the `d` of a cubic bezier from the source node's
 * right-centre to the target node's left-centre. */
export interface PreviewEdgePath {
  d: string;
}

export interface WorkflowGeometry {
  nodes: PreviewNodeBox[];
  edges: PreviewEdgePath[];
  /** The factor the canvas was scaled by to fit the frame, so a drawing can
   * scale a node's furniture — padding, an icon box, a font size — by the
   * same amount its boxes were scaled by. `1` when there is nothing to fit. */
  scale: number;
}

export interface LayoutGeometry {
  page: PreviewBox;
  elements: PreviewElementBox[];
  /** The frame's pixels per millimetre of page, so a drawing can convert a
   * size the layout canvas carries in its own pixels — a text block's font —
   * into the frame the page was fitted into. */
  scale: number;
}

const round = (value: number): number => Math.round(value * 100) / 100;

const round1 = (value: number): number => Math.round(value * 10) / 10;

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

/** `value` as a number, or null where it is not one — the mirror of
 * `_number` in `core.templates.snapshot`, which counts booleans out too. */
const numberOf = (value: unknown): number | null =>
  typeof value === "number" && Number.isFinite(value) ? value : null;

/**
 * The box a node occupies on the workflow canvas, per node type — the
 * mirror of `_NODE_SIZES` in `apps/core/src/core/templates/snapshot.py`, so
 * a descriptor built here matches the one `build_preview` stores for the
 * same config.
 *
 * The width is the node container's declared `minWidth` (dataset 160 in
 * `components/workflows/nodes/DatasetNode.tsx`, 220 for the shared
 * `NodeContainer` in `components/workflows/nodes/shared.tsx` that the tool,
 * export and if nodes use); the cards grow with their content, so the
 * minimum is the one deterministic width. The height is that container's
 * header row: a 2px border plus 6px of padding on each side, around the
 * 40px icon row — 56. The padding is `NodeContainer`'s `theme.spacing(1.5)`,
 * and this theme's spacing is `0.25 * factor rem`
 * (`packages/js/ui/theme/spacing.ts`), so it is 6px rather than MUI's
 * default 12. A node type outside this table (and the annotation below) is
 * one the canvas could not place, and is left out of a descriptor entirely.
 */
export const PREVIEW_NODE_SIZE: Record<string, { w: number; h: number }> = {
  dataset: { w: 160, h: 56 },
  tool: { w: 220, h: 56 },
  export: { w: 220, h: 56 },
  if: { w: 220, h: 56 },
};

/** The size an annotation is created at, and what it falls back to where it
 * carries no size of its own (`TextAnnotationNode.tsx`). */
export const TEXT_ANNOTATION_SIZE = { w: 400, h: 200 };

/** The colour an annotation is created in, and what a note with no colour of
 * its own is painted in (`TextAnnotationNode.tsx`). */
export const TEXT_ANNOTATION_COLOR = "#F2CE58";

/** A `#rgb`/`#rrggbb` value, the only thing carried as a node's colour — the
 * mirror of `_HEX_COLOR_RE` in `core.templates.snapshot`. */
const HEX_COLOR = /^#(?:[0-9a-fA-F]{3}|[0-9a-fA-F]{6})$/;

/** How many nodes or elements a descriptor carries at most — the same cap
 * (`_PREVIEW_CAP`) the backend applies when it builds one. */
export const PREVIEW_ITEM_CAP = 60;

/** The page a layout's size name stands for, in millimetres, rounded the way
 * `_PAGE_SIZES` in `core.templates.snapshot` rounds it. `Custom` and any
 * unrecognised name resolve to A4, which is what the layout canvas itself
 * draws. */
export const PREVIEW_PAGE_SIZE: Record<string, { width: number; height: number }> = Object.fromEntries(
  Object.entries(PAGE_SIZES).map(([name, size]) => [
    name,
    { width: round1(size.width), height: round1(size.height) },
  ])
);

/**
 * The size a generated snapshot is rasterised at — 16:9, the ratio every
 * thumbnail box shows.
 */
export const SNAPSHOT_SIZE = { width: 1280, height: 720 };

/** The frame the drawn scaffold is laid out in: a 16:9 viewBox the SVG
 * scales to whatever width the preview column gives it. */
export const SCAFFOLD_FRAME = { width: 480, height: 270 };

/** The padding the scaffold keeps inside its frame. */
export const SCAFFOLD_PADDING = 18;

/** The scaffold never draws a node larger than it is on the canvas: at the
 * frame's size a one-node workflow reads as one node in the middle of it
 * rather than as a box filling it. A caller with a bigger frame — the
 * 1280×720 snapshot — passes its own ceiling. */
export const SCAFFOLD_MAX_SCALE = 1;

/** How far a bezier's control point reaches along the x axis. */
const MIN_EDGE_CURVE = 18;

/** A table lookup that stops at the table's own keys, the way the backend's
 * `node_type in _NODE_SIZES` does: a node type or page size named
 * `constructor` or `toString` is not an entry. */
const tableEntry = <T>(table: Record<string, T>, key: string): T | undefined =>
  Object.prototype.hasOwnProperty.call(table, key) ? table[key] : undefined;

/**
 * As much of a label as fits `available` px at this font size, cut with an
 * ellipsis where it does not: SVG text neither wraps nor ellipsises itself,
 * so both the scaffold and the snapshot cut it here. The 0.55em average
 * glyph width is the approximation a proportional sans reads at — the label
 * is measured before any font has loaded.
 */
export const truncateLabel = (label: string, available: number, fontSize: number): string => {
  const maxChars = Math.floor(available / (fontSize * 0.55));
  if (maxChars <= 0) return "";
  if (label.length <= maxChars) return label;
  if (maxChars <= 1) return "…";
  return `${label.slice(0, maxChars - 1).trimEnd()}…`;
};

/**
 * A text annotation keeps its content in `data.text` as the rich HTML TipTap
 * writes — headings, bold/italic/underline/strike, per-run colours and font
 * families, alignment, links — and a thumbnail draws that markup rather than
 * a flattening of it. What a descriptor carries is reduced to this
 * allow-list: everything below is what a note may bring into a drawing.
 */
const NOTE_TAGS = new Set([
  "p",
  "h1",
  "h2",
  "h3",
  "h4",
  "h5",
  "h6",
  "strong",
  "b",
  "em",
  "i",
  "u",
  "s",
  "br",
  "span",
  "a",
  "ul",
  "ol",
  "li",
  "mark",
  "sub",
  "sup",
]);

const NOTE_VOID_TAGS = new Set(["br"]);

const NOTE_STYLE_PROPS = new Set([
  "color",
  "background-color",
  "font-family",
  "text-align",
  "font-weight",
  "font-style",
  "text-decoration",
]);

const NOTE_URL_SCHEMES = ["http://", "https://", "mailto:"];

/** What one note contributes to a descriptor: its visible characters, and
 * the markup around them. A preview is drawn a few hundred pixels wide, so a
 * note longer than this has nothing readable past the cap either way. */
export const NOTE_TEXT_CAP = 2000;
const NOTE_MARKUP_CAP = 6000;

/** `<script>`/`<style>` go with their content, an unclosed one to the end of
 * the string; comments, CDATA, doctypes and processing instructions go too. */
const NOTE_DROP_ELEMENT = /<(script|style)\b[\s\S]*?(?:<\/\1\s*>|$)/gi;
const NOTE_DROP_MARKUP =
  /<!--[\s\S]*?(?:-->|$)|<!\[CDATA\[[\s\S]*?(?:\]\]>|$)|<![^>]*>|<\?[\s\S]*?(?:\?>|$)/g;
const NOTE_TOKEN = /<[^>]*>|[^<]+|</g;
const NOTE_TAG = /^<\s*(\/?)\s*([a-zA-Z][a-zA-Z0-9]*)([^>]*?)\/?>$/;
const NOTE_ATTR = /([a-zA-Z-]+)\s*=\s*(?:"([^"]*)"|'([^']*)')/g;

/** The entities a TipTap fragment carries. One outside the table stays
 * literal text — its `&` is escaped, so it reads as it was written. */
const NOTE_ENTITY = /&(#\d+|#[xX][0-9a-fA-F]+|[a-zA-Z]+);/g;

const NOTE_ENTITIES: Record<string, string> = {
  amp: "&",
  lt: "<",
  gt: ">",
  quot: '"',
  apos: "'",
  nbsp: "\u00a0",
};

const noteUnescape = (text: string): string =>
  text.replace(NOTE_ENTITY, (match, entity: string) => {
    if (entity.startsWith("#")) {
      const code =
        entity[1] === "x" || entity[1] === "X" ? parseInt(entity.slice(2), 16) : Number(entity.slice(1));
      // A lone surrogate is not a character XML can carry.
      if (!Number.isFinite(code) || code <= 0 || code > 0x10ffff) return match;
      if (code >= 0xd800 && code <= 0xdfff) return match;
      return String.fromCodePoint(code);
    }
    return NOTE_ENTITIES[entity.toLowerCase()] ?? match;
  });

/** Escape for XML, which is how a `foreignObject`'s content is parsed inside
 * a standalone SVG: the text is unescaped first, so only the entities XML
 * itself defines are ever written back. */
const noteEscape = (text: string, quotes = false): string => {
  const escaped = text.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
  return quotes ? escaped.replace(/"/g, "&quot;") : escaped;
};

/** The declarations of a `style` attribute that survive the allow-list. A
 * property outside `NOTE_STYLE_PROPS` is dropped, and so is any value
 * carrying a function call or markup — `url(...)` above all, which would
 * make the drawing fetch something. */
const noteStyle = (value: string): string | null => {
  const kept: string[] = [];
  for (const declaration of value.split(";")) {
    const colon = declaration.indexOf(":");
    if (colon < 0) continue;
    const prop = declaration.slice(0, colon).trim().toLowerCase();
    const raw = declaration.slice(colon + 1).trim();
    if (!prop || !raw || !NOTE_STYLE_PROPS.has(prop)) continue;
    const lowered = raw.toLowerCase();
    if (lowered.includes("url(") || lowered.includes("expression(") || raw.includes("@")) continue;
    if (/[<>"'\\]/.test(raw)) continue;
    kept.push(`${prop}: ${raw}`);
  }
  return kept.length > 0 ? kept.join("; ") : null;
};

/** The attributes an allowed tag keeps: `href` on a link, pointing at
 * http/https/mailto, and a filtered `style`. Everything else — an `onclick`,
 * a `class`, a `src` — is dropped. */
const noteAttributes = (name: string, raw: string): string => {
  let href: string | null = null;
  let style: string | null = null;
  for (const attr of raw.matchAll(NOTE_ATTR)) {
    const key = attr[1].toLowerCase();
    const value = attr[2] ?? attr[3] ?? "";
    if (key === "href" && name === "a") {
      const candidate = noteUnescape(value).trim();
      if (NOTE_URL_SCHEMES.some((scheme) => candidate.toLowerCase().startsWith(scheme))) {
        href = candidate;
      }
    } else if (key === "style") {
      style = noteStyle(noteUnescape(value));
    }
  }
  let rendered = "";
  if (href !== null) rendered += ` href="${noteEscape(href, true)}"`;
  if (style !== null) rendered += ` style="${noteEscape(style, true)}"`;
  return rendered;
};

/**
 * A text annotation's own HTML, reduced to what a thumbnail may draw.
 *
 * Keeps the tags and attributes above — the headings, emphasis, per-run
 * `span` styles, alignment and links TipTap writes — and drops everything
 * else: `<script>`/`<style>` with their content, event-handler attributes,
 * and any tag the allow-list does not name (its text is kept). Tags left
 * open are closed, a stray close is dropped, and the text is capped at
 * `NOTE_TEXT_CAP` visible characters. Null where the note shows nothing —
 * the empty `<p></p>` a new annotation carries — and for a `data.text` that
 * is not a string.
 *
 * Well-formed XML by construction, since a `foreignObject` in the standalone
 * SVG the PNG is rasterised from is parsed as XML: every tag is closed and
 * only XML's own five entities are written.
 */
export const sanitizeNoteHtml = (value: unknown): string | null => {
  if (typeof value !== "string" || !value) return null;
  const cleaned = value.replace(NOTE_DROP_ELEMENT, "").replace(NOTE_DROP_MARKUP, "");

  const out: string[] = [];
  const stack: string[] = [];
  let textLength = 0;
  let markupLength = 0;
  let hasText = false;
  for (const match of cleaned.matchAll(NOTE_TOKEN)) {
    if (markupLength >= NOTE_MARKUP_CAP) break;
    const token = match[0];
    const tag = token.length > 1 ? NOTE_TAG.exec(token) : null;
    if (tag) {
      const name = tag[2].toLowerCase();
      if (!NOTE_TAGS.has(name)) continue;
      if (tag[1]) {
        if (NOTE_VOID_TAGS.has(name) || !stack.includes(name)) continue;
        while (stack.length > 0) {
          const open = stack.pop() as string;
          out.push(`</${open}>`);
          markupLength += open.length + 3;
          if (open === name) break;
        }
        continue;
      }
      if (NOTE_VOID_TAGS.has(name)) {
        out.push("<br />");
        markupLength += 6;
        continue;
      }
      const rendered = noteAttributes(name, tag[3]);
      stack.push(name);
      out.push(`<${name}${rendered}>`);
      markupLength += name.length + rendered.length + 2;
      continue;
    }
    if (textLength >= NOTE_TEXT_CAP) continue;
    const plain = noteUnescape(token).slice(0, NOTE_TEXT_CAP - textLength);
    textLength += plain.length;
    hasText = hasText || plain.trim().length > 0;
    const escaped = noteEscape(plain);
    out.push(escaped);
    markupLength += escaped.length;
  }
  while (stack.length > 0) out.push(`</${stack.pop() as string}>`);
  return hasText ? out.join("") : null;
};

/**
 * The box a workflow node occupies, or null for a type the canvas has no
 * node for. An annotation is resizable, so its own size wins: `data.width`
 * /`data.height`, else the measurements ReactFlow stored on the node, else
 * the 400×200 it was created at — the same order `_node_size` reads them in.
 */
export const previewNodeSize = (
  type: string,
  node?: Record<string, unknown> | null
): { w: number; h: number } | null => {
  if (type !== "textAnnotation") return tableEntry(PREVIEW_NODE_SIZE, type) ?? null;
  const data = isRecord(node?.data) ? node.data : {};
  const width = numberOf(data.width) || numberOf(node?.width);
  const height = numberOf(data.height) || numberOf(node?.height);
  return { w: width || TEXT_ANNOTATION_SIZE.w, h: height || TEXT_ANNOTATION_SIZE.h };
};

/** The geometries a dataset node's icon is named by — the cases
 * `DatasetNode.getGeometryIcon` switches on. Anything else falls through to
 * the table glyph, so it is named `table` here. */
const GEOMETRY_ICONS = new Set(["point", "line", "polygon"]);

/**
 * The icon a node carries in a descriptor, from the same node data the
 * canvas reads it off — the mirror of what `build_preview` stores: a
 * dataset's geometry, a tool's process id, the export glyph, the branch.
 * Null for a node type that shows no icon.
 */
export const previewNodeIcon = (type: string, data?: Record<string, unknown> | null): string | null => {
  switch (type) {
    case "dataset": {
      const geometry = typeof data?.geometryType === "string" ? data.geometryType : "";
      return GEOMETRY_ICONS.has(geometry) ? geometry : "table";
    }
    case "tool":
      return typeof data?.processId === "string" && data.processId ? data.processId : null;
    case "export":
      return "export_dataset";
    case "if":
      return "if";
    default:
      return null;
  }
};

/**
 * The colour a node is painted in, for the node types that carry one — the
 * mirror of `_node_color` in `core.templates.snapshot`. Only an annotation
 * does: its own `data.backgroundColor`, which the canvas paints its 5% fill
 * and its 2px border from. Null for every other type and for a value that is
 * not a hex colour, which the drawing paints in `TEXT_ANNOTATION_COLOR`.
 */
export const previewNodeColor = (type: string, data?: Record<string, unknown> | null): string | null => {
  if (type !== "textAnnotation") return null;
  const color = data?.backgroundColor;
  return typeof color === "string" && HEX_COLOR.test(color) ? color : null;
};

/** Translates one of the canvas's own title keys. The descriptor stays
 * language-neutral, so the title is resolved where it is drawn. */
export type PreviewTranslate = (key: string, options?: { defaultValue?: string }) => string;

/**
 * The title the canvas writes beside a node's icon, for a node a descriptor
 * carries: `data.label` for a dataset (`DatasetNode`'s display name, whose
 * live project-layer lookup a descriptor cannot repeat), the translated
 * process title for a tool (`ToolNode`'s
 * `t(data.processId, { defaultValue: data.label })` — its `process.title`
 * needs a request the drawing does not make), `t("export_dataset")` for an
 * export and `IfNode`'s label-or-`t("if_node")`. An annotation is titleless:
 * its content is the note markup a descriptor carries as `html`, which the
 * drawing renders as rich text rather than as a card title. Untranslated
 * where no `translate` is given.
 */
export const previewNodeTitle = (node: PreviewNodeBox, translate?: PreviewTranslate): string => {
  // A descriptor falls a missing label back to the node type, which is a
  // word from the code rather than one to write on a card.
  const own = node.label && node.label !== node.type ? node.label : "";
  if (!translate) return own;
  switch (node.type) {
    case "tool":
      return node.icon ? translate(node.icon, { defaultValue: own || node.icon }) : own;
    case "export":
      return translate("export_dataset", { defaultValue: own || "Save as Dataset" });
    case "if":
      return own || translate("if_node", { defaultValue: "Conditional" });
    default:
      return own;
  }
};

/** Where a node type's handles sit on its box, as fractions of its height:
 * the canvas gives a dataset one source on the right, a tool one target and
 * one source, an export a target alone, and an if node a target with its
 * true/false sources at 33% and 66% (`IfNode.tsx`). An annotation has none. */
export const previewNodeHandles = (type: string): { targets: number[]; sources: number[] } => {
  switch (type) {
    case "dataset":
      return { targets: [], sources: [0.5] };
    case "tool":
      return { targets: [0.5], sources: [0.5] };
    case "export":
      return { targets: [0.5], sources: [] };
    case "if":
      return { targets: [0.5], sources: [0.33, 0.66] };
    default:
      return { targets: [], sources: [] };
  }
};

/**
 * The name a layout descriptor's page size stands for, by reverse lookup of
 * its millimetres against `PAGE_SIZES` — either way round, since a landscape
 * page carries the size swapped. `null` for a page no standard size matches,
 * which the caller names "Custom".
 */
export const previewPageSizeName = (page: { width: number; height: number }): string | null => {
  const width = round1(page.width);
  const height = round1(page.height);
  for (const [name, size] of Object.entries(PREVIEW_PAGE_SIZE)) {
    if (
      (size.width === width && size.height === height) ||
      (size.width === height && size.height === width)
    ) {
      return name;
    }
  }
  return null;
};

/** What a layout template's page is, said in words and in millimetres:
 * "A4 · Landscape" for the tag beside its kind, and the two numbers for the
 * line under its thumbnail. Null for anything but a layout descriptor. */
export interface LayoutPageDescription {
  /** The standard size's name, or the translated "Custom". */
  name: string;
  label: string;
  width: number;
  height: number;
}

/** The page a layout carries, as the template stores it: the size's own
 * name and its orientation, verbatim from the config the author saved. */
export interface LayoutPageFields {
  page_size?: string | null;
  page_orientation?: "portrait" | "landscape" | null;
}

/**
 * The page a layout template prints on, from the two values it stores: the
 * size's name and orientation as words, and the millimetres that size stands
 * for. Null for a template that names no page — a workflow or project
 * payload, and a layout saved before the fields existed. A size this build
 * has no millimetres for is drawn as "Custom" on A4's page, which is what
 * the layout canvas itself falls back to.
 */
export const layoutPageDescription = (
  template: LayoutPageFields,
  translate: PreviewTranslate
): LayoutPageDescription | null => {
  if (!template.page_size) return null;
  const size = tableEntry(PREVIEW_PAGE_SIZE, template.page_size);
  const named = size ? template.page_size : translate("custom", { defaultValue: "Custom" });
  const { width: portraitWidth, height: portraitHeight } = size ?? PREVIEW_PAGE_SIZE.A4;
  const landscape = template.page_orientation === "landscape";
  const width = landscape ? portraitHeight : portraitWidth;
  const height = landscape ? portraitWidth : portraitHeight;
  const orientation = translate(landscape ? "landscape" : "portrait", {
    defaultValue: landscape ? "Landscape" : "Portrait",
  });
  return { name: named, label: `${named} · ${orientation}`, width: round1(width), height: round1(height) };
};

/**
 * The two page values a layout config declares, for a save or a refresh to
 * store on the template: read verbatim, since they are what a card labels it
 * with. Both null for a config that declares no page.
 */
export const layoutPageFromConfig = (
  config: Record<string, unknown> | null | undefined
): { page_size: string | null; page_orientation: "portrait" | "landscape" | null } => {
  const page = isRecord(config) && isRecord(config.page) ? config.page : null;
  const size = typeof page?.size === "string" && page.size ? page.size : null;
  const orientation =
    page?.orientation === "landscape" ? "landscape" : page?.orientation === "portrait" ? "portrait" : null;
  return { page_size: size, page_orientation: orientation };
};

/**
 * The workflow descriptor a live canvas config stands for — built on the
 * client, for the save dialog's live preview and for the PNG it generates.
 * Node order is config order, since the edges address
 * nodes by index; a node the canvas could not place (an unknown type, no
 * numeric position) is dropped along with every edge touching it; a label
 * falls back to the node type. Null where there is nothing to draw.
 */
export const descriptorFromWorkflowConfig = (
  config: Record<string, unknown> | null | undefined
): TemplateWorkflowPreview | null => {
  if (!isRecord(config) || !Array.isArray(config.nodes)) return null;

  const nodes: TemplateWorkflowPreview["nodes"] = [];
  const indexOfId = new Map<string, number>();
  for (const raw of config.nodes) {
    if (nodes.length >= PREVIEW_ITEM_CAP) break;
    if (!isRecord(raw)) continue;
    const type = typeof raw.type === "string" ? raw.type : "";
    const size = previewNodeSize(type, raw);
    if (!size) continue;
    if (!isRecord(raw.position)) continue;
    const x = numberOf(raw.position.x);
    const y = numberOf(raw.position.y);
    if (x === null || y === null) continue;
    const data = isRecord(raw.data) ? raw.data : null;
    const label = data && typeof data.label === "string" && data.label.trim() ? data.label : type;
    if (typeof raw.id === "string" && !indexOfId.has(raw.id)) indexOfId.set(raw.id, nodes.length);
    nodes.push({
      label,
      type,
      x,
      y,
      w: size.w,
      h: size.h,
      icon: previewNodeIcon(type, data),
      color: previewNodeColor(type, data),
      html: type === "textAnnotation" ? sanitizeNoteHtml(data?.text) : null,
    });
  }
  if (nodes.length === 0) return null;

  const edges: TemplateWorkflowPreview["edges"] = [];
  if (Array.isArray(config.edges)) {
    for (const raw of config.edges) {
      if (!isRecord(raw)) continue;
      const source = typeof raw.source === "string" ? indexOfId.get(raw.source) : undefined;
      const target = typeof raw.target === "string" ? indexOfId.get(raw.target) : undefined;
      if (source === undefined || target === undefined) continue;
      edges.push([source, target]);
    }
  }

  return { kind: "workflow", nodes, edges };
};

/** How far a legend's title and a text block's markup are read: a preview is
 * drawn a few hundred pixels wide, so nothing past this is legible either
 * way. The markup itself is capped by `sanitizeNoteHtml`. */
export const LAYOUT_TITLE_CAP = 200;

/** The size a text block's prose is set at where its markup declares none —
 * the browser's own default, which is what the layout canvas renders a
 * fragment carrying no size at. */
export const LAYOUT_TEXT_FONT_SIZE = 16;

/** What a text block's markup may ask to be set at, in the layout canvas's
 * own pixels: below this a thumbnail shows nothing, above it one run would
 * fill the page. */
const LAYOUT_FONT_RANGE = { min: 6, max: 96 };

/** The first size the markup declares, and the unit it is declared in. */
const LAYOUT_FONT_SIZE = /font-size\s*:\s*([\d.]+)\s*(pt|px|em|rem)/i;

/** A CSS length in the canvas's own pixels: `pt` at 96 DPI, `em`/`rem`
 * against the default body size. */
const LAYOUT_FONT_UNITS: Record<string, number> = {
  px: 1,
  pt: 96 / 72,
  em: LAYOUT_TEXT_FONT_SIZE,
  rem: LAYOUT_TEXT_FONT_SIZE,
};

/**
 * The camera a layout's map frame is frozen at, off the element's own config:
 * `viewState`'s centre and zoom. Null for a config that names none and for
 * one whose values are not a usable camera — the drawing then falls back to
 * the placeholder map glyph rather than requesting a frame of nowhere.
 */
export const layoutViewState = (config: unknown): TemplatePreviewViewState | null => {
  const view = isRecord(config) && isRecord(config.viewState) ? config.viewState : null;
  if (!view) return null;
  const latitude = numberOf(view.latitude);
  const longitude = numberOf(view.longitude);
  const zoom = numberOf(view.zoom);
  if (latitude === null || longitude === null || zoom === null) return null;
  if (Math.abs(latitude) > 90 || Math.abs(longitude) > 180 || zoom < 0 || zoom > 24) return null;
  return { latitude, longitude, zoom };
};

/**
 * A text block's own markup, off the element's config: `setup.text` is where
 * the layout canvas keeps it, with the two flatter shapes an older config
 * carries. Sanitised by the same allow-list a workflow note goes through, so
 * what comes back is what a drawing may write.
 */
export const layoutTextHtml = (config: unknown): string | null => {
  if (!isRecord(config)) return null;
  const setup = isRecord(config.setup) ? config.setup : null;
  return sanitizeNoteHtml(setup?.text ?? config.text ?? config.content);
};

/**
 * The size a text block's prose is set at, in the layout canvas's own pixels:
 * the first `font-size` its markup declares, which is the one its title run
 * carries. Null where the markup declares none — the drawing then sets the
 * canvas's own default. The declaration itself does not survive
 * `sanitizeNoteHtml`, whose allow-list carries no `font-size`, so it is read
 * off the raw markup and set on the fragment as a whole.
 */
export const layoutTextFontSize = (config: unknown): number | null => {
  if (!isRecord(config)) return null;
  const setup = isRecord(config.setup) ? config.setup : null;
  const raw = setup?.text ?? config.text ?? config.content;
  if (typeof raw !== "string") return null;
  const match = LAYOUT_FONT_SIZE.exec(raw);
  if (!match) return null;
  const size = Number(match[1]) * LAYOUT_FONT_UNITS[match[2].toLowerCase()];
  if (!Number.isFinite(size) || size < LAYOUT_FONT_RANGE.min) return null;
  return Math.round(Math.min(size, LAYOUT_FONT_RANGE.max) * 100) / 100;
};

/** The style values the layout itself defaults to where an element's own
 * `style` leaves them out — `borderStyleSchema` and
 * `backgroundStyleSchema` in `lib/validations/reportLayout.ts`. Both are
 * disabled by default: a layout element draws no frame and no fill unless it
 * was asked to. */
const ELEMENT_STYLE_DEFAULT = {
  borderColor: "#000000",
  borderWidth: 0.5,
  backgroundColor: "#ffffff",
  opacity: 1,
} as const;

/** What a border's width may be, in page millimetres — the range
 * `borderStyleSchema` allows. */
const BORDER_WIDTH_RANGE = { min: 0.1, max: 5 };

/** The inset an element may keep its content in, in page millimetres: a
 * padding past this would leave a thumbnail's block empty. */
const PADDING_MAX = 25;

/** A colour a drawing will paint with: the hex the style editor writes.
 * Anything else — a named colour, a function — is not written into the SVG,
 * and the element takes the layout's own default for that half of its
 * style. */
const styleColor = (value: unknown, fallback: string): string =>
  typeof value === "string" && HEX_COLOR.test(value.trim()) ? value.trim() : fallback;

const clampValue = (value: number, min: number, max: number): number => Math.min(Math.max(value, min), max);

/**
 * What an element is framed and filled with, off its own `style`: the halves
 * it enabled, with the layout's own defaults for whatever they leave out.
 * Null for an element that asked for none of it — which is what a layout
 * element does by default, and the reason a thumbnail draws most blocks as
 * their content alone.
 */
export const layoutElementStyle = (style: unknown): TemplatePreviewElementStyle | null => {
  if (!isRecord(style)) return null;
  const borderStyle = isRecord(style.border) ? style.border : null;
  const backgroundStyle = isRecord(style.background) ? style.background : null;

  const border =
    borderStyle?.enabled === true
      ? {
          color: styleColor(borderStyle.color, ELEMENT_STYLE_DEFAULT.borderColor),
          width: clampValue(
            numberOf(borderStyle.width) ?? ELEMENT_STYLE_DEFAULT.borderWidth,
            BORDER_WIDTH_RANGE.min,
            BORDER_WIDTH_RANGE.max
          ),
        }
      : null;
  const background =
    backgroundStyle?.enabled === true
      ? {
          color: styleColor(backgroundStyle.color, ELEMENT_STYLE_DEFAULT.backgroundColor),
          opacity: clampValue(numberOf(backgroundStyle.opacity) ?? ELEMENT_STYLE_DEFAULT.opacity, 0, 1),
        }
      : null;

  const paddingValue = numberOf(style.padding) ?? 0;
  const padding = paddingValue > 0 ? clampValue(paddingValue, 0, PADDING_MAX) : null;
  const opacityValue = numberOf(style.opacity);
  const opacity = opacityValue !== null && opacityValue < 1 ? clampValue(opacityValue, 0, 1) : null;

  if (!border && !background && padding === null && opacity === null) return null;
  return {
    ...(border ? { border } : {}),
    ...(background ? { background } : {}),
    ...(padding === null ? {} : { padding }),
    ...(opacity === null ? {} : { opacity }),
  };
};

/**
 * A legend's own title, off the element's config: the plain text
 * `config.title.text` carries, trimmed and capped. Null for a legend that
 * shows none.
 */
export const layoutLegendTitle = (config: unknown): string | null => {
  const title = isRecord(config) && isRecord(config.title) ? config.title : null;
  if (!title || typeof title.text !== "string") return null;
  // A control character has no glyph and would travel into the SVG.
  const text = title.text.replace(/[\u0000-\u001f\u007f]+/g, " ").trim();
  return text ? text.slice(0, LAYOUT_TITLE_CAP) : null;
};

/**
 * The layout descriptor a live layout config stands for, the counterpart of
 * `descriptorFromWorkflowConfig`: the page in millimetres from its size name
 * and orientation, and every element that carries a numeric position box.
 * Null where there is nothing to draw.
 *
 * An element also carries the little of its own content a drawing can show —
 * a map frame's camera, a text block's markup, a legend's title — and only
 * where it has it, so a config carrying none of it builds exactly the
 * descriptor `core.templates.snapshot.build_preview` builds.
 */
export const descriptorFromLayoutConfig = (
  config: Record<string, unknown> | null | undefined
): TemplateLayoutPreview | null => {
  if (!isRecord(config)) return null;
  const page = isRecord(config.page) ? config.page : {};
  const orientation = page.orientation === "landscape" ? "landscape" : "portrait";
  const size =
    (typeof page.size === "string" ? tableEntry(PREVIEW_PAGE_SIZE, page.size) : undefined) ??
    PREVIEW_PAGE_SIZE.A4;
  const pageBox = orientation === "landscape" ? { width: size.height, height: size.width } : { ...size };

  const rawElements = config.elements;
  if (rawElements !== undefined && rawElements !== null && !Array.isArray(rawElements)) return null;
  const elements: TemplateLayoutPreview["elements"] = [];
  for (const raw of Array.isArray(rawElements) ? rawElements : []) {
    if (elements.length >= PREVIEW_ITEM_CAP) break;
    if (!isRecord(raw) || typeof raw.type !== "string" || !isRecord(raw.position)) continue;
    const x = numberOf(raw.position.x);
    const y = numberOf(raw.position.y);
    const width = numberOf(raw.position.width);
    const height = numberOf(raw.position.height);
    if (x === null || y === null || width === null || height === null) continue;
    const config = isRecord(raw.config) ? raw.config : null;
    const html = raw.type === "text" ? layoutTextHtml(config) : null;
    const viewState = raw.type === "map" ? layoutViewState(config) : null;
    const fontSize = html ? layoutTextFontSize(config) : null;
    const title = raw.type === "legend" ? layoutLegendTitle(config) : null;
    const style = layoutElementStyle(raw.style);
    elements.push({
      type: raw.type,
      x,
      y,
      width,
      height,
      ...(viewState ? { viewState } : {}),
      ...(html ? { html } : {}),
      ...(fontSize ? { fontSize } : {}),
      ...(title ? { title } : {}),
      ...(style ? { style } : {}),
    });
  }

  return { kind: "layout", orientation, page: pageBox, elements };
};

/**
 * Lays a workflow descriptor's nodes out inside a `width`×`height` frame:
 * one uniform scale for both axes (so the canvas layout is not distorted),
 * never magnified past `maxScale`, and centred on what is left.
 * Edges come back as bezier paths between the boxes they connect; an edge
 * naming a node the descriptor does not carry is dropped.
 */
export const fitWorkflow = (
  descriptor: TemplateWorkflowPreview | null | undefined,
  width: number,
  height: number,
  padding: number = SCAFFOLD_PADDING,
  maxScale: number = SCAFFOLD_MAX_SCALE
): WorkflowGeometry => {
  const source = (descriptor?.nodes ?? []).filter((node) => node.w > 0 && node.h > 0);
  if (source.length === 0) return { nodes: [], edges: [], scale: 1 };

  const minX = Math.min(...source.map((node) => node.x));
  const minY = Math.min(...source.map((node) => node.y));
  const maxX = Math.max(...source.map((node) => node.x + node.w));
  const maxY = Math.max(...source.map((node) => node.y + node.h));
  const contentWidth = Math.max(maxX - minX, 1);
  const contentHeight = Math.max(maxY - minY, 1);

  const availableWidth = Math.max(width - 2 * padding, 1);
  const availableHeight = Math.max(height - 2 * padding, 1);
  const scale = Math.min(availableWidth / contentWidth, availableHeight / contentHeight, maxScale);

  const offsetX = padding + (availableWidth - contentWidth * scale) / 2 - minX * scale;
  const offsetY = padding + (availableHeight - contentHeight * scale) / 2 - minY * scale;

  const nodes: PreviewNodeBox[] = source.map((node) => ({
    x: round(node.x * scale + offsetX),
    y: round(node.y * scale + offsetY),
    w: round(node.w * scale),
    h: round(node.h * scale),
    label: node.label,
    type: node.type,
    icon: node.icon ?? null,
    color: node.color ?? null,
    html: node.html ?? null,
  }));

  const edges: PreviewEdgePath[] = [];
  for (const [from, to] of descriptor?.edges ?? []) {
    const start = nodes[from];
    const end = nodes[to];
    if (!start || !end) continue;
    const x1 = round(start.x + start.w);
    const y1 = round(start.y + start.h / 2);
    const x2 = round(end.x);
    const y2 = round(end.y + end.h / 2);
    const curve = round(Math.max(MIN_EDGE_CURVE, Math.abs(x2 - x1) / 2));
    edges.push({ d: `M ${x1} ${y1} C ${x1 + curve} ${y1}, ${x2 - curve} ${y2}, ${x2} ${y2}` });
  }

  return { nodes, edges, scale };
};

/**
 * Lays a layout descriptor's page out inside a `width`×`height` frame — the
 * page fills the frame in its own aspect ratio — and places its elements on
 * it in the page's own units.
 */
export const fitLayout = (
  descriptor: TemplateLayoutPreview | null | undefined,
  width: number,
  height: number,
  padding: number = SCAFFOLD_PADDING
): LayoutGeometry => {
  const pageWidth =
    descriptor?.page.width && descriptor.page.width > 0 ? descriptor.page.width : PREVIEW_PAGE_SIZE.A4.width;
  const pageHeight =
    descriptor?.page.height && descriptor.page.height > 0
      ? descriptor.page.height
      : PREVIEW_PAGE_SIZE.A4.height;

  const availableWidth = Math.max(width - 2 * padding, 1);
  const availableHeight = Math.max(height - 2 * padding, 1);
  const scale = Math.min(availableWidth / pageWidth, availableHeight / pageHeight);

  const page: PreviewBox = {
    x: round(padding + (availableWidth - pageWidth * scale) / 2),
    y: round(padding + (availableHeight - pageHeight * scale) / 2),
    w: round(pageWidth * scale),
    h: round(pageHeight * scale),
  };

  const elements: PreviewElementBox[] = (descriptor?.elements ?? []).map((element) => ({
    x: round(page.x + element.x * scale),
    y: round(page.y + element.y * scale),
    w: round(Math.max(element.width, 0) * scale),
    h: round(Math.max(element.height, 0) * scale),
    type: element.type,
    viewState: element.viewState ?? null,
    html: element.html ?? null,
    fontSize: element.fontSize ?? null,
    title: element.title ?? null,
    style: element.style ?? null,
  }));

  return { page, elements, scale };
};
