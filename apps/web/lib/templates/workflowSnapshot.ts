import type { Theme } from "@mui/material";

import type { NodeIconMarkup } from "@/lib/templates/nodeIconMarkup";
import { loadNodeIconMarkup } from "@/lib/templates/nodeIconMarkup";
import { NODE_ICON_TONES } from "@/lib/templates/nodeIcons";
import type { PreviewTranslate } from "@/lib/templates/previewGeometry";
import { TEXT_ANNOTATION_COLOR } from "@/lib/templates/previewGeometry";
import { rasterizeSvg } from "@/lib/templates/rasterizeSvg";
import type { WorkflowDrawing } from "@/lib/templates/workflowDrawing";
import { ANNOTATION_FILL_OPACITY, SNAPSHOT_SIZE, workflowDrawing } from "@/lib/templates/workflowDrawing";
import type { TemplateWorkflowPreview } from "@/lib/validations/template";

export { SNAPSHOT_SIZE };

export interface WorkflowPalette {
  /** The pane the canvas sits on. */
  ground: string;
  /** The canvas's dot grid. */
  grid: string;
  edge: string;
  nodeFill: string;
  nodeStroke: string;
  text: string;
  /** What a dataset's geometry glyph is drawn in: `DatasetNode` gives it
   * `text.secondary` while the node is idle. */
  icon: string;
  /** What the gear and the branch are drawn in: `ToolNode`'s fallback
   * `SettingsIcon` takes `color: "inherit"` and `IfNode`'s `CallSplitIcon`
   * is uncoloured, so both take the node's own text colour,
   * `text.primary`. */
  iconStrong: string;
  /** The five tones a two-tone tool icon is drawn in. */
  iconTones: readonly string[];
  handle: string;
  /** The ring a handle carries, the paper the node is drawn on. */
  handleRing: string;
  /** A text annotation's own colour, which it keeps in either theme. It is
   * the colour a note with none of its own is painted in; a note that
   * carries one is painted in that. */
  annotation: string;
  /** The ink a note's own prose is written in — `text.primary`, the colour
   * `TipTapEditorContent` gives it on the canvas. */
  noteText: string;
}

/**
 * The snapshot's palette, fixed rather than taken from the theme: the PNG is
 * stored and shown to every reader, on a light card, whatever theme the
 * author happened to be in when they saved it. Every value is what the
 * workflow canvas itself paints in the light theme.
 */
export const SNAPSHOT_PALETTE: WorkflowPalette = {
  // `background.default`, the pane `WorkflowCanvas` puts ReactFlow on.
  ground: "#F4F5FA",
  // ReactFlow's own dot colour, which `<Background variant="dots">` uses.
  grid: "#91919A",
  // `grey[500]`, the canvas's `defaultEdgeOptions` stroke.
  edge: "#9E9E9E",
  // `background.paper`.
  nodeFill: "#FFFFFF",
  // The light theme's divider, `rgba(58, 53, 65, 0.12)` over paper.
  nodeStroke: "#E7E7E8",
  // `text.secondary`, `rgba(58, 53, 65, 0.68)` over paper — the colour the
  // theme gives the `caption` variant a node's title is written in.
  text: "#79767E",
  // A dataset's geometry glyph: `text.secondary`, the same value the title
  // is written in.
  icon: "#79767E",
  // `text.primary`, `rgba(58, 53, 65, 0.87)` over paper.
  iconStrong: "#544F5A",
  iconTones: NODE_ICON_TONES.light,
  handle: "#9E9E9E",
  handleRing: "#FFFFFF",
  annotation: TEXT_ANNOTATION_COLOR,
  // `text.primary` over paper, the same value the glyphs take.
  noteText: "#544F5A",
};

/**
 * The same drawing in the reader's own theme, for the scaffold — which is
 * rendered into the page rather than stored, so it follows the theme the way
 * the canvas does.
 */
export const workflowPaletteForTheme = (theme: Theme): WorkflowPalette => {
  const tones = theme.palette.mode === "dark" ? NODE_ICON_TONES.dark : NODE_ICON_TONES.light;
  return {
    ground: theme.palette.background.default,
    grid: SNAPSHOT_PALETTE.grid,
    edge: theme.palette.grey[500],
    nodeFill: theme.palette.background.paper,
    nodeStroke: theme.palette.divider,
    text: theme.palette.text.secondary,
    icon: theme.palette.text.secondary,
    iconStrong: theme.palette.text.primary,
    iconTones: tones,
    handle: theme.palette.grey[500],
    handleRing: theme.palette.background.paper,
    annotation: SNAPSHOT_PALETTE.annotation,
    noteText: theme.palette.text.primary,
  };
};

/**
 * What a node type's single-tone glyph is drawn in. A two-tone tool icon
 * takes neither: it reads its own colours off `iconTones`, which is what
 * `NodeIconWrapper` sets on an idle node.
 */
export const nodeIconColor = (type: string, palette: WorkflowPalette): string =>
  type === "dataset" ? palette.icon : palette.iconStrong;

/** The ids one drawing defines and refers to. They are suffixed per
 * drawing, since `url(#id)` resolves to the first match in the document and
 * both the grid gap and the shadow blur are scale-dependent — two drawings
 * on one page must not share them. */
let drawingSerial = 0;

const drawingIds = (): { grid: string; shadow: string } => {
  drawingSerial += 1;
  return {
    grid: `goat-snapshot-grid-${drawingSerial}`,
    shadow: `goat-snapshot-shadow-${drawingSerial}`,
  };
};

/** No webfont is loaded into the SVG, so the label is drawn in whatever the
 * rasterising browser has. The scaffold sets the same stack on a note, so
 * the two drawings break its lines in the same places. */
export const NOTE_FONT_STACK =
  "-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif";

const FONT_STACK = NOTE_FONT_STACK;

/** The class a note's markup is rendered under, and the rules that give it
 * the shape `TipTapEditorContent` gives it on the canvas: no margins on the
 * blocks, a larger semibold heading, ProseMirror's word breaking. Every size
 * is relative to the note's own font size, which the drawing scales, so the
 * text keeps the proportion of the card it has on the canvas. The colour and
 * that font size are set on the element itself, which is what makes these
 * rules identical in both drawings — the scaffold in the reader's theme and
 * the stored PNG in the light one. */
export const NOTE_CLASS = "goat-template-note";

export const NOTE_CSS = [
  `.${NOTE_CLASS} { line-height: 1.5; overflow: hidden; word-break: break-word; overflow-wrap: break-word; }`,
  `.${NOTE_CLASS} p, .${NOTE_CLASS} h1, .${NOTE_CLASS} h2, .${NOTE_CLASS} h3, .${NOTE_CLASS} h4, .${NOTE_CLASS} h5, .${NOTE_CLASS} h6 { margin: 0; }`,
  `.${NOTE_CLASS} h1 { font-size: 2em; font-weight: 600; }`,
  `.${NOTE_CLASS} h2 { font-size: 1.5em; font-weight: 600; }`,
  `.${NOTE_CLASS} h3 { font-size: 1.17em; font-weight: 600; }`,
  `.${NOTE_CLASS} ul, .${NOTE_CLASS} ol { margin: 0; padding-left: 1.2em; }`,
  `.${NOTE_CLASS} a { color: inherit; text-decoration: underline; }`,
].join(" ");

const escapeXml = (value: string): string =>
  value
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&apos;");

export interface DrawingToSvgOptions {
  palette?: WorkflowPalette;
  /** Writes each node's icon. Without it the cards carry their icon wrapper
   * and no glyph. */
  iconMarkup?: NodeIconMarkup;
}

/**
 * A drawing as the body of an `<svg>`: the ground, the canvas's dot grid,
 * the edges, and one card per node. The geometry is the shared
 * `workflowDrawing`, so this is the same picture `TemplatePreviewFallback`
 * renders as React elements — written out as a string for the stored PNG.
 */
export const drawingToSvg = (drawing: WorkflowDrawing, options: DrawingToSvgOptions = {}): string => {
  const palette = options.palette ?? SNAPSHOT_PALETTE;
  const ids = drawingIds();

  const edgePaths = drawing.edges
    .map(
      (edge) =>
        `<path d="${edge.d}" fill="none" stroke="${palette.edge}" stroke-width="${edge.width}" stroke-linecap="round" />`
    )
    .join("");

  const nodeShapes = drawing.nodes
    .map((node) => {
      // A note is painted in its own colour — the canvas's 5% fill and 2px
      // border of it — and falls back to the colour the canvas defaults to.
      const noteColor = node.color ?? palette.annotation;
      const body = `<rect x="${node.x}" y="${node.y}" width="${node.w}" height="${node.h}" rx="${
        node.radius
      }" fill="${node.annotation ? noteColor : palette.nodeFill}"${
        node.annotation ? ` fill-opacity="${ANNOTATION_FILL_OPACITY}"` : ""
      } stroke="${node.annotation ? noteColor : palette.nodeStroke}" stroke-width="${
        node.border
      }"${node.annotation ? "" : ` filter="url(#${ids.shadow})"`} />`;

      const iconWrapper = node.icons
        ? [
            `<rect x="${node.icons.x}" y="${node.icons.y}" width="${node.icons.size}" height="${node.icons.size}" rx="${node.icons.radius}" fill="none" stroke="${palette.nodeStroke}" stroke-width="${node.icons.border}" />`,
            options.iconMarkup?.(node.type, node.icon, {
              x: node.icons.glyphX,
              y: node.icons.glyphY,
              size: node.icons.glyphSize,
              color: nodeIconColor(node.type, palette),
              tones: palette.iconTones,
            }) ?? "",
          ].join("")
        : "";

      const handles = node.handles
        .map(
          (handle) =>
            `<circle cx="${handle.x}" cy="${handle.y}" r="${handle.r}" fill="${palette.handle}" stroke="${palette.handleRing}" stroke-width="${handle.ring}" />`
        )
        .join("");

      const text = node.label
        ? `<text x="${node.labelX}" y="${node.labelY}" font-family="${FONT_STACK}" font-size="${
            node.fontSize
          }" font-weight="700" letter-spacing="${node.letterSpacing}" fill="${
            palette.text
          }">${escapeXml(node.label)}</text>`
        : "";

      // A note's own rich text, laid out by the rasterising browser inside a
      // `foreignObject` — the markup is already sanitised and well-formed
      // XML, and the styles are inlined, so the SVG stays self-contained.
      const prose =
        node.html && node.prose
          ? [
              `<foreignObject x="${node.prose.x}" y="${node.prose.y}" width="${node.prose.w}" height="${node.prose.h}">`,
              `<div xmlns="http://www.w3.org/1999/xhtml" class="${NOTE_CLASS}" style="font-family: ${FONT_STACK}; font-size: ${node.prose.fontSize}px; color: ${palette.noteText}; width: 100%; height: 100%;">`,
              // The rules travel with the note, inside the same xhtml
              // fragment: every selector is scoped to the note's class, so a
              // page holding several of these cannot be reached by them.
              `<style>${NOTE_CSS}</style>`,
              node.html,
              `</div></foreignObject>`,
            ].join("")
          : "";

      return `<g data-type="${escapeXml(node.type)}">${body}${iconWrapper}${text}${prose}${handles}</g>`;
    })
    .join("");

  return [
    `<defs><pattern id="${ids.grid}" width="${drawing.grid.gap}" height="${drawing.grid.gap}" patternUnits="userSpaceOnUse">`,
    `<circle cx="${drawing.grid.dot}" cy="${drawing.grid.dot}" r="${drawing.grid.dot}" fill="${palette.grid}" /></pattern>`,
    // The card shadow `NodeContainer` carries — `0 2px 8px rgba(0,0,0,0.08)`
    // as a filter, since SVG has no box-shadow.
    `<filter id="${ids.shadow}" x="-20%" y="-20%" width="140%" height="140%">`,
    `<feDropShadow dx="0" dy="${drawing.shadow.dy}" stdDeviation="${drawing.shadow.blur}" flood-color="#000000" flood-opacity="0.08" /></filter></defs>`,
    `<rect width="${drawing.width}" height="${drawing.height}" fill="${palette.ground}" />`,
    `<rect width="${drawing.width}" height="${drawing.height}" fill="url(#${ids.grid})" />`,
    edgePaths,
    nodeShapes,
  ].join("");
};

/**
 * The whole snapshot as one self-contained SVG string: no external
 * stylesheet, no image reference, no webfont — so it rasterises through an
 * `<img>` without a second network round trip, and renders the same
 * whatever the page around it looks like. Async because the icons are
 * written by React's server renderer, which is loaded on demand.
 */
export const svgForWorkflow = async (
  descriptor: TemplateWorkflowPreview | null | undefined,
  size: { width: number; height: number } = SNAPSHOT_SIZE,
  translate?: PreviewTranslate
): Promise<string> => {
  const { width, height } = size;
  const drawing = workflowDrawing(descriptor, { size, translate });
  const iconMarkup = await loadNodeIconMarkup();
  return [
    `<svg xmlns="http://www.w3.org/2000/svg" width="${width}" height="${height}" viewBox="0 0 ${width} ${height}">`,
    drawingToSvg(drawing, { iconMarkup }),
    `</svg>`,
  ].join("");
};

/**
 * The snapshot as a PNG blob: the SVG above put through the shared
 * rasteriser, which draws it into a canvas through an `<img>`. Rejects on
 * every failure, and the caller falls back to the drawn scaffold.
 */
export const renderWorkflowSnapshot = async (
  descriptor: TemplateWorkflowPreview | null | undefined,
  size: { width: number; height: number } = SNAPSHOT_SIZE,
  translate?: PreviewTranslate
): Promise<Blob> => {
  const svg = await svgForWorkflow(descriptor, size, translate);
  return rasterizeSvg(svg, size, SNAPSHOT_PALETTE.ground);
};
