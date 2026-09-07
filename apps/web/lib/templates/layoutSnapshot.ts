import type { Theme } from "@mui/material";
import { alpha } from "@mui/material";

import type { LayoutDrawing, LayoutDrawingElement } from "@/lib/templates/layoutDrawing";
import { layoutDrawing } from "@/lib/templates/layoutDrawing";
import type { GlyphRole, GlyphShape } from "@/lib/templates/layoutGlyphs";
import { SNAPSHOT_SIZE } from "@/lib/templates/previewGeometry";
import { rasterizeSvg } from "@/lib/templates/rasterizeSvg";
import { loadStaticMapImage } from "@/lib/templates/staticMap";
import { NOTE_CLASS, NOTE_CSS, NOTE_FONT_STACK } from "@/lib/templates/workflowSnapshot";
import type { TemplateLayoutPreview } from "@/lib/validations/template";

export interface LayoutPalette {
  /** The ground the page sits on. */
  ground: string;
  /** The paper itself. */
  page: string;
  pageBorder: string;
  /** The ink an element's own words are written in — a text block's prose, a
   * legend's title. */
  text: string;
  /** A furniture block's outline — a scalebar, a north arrow, a divider. */
  elementStroke: string;
  /** A content block's outline — a map, a chart, a picture, a table. */
  contentStroke: string;
  /** What each glyph role is drawn in. */
  glyph: Record<GlyphRole, string>;
}

/**
 * The snapshot's palette, fixed rather than taken from the theme: the PNG is
 * stored and shown to every reader, on a light card, whatever theme the
 * author happened to be in when they saved it. Every value is the light
 * theme's own, flattened over what it is drawn on — the page over the
 * ground, a glyph over the paper — since a rasterised drawing carries no
 * compositing of its own to rely on.
 */
export const LAYOUT_SNAPSHOT_PALETTE: LayoutPalette = {
  // `background.default`, the same ground the workflow snapshot paints.
  ground: "#F4F5FA",
  // `background.paper`.
  page: "#FFFFFF",
  // The light theme's divider, `rgba(58, 53, 65, 0.12)` over the ground.
  pageBorder: "#DEDEE4",
  // `text.primary`, `rgba(58, 53, 65, 0.87)` over paper — the ink the layout
  // canvas writes an element's own text in.
  text: "#544F5A",
  // `text.primary` at 0.2 over paper.
  elementStroke: "#D8D7D9",
  // `primary.main` at 0.35 over paper.
  contentStroke: "#B5E4D3",
  glyph: {
    // `primary.main` at 0.28 and at 0.08, over paper.
    tint: "#C4EADC",
    wash: "#EEF9F5",
    // `text.primary` at 0.26 over paper.
    line: "#CCCACE",
    // `warning.main` at 0.5 over paper.
    accent: "#FFDA80",
  },
};

/**
 * The same drawing in the reader's own theme, for the scaffold — which is
 * rendered into the page rather than stored, so it follows the theme the way
 * the layout canvas does. The glyph roles stay low contrast, so a page of
 * placeholders reads as structure rather than as content.
 */
export const layoutPaletteForTheme = (theme: Theme): LayoutPalette => ({
  ground: theme.palette.background.default,
  page: theme.palette.background.paper,
  pageBorder: theme.palette.divider,
  text: theme.palette.text.primary,
  elementStroke: alpha(theme.palette.text.primary, 0.2),
  contentStroke: alpha(theme.palette.primary.main, 0.35),
  glyph: {
    tint: alpha(theme.palette.primary.main, 0.28),
    wash: alpha(theme.palette.primary.main, 0.08),
    line: alpha(theme.palette.text.primary, 0.26),
    accent: alpha(theme.palette.warning.main, 0.5),
  },
});

/** What an element's outline is drawn in: a content block takes the content
 * tone, the page's furniture the muted one. */
export const elementStrokeColor = (filled: boolean, palette: LayoutPalette): string =>
  filled ? palette.contentStroke : palette.elementStroke;

/** The id one drawing defines and refers to. It is suffixed per drawing,
 * since `url(#id)` resolves to the first match in the document and the
 * shadow's blur is scale-dependent — two drawings on one page must not
 * share it. */
let drawingSerial = 0;

const shadowId = (): string => {
  drawingSerial += 1;
  return `goat-layout-shadow-${drawingSerial}`;
};

/** A legend's title is written into an XML document, so its own characters
 * are escaped. The prose beside it comes from `sanitizeNoteHtml`, which is
 * already well-formed XML. */
const escapeXml = (value: string): string =>
  value
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&apos;");

/** One glyph shape as SVG. A rect, a circle and a path are filled; a line is
 * stroked. */
const glyphToSvg = (shape: GlyphShape, color: string): string => {
  switch (shape.shape) {
    case "rect":
      return `<rect x="${shape.x}" y="${shape.y}" width="${shape.w}" height="${shape.h}"${
        shape.rx === undefined ? "" : ` rx="${shape.rx}"`
      } fill="${color}" />`;
    case "circle":
      return `<circle cx="${shape.cx}" cy="${shape.cy}" r="${shape.r}" fill="${color}" />`;
    case "line":
      return `<line x1="${shape.x1}" y1="${shape.y1}" x2="${shape.x2}" y2="${shape.y2}" stroke="${color}" stroke-width="${shape.width}" stroke-linecap="round" />`;
    default:
      return shape.stroke
        ? `<path d="${shape.d}" fill="none" stroke="${color}" stroke-width="${shape.stroke}" />`
        : `<path d="${shape.d}" fill="${color}" />`;
  }
};

export interface LayoutDrawingToSvgOptions {
  palette?: LayoutPalette;
  /** The basemap frame fetched for each map element, by its index in the
   * drawing — see `loadLayoutMapImages`. An element with no frame keeps the
   * placeholder map glyph the drawing carries. */
  mapImages?: Map<number, string>;
}

/**
 * A drawing as the body of an `<svg>`: the ground, the paper with its
 * hairline border and its shadow, and one group per element carrying that
 * element's outline, its glyph and whatever of its own content it brought —
 * a basemap frame, a text block's words, a legend's title. The geometry is
 * the shared `layoutDrawing`, so this is the same picture
 * `TemplatePreviewFallback` renders as React elements — written out as a
 * string for the stored PNG.
 */
export const layoutDrawingToSvg = (
  drawing: LayoutDrawing,
  options: LayoutDrawingToSvgOptions = {}
): string => {
  const palette = options.palette ?? LAYOUT_SNAPSHOT_PALETTE;
  const shadow = shadowId();
  const { page } = drawing;

  const elements = drawing.elements
    .map((element, index) => {
      // What the element itself asks to be filled and framed with — and
      // nothing where it asks for neither, which is a layout element's own
      // default. The frame is drawn square, the way the layout canvas draws
      // it, and the faint placeholder outline keeps the rounding a
      // wireframe reads better with.
      const background = element.background
        ? `<rect x="${element.x}" y="${element.y}" width="${element.w}" height="${element.h}" fill="${element.background.color}" fill-opacity="${element.background.opacity}" />`
        : "";
      const border = element.frame
        ? `<rect x="${element.x}" y="${element.y}" width="${element.w}" height="${element.h}" fill="none" stroke="${element.frame.color}" stroke-width="${element.frame.width}" />`
        : "";
      const outline = element.placeholder
        ? `<rect x="${element.x}" y="${element.y}" width="${element.w}" height="${
            element.h
          }" rx="${element.radius}" fill="none" stroke="${elementStrokeColor(
            element.filled,
            palette
          )}" stroke-width="${element.border}" />`
        : "";
      const glyphs = element.glyphs.map((shape) => glyphToSvg(shape, palette.glyph[shape.role])).join("");

      // The basemap frame, inlined: it covers the placeholder map under it,
      // which is what shows through where no frame could be fetched.
      const image = options.mapImages?.get(index);
      const basemap = image
        ? `<image x="${element.content.x}" y="${element.content.y}" width="${element.content.w}" height="${element.content.h}" preserveAspectRatio="xMidYMid slice" href="${image}" />`
        : "";

      // A text block's own rich text, laid out by the rasterising browser
      // inside a `foreignObject` — the markup is already sanitised and
      // well-formed XML, and the styles are inlined, so the SVG stays
      // self-contained.
      const prose =
        element.html && element.prose
          ? [
              `<foreignObject x="${element.prose.x}" y="${element.prose.y}" width="${element.prose.w}" height="${element.prose.h}">`,
              `<div xmlns="http://www.w3.org/1999/xhtml" class="${NOTE_CLASS}" style="font-family: ${NOTE_FONT_STACK}; font-size: ${element.prose.fontSize}px; color: ${palette.text}; width: 100%; height: 100%;">`,
              // The rules travel with the text, inside the same xhtml
              // fragment: every selector is scoped to the class, so a page
              // holding several of these cannot be reached by them.
              `<style>${NOTE_CSS}</style>`,
              element.html,
              `</div></foreignObject>`,
            ].join("")
          : "";

      const title = element.title
        ? `<text x="${element.title.x}" y="${element.title.y}" font-family="${NOTE_FONT_STACK}" font-size="${
            element.title.fontSize
          }" font-weight="600" fill="${palette.text}">${escapeXml(element.title.text)}</text>`
        : "";

      // The element's own border goes over its content rather than under it:
      // a basemap covers its whole box, and the border reads on top of it.
      const opacity = element.opacity < 1 ? ` opacity="${element.opacity}"` : "";
      return `<g data-type="${element.type}"${opacity}>${background}${glyphs}${basemap}${outline}${border}${prose}${title}</g>`;
    })
    .join("");

  return [
    // The paper's own drop shadow, as a filter — SVG has no box-shadow.
    `<defs><filter id="${shadow}" x="-20%" y="-20%" width="140%" height="140%">`,
    `<feDropShadow dx="0" dy="${drawing.shadow.dy}" stdDeviation="${drawing.shadow.blur}" flood-color="#000000" flood-opacity="0.1" /></filter></defs>`,
    `<rect width="${drawing.width}" height="${drawing.height}" fill="${palette.ground}" />`,
    `<rect x="${page.x}" y="${page.y}" width="${page.w}" height="${page.h}" rx="${page.radius}" fill="${palette.page}" stroke="${palette.pageBorder}" stroke-width="${page.border}" filter="url(#${shadow})" />`,
    elements,
  ].join("");
};

/**
 * The basemap frame for every map element of a drawing that names a camera,
 * inlined as a `data:` URI and keyed by the element's index — one request
 * each, in parallel, each abandoned after `STATIC_MAP_TIMEOUT`. An element
 * whose frame could not be fetched is simply not in the map, and keeps the
 * placeholder map glyph.
 */
export const loadLayoutMapImages = async (elements: LayoutDrawingElement[]): Promise<Map<number, string>> => {
  const wanted = elements
    .map((element, index) => ({ element, index }))
    .filter(({ element }) => element.viewState !== null && element.content.w > 0 && element.content.h > 0);
  const images = new Map<number, string>();
  if (wanted.length === 0) return images;
  const frames = await Promise.all(
    wanted.map(({ element }) =>
      loadStaticMapImage(element.viewState as NonNullable<LayoutDrawingElement["viewState"]>, {
        width: element.content.w,
        height: element.content.h,
      })
    )
  );
  wanted.forEach(({ index }, slot) => {
    const frame = frames[slot];
    if (frame) images.set(index, frame);
  });
  return images;
};

/**
 * The whole layout snapshot as one self-contained SVG string: no external
 * stylesheet, no webfont and no image reference — the basemap frames are
 * fetched here and inlined as `data:` URIs, since the SVG is rasterised
 * through an `<img>` that resolves nothing external. So it renders the same
 * whatever the page around it looks like, and whether the network answered.
 *
 * Async for those frames alone: a layout with no map camera in it, or one
 * whose frames could not be fetched, is the wireframe it always was.
 */
export const svgForLayout = async (
  descriptor: TemplateLayoutPreview | null | undefined,
  size: { width: number; height: number } = SNAPSHOT_SIZE
): Promise<string> => {
  const { width, height } = size;
  const drawing = layoutDrawing(descriptor, { size });
  const mapImages = await loadLayoutMapImages(drawing.elements);
  return [
    `<svg xmlns="http://www.w3.org/2000/svg" width="${width}" height="${height}" viewBox="0 0 ${width} ${height}">`,
    layoutDrawingToSvg(drawing, { mapImages }),
    `</svg>`,
  ].join("");
};

/**
 * The layout snapshot as a PNG blob: the SVG above put through the shared
 * rasteriser, which draws it into a canvas through an `<img>`. Rejects on
 * every failure, and the caller falls back to the drawn scaffold.
 */
export const renderLayoutSnapshot = async (
  descriptor: TemplateLayoutPreview | null | undefined,
  size: { width: number; height: number } = SNAPSHOT_SIZE
): Promise<Blob> => rasterizeSvg(await svgForLayout(descriptor, size), size, LAYOUT_SNAPSHOT_PALETTE.ground);
