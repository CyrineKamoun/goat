import { mmToPx } from "@/lib/print/units";
import type { GlyphShape } from "@/lib/templates/layoutGlyphs";
import { FILLED_ELEMENTS, glyphInset, layoutGlyph } from "@/lib/templates/layoutGlyphs";
import type { PreviewBox } from "@/lib/templates/previewGeometry";
import {
  LAYOUT_TEXT_FONT_SIZE,
  SCAFFOLD_FRAME,
  SCAFFOLD_PADDING,
  SNAPSHOT_SIZE,
  fitLayout,
  truncateLabel,
} from "@/lib/templates/previewGeometry";
import type { TemplateLayoutPreview, TemplatePreviewViewState } from "@/lib/validations/template";

/**
 * The page's furniture at the scaffold frame's own size: the paper's 2px
 * radius and hairline border, an element's 1.5px radius and 0.7px outline,
 * and the paper's `0 1.5px 4px` drop shadow. Every one of them is
 * multiplied by the frame's scale, so the 1280×720 snapshot is the same
 * wireframe the 480×270 scaffold is, drawn larger.
 */
const PAGE_DESIGN = {
  radius: 2,
  border: 1,
  elementRadius: 1.5,
  elementBorder: 0.7,
  shadowDy: 1.5,
  shadowBlur: 4,
} as const;

/**
 * The layout canvas's own pixels per millimetre of page: it places every
 * element with `mmToPx` at 96 DPI. A size an element carries in those pixels
 * — a text block's font — is divided by this and multiplied by the frame's
 * own pixels per millimetre, so the text keeps the share of the page it has
 * on the canvas.
 */
const CANVAS_PX_PER_MM = mmToPx(1);

/** The size a legend's title is set at on the canvas, in its own pixels:
 * `LegendElementRenderer` writes it in the `subtitle2` variant, 0.875rem. */
const LEGEND_TITLE_FONT_SIZE = 14;

/** How much of a legend's height its title takes, as a share of the title's
 * own line: the text plus the gap to the first swatch row. A title asking
 * for more than half the box is not drawn at all — the swatch rows below it
 * would have nowhere to go. */
const TITLE_LINE = 1.7;
const TITLE_MAX_SHARE = 0.5;

/** The smallest text either drawing writes: below this a glyph is a smudge
 * rather than a word, and the abstract placeholder reads better. */
const MIN_FONT_SIZE = 3;

/** The thinnest a frame is drawn at the scaffold frame's own size, scaled
 * with it: the layout allows a border of 0.1mm, which is under a fifth of a
 * pixel there. */
const MIN_FRAME_WIDTH = 0.5;

const round = (value: number): number => Math.round(value * 100) / 100;

/** The paper the layout prints on, placed in the frame. */
export interface LayoutDrawingPage extends PreviewBox {
  radius: number;
  border: number;
}

/** Where a text block's markup is laid out and at what size — the element's
 * own box, and the canvas's font scaled into the frame, with every size
 * inside the markup relative to it. */
export interface LayoutDrawingProse extends PreviewBox {
  fontSize: number;
}

/** A line of an element's own text, placed: a legend's title. `x`/`y` are
 * the text's start and its baseline. */
export interface LayoutDrawingText {
  text: string;
  x: number;
  y: number;
  fontSize: number;
}

/** The frame an element draws around itself, in its own colour and at its
 * own width — scaled from the millimetres the layout stores. */
export interface LayoutDrawingFrame {
  color: string;
  width: number;
}

/** The ground an element is filled with, in its own colour and at its own
 * opacity. */
export interface LayoutDrawingBackground {
  color: string;
  opacity: number;
}

/** One element as drawn: what it is filled and framed with, and its own
 * content inside it. */
export interface LayoutDrawingElement {
  type: string;
  x: number;
  y: number;
  w: number;
  h: number;
  /** The box the element's content is drawn in: its own, inside whatever
   * padding it keeps. */
  content: PreviewBox;
  radius: number;
  border: number;
  /** The page's content blocks — a map, a chart, a picture, a table — take
   * the content tone for their outline; its furniture takes the muted one. */
  filled: boolean;
  glyphs: GlyphShape[];
  /** The frame the element itself asks for. Null where it asks for none,
   * which is a layout element's default — a thumbnail then shows the block's
   * content alone, since the frame is the layout's decoration and the
   * content is what says what the block is. */
  frame: LayoutDrawingFrame | null;
  /** The ground the element itself asks for. Null where it has none. */
  background: LayoutDrawingBackground | null;
  /** How opaque the element is as a whole. */
  opacity: number;
  /** An element that asks for no frame, no ground and has no content to draw
   * would be nothing at all: this draws it the faintest outline instead, so
   * the block does not vanish from the page. */
  placeholder: boolean;
  /** A map frame's frozen camera, for a drawing that can fetch a basemap
   * frame of it. Null on every other element type, and on a map frame that
   * names no camera — which is drawn as the placeholder map alone. */
  viewState: TemplatePreviewViewState | null;
  /** A text block's own markup, already sanitised by the descriptor. Null on
   * every other element type and on a block carrying no text. */
  html: string | null;
  /** Where that markup is laid out. Null wherever there is none. */
  prose: LayoutDrawingProse | null;
  /** A legend's own title, placed and cut to the width it has. Null for a
   * legend that shows none, on every other element type, and in a box with
   * no room for both a title and the rows under it. */
  title: LayoutDrawingText | null;
}

/**
 * A whole layout drawing, in the frame's own coordinates: what to draw and
 * where, with no colour and no markup in it. The one geometry source behind
 * both drawings of a layout — the stored PNG's SVG string and the React
 * scaffold rendered into the page — so the two are the same picture.
 */
export interface LayoutDrawing {
  width: number;
  height: number;
  /** What the frame was scaled by against the scaffold's own size. */
  scale: number;
  orientation: "portrait" | "landscape";
  page: LayoutDrawingPage;
  /** The paper's shadow, scaled. Only the stored PNG draws it: the scaffold
   * sits in a frame of its own and reads flat. */
  shadow: { dy: number; blur: number };
  elements: LayoutDrawingElement[];
}

export interface LayoutDrawingOptions {
  /** The frame to fit the page into. */
  size?: { width: number; height: number };
  padding?: number;
}

/**
 * Everything a layout drawing consists of: the page `meet`-fitted and
 * centred in the frame — a portrait page in a 16:9 frame keeps margins
 * either side — and one placeholder per element, placed in the page's own
 * units and carrying the glyph its element type reads as.
 *
 * An element that carries content of its own carries it through: a map
 * frame's camera, a text block's markup in the box and at the size it is set
 * at, a legend's title above its rows. The placeholder glyph stays under a
 * map frame, since whether a basemap frame can be fetched for it is only
 * known where the drawing is written.
 */
export const layoutDrawing = (
  descriptor: TemplateLayoutPreview | null | undefined,
  options: LayoutDrawingOptions = {}
): LayoutDrawing => {
  const { width, height } = options.size ?? SNAPSHOT_SIZE;
  const scale = width / SCAFFOLD_FRAME.width;
  const {
    page,
    elements,
    scale: pxPerMm,
  } = fitLayout(descriptor, width, height, options.padding ?? SCAFFOLD_PADDING * scale);

  const elementRadius = round(PAGE_DESIGN.elementRadius * scale);
  const elementBorder = round(PAGE_DESIGN.elementBorder * scale);
  // A size the canvas carries in its own pixels, in the frame's.
  const textScale = pxPerMm / CANVAS_PX_PER_MM;

  return {
    width,
    height,
    scale,
    orientation: descriptor?.orientation ?? "portrait",
    page: {
      ...page,
      radius: round(PAGE_DESIGN.radius * scale),
      border: round(PAGE_DESIGN.border * scale),
    },
    shadow: { dy: round(PAGE_DESIGN.shadowDy * scale), blur: round(PAGE_DESIGN.shadowBlur * scale) },
    elements: elements.map((element): LayoutDrawingElement => {
      const style = element.style ?? null;
      // The element's own padding, from page millimetres into the frame.
      const padding = (style?.padding ?? 0) * pxPerMm;
      const box = {
        x: round(element.x + padding),
        y: round(element.y + padding),
        w: round(Math.max(element.w - 2 * padding, 0)),
        h: round(Math.max(element.h - 2 * padding, 0)),
      };
      // A border of 0.1mm is a tenth of a pixel in a card band: it is drawn
      // at the thinnest width that still shows rather than not at all.
      const frame = style?.border
        ? {
            color: style.border.color,
            width: round(Math.max(style.border.width * pxPerMm, MIN_FRAME_WIDTH * scale)),
          }
        : null;
      const background = style?.background
        ? { color: style.background.color, opacity: style.background.opacity }
        : null;
      const html = element.html ?? null;
      const proseFontSize = round((element.fontSize ?? LAYOUT_TEXT_FONT_SIZE) * textScale);
      // A block whose text would come out smaller than a glyph keeps the
      // glyph instead: the words would not be readable at that size.
      const prose = html && proseFontSize >= MIN_FONT_SIZE ? { ...box, fontSize: proseFontSize } : null;

      const titleFontSize = round(LEGEND_TITLE_FONT_SIZE * textScale);
      const titleInset = glyphInset(box);
      const titleBand = titleFontSize * TITLE_LINE;
      const titleRoom = titleFontSize >= MIN_FONT_SIZE && titleBand <= box.h * TITLE_MAX_SHARE;
      const titleText = titleRoom
        ? truncateLabel(element.title ?? "", Math.max(box.w - 2 * titleInset, 0), titleFontSize)
        : "";
      const title = titleText
        ? {
            text: titleText,
            x: round(box.x + titleInset),
            y: round(box.y + titleInset + titleFontSize),
            fontSize: titleFontSize,
          }
        : null;

      // The glyph is drawn in what the element's own content leaves of the
      // box: nothing for a text block that carries its words, and the rows
      // under a legend's title.
      const glyphBox = title ? { ...box, y: box.y + titleBand, h: Math.max(box.h - titleBand, 0) } : box;

      const glyphs = prose ? [] : layoutGlyph(element.type, glyphBox);
      const viewState = element.viewState ?? null;

      return {
        type: element.type,
        x: element.x,
        y: element.y,
        w: element.w,
        h: element.h,
        content: box,
        radius: elementRadius,
        border: elementBorder,
        filled: FILLED_ELEMENTS.has(element.type),
        glyphs,
        frame,
        background,
        opacity: style?.opacity ?? 1,
        placeholder:
          !frame && !background && !viewState && !prose && !title && glyphs.length === 0 && element.w > 0,
        viewState,
        html: prose ? html : null,
        prose,
        title,
      };
    }),
  };
};
