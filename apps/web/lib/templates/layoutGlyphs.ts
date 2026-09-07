/**
 * The drawing a layout element stands for in a template's scaffold: not the
 * content (a descriptor carries none) but a stylised placeholder that says
 * what kind of block sits there — a map, a legend, a picture, a chart. Every
 * glyph is a pure function of the box it is drawn in, so the same element
 * reads the same at card size and in the 760px preview, and nothing about it
 * is random.
 */

/** How a shape is coloured. The scaffold maps each role onto the theme, so
 * one glyph reads in both themes without carrying a colour of its own. */
export type GlyphRole =
  /** The block's own tint — a map's land, a chart's bars. */
  | "tint"
  /** A softer wash than `tint`, for what sits behind it. */
  | "wash"
  /** A drawn line — a road, a table rule, a text line. */
  | "line"
  /** A line that carries the block's accent rather than the muted tone. */
  | "accent";

export type GlyphShape =
  | { shape: "rect"; x: number; y: number; w: number; h: number; rx?: number; role: GlyphRole }
  | { shape: "line"; x1: number; y1: number; x2: number; y2: number; width: number; role: GlyphRole }
  | { shape: "path"; d: string; role: GlyphRole; stroke?: number }
  | { shape: "circle"; cx: number; cy: number; r: number; role: GlyphRole };

export interface GlyphBox {
  x: number;
  y: number;
  w: number;
  h: number;
}

/** The element types drawn with a filled ground behind their glyph: the
 * content of the page, as opposed to its furniture. */
export const FILLED_ELEMENTS = new Set([
  "map",
  "chart",
  "histogram_chart",
  "categories_chart",
  "pie_chart",
  "image",
  "table",
]);

const round = (value: number): number => Math.round(value * 100) / 100;

const clamp = (value: number, min: number, max: number): number => Math.min(Math.max(value, min), max);

/** A line width that stays visible at card size without turning into a bar
 * on the big preview. */
const hairline = (box: GlyphBox): number => clamp(Math.min(box.w, box.h) * 0.03, 0.4, 1.2);

/** The inset a glyph keeps from its element's own edge. Exported because a
 * drawing that writes an element's real content into it — a legend's own
 * title — aligns that text with the glyph rows under it. */
export const glyphInset = (box: GlyphBox): number => clamp(Math.min(box.w, box.h) * 0.12, 1, 8);

/** Evenly spaced rows inside a box: `count` positions, each the centre of its
 * own share of the height. */
const rows = (box: GlyphBox, count: number, padding: number): number[] => {
  const top = box.y + padding;
  const usable = Math.max(box.h - 2 * padding, 0);
  const step = usable / count;
  return Array.from({ length: count }, (_unused, index) => round(top + step * (index + 0.5)));
};

/** Text as a stack of lines, the last one short — the shape a paragraph
 * reads as. Shared by `text` and `metadata`, which differ only in how many
 * lines fit. */
const textLines = (box: GlyphBox, count: number): GlyphShape[] => {
  const padding = glyphInset(box);
  const width = Math.max(box.w - 2 * padding, 0);
  const thickness = clamp(box.h * 0.06, 0.6, 2.4);
  const widths = [1, 0.92, 0.97, 0.88, 0.6];
  return rows(box, count, padding).map((y, index) => ({
    shape: "rect" as const,
    x: round(box.x + padding),
    y: round(y - thickness / 2),
    w: round(width * (index === count - 1 ? 0.62 : widths[index % widths.length])),
    h: round(thickness),
    rx: round(thickness / 2),
    role: "line" as const,
  }));
};

/** A map: soft land blocks with a couple of roads across them — the shape a
 * map reads as at a glance, drawn from fixed fractions of the box so it is
 * the same map every time. */
const mapGlyph = (box: GlyphBox): GlyphShape[] => {
  const { x, y, w, h } = box;
  const at = (fx: number, fy: number): string => `${round(x + w * fx)} ${round(y + h * fy)}`;
  const road = clamp(Math.min(w, h) * 0.045, 0.5, 2);
  return [
    { shape: "rect", x, y, w, h, role: "wash" },
    // Two land masses and a smaller block, in the tint.
    {
      shape: "path",
      d: `M ${at(0.06, 0.62)} L ${at(0.3, 0.38)} L ${at(0.52, 0.5)} L ${at(0.46, 0.94)} L ${at(0.06, 0.94)} Z`,
      role: "tint",
    },
    {
      shape: "path",
      d: `M ${at(0.58, 0.08)} L ${at(0.94, 0.14)} L ${at(0.94, 0.46)} L ${at(0.62, 0.4)} Z`,
      role: "tint",
    },
    {
      shape: "rect",
      x: round(x + w * 0.64),
      y: round(y + h * 0.6),
      w: round(w * 0.26),
      h: round(h * 0.24),
      rx: round(Math.min(w, h) * 0.03),
      role: "tint",
    },
    // The roads: one across, one down, meeting off-centre.
    {
      shape: "line",
      x1: x,
      y1: round(y + h * 0.55),
      x2: x + w,
      y2: round(y + h * 0.42),
      width: road,
      role: "line",
    },
    {
      shape: "line",
      x1: round(x + w * 0.4),
      y1: y,
      x2: round(x + w * 0.56),
      y2: y + h,
      width: road,
      role: "line",
    },
  ];
};

/** A legend's rows: at most this many, and never one shorter than this, so
 * a short block shows fewer entries rather than a stack of slivers. */
const LEGEND_MAX_ROWS = 4;
const LEGEND_MIN_ROW = 5;

/**
 * A legend: one row per entry, each a square swatch and the label rule
 * beside it. Every row is the same height and carries the same swatch — one
 * shape throughout, the fill square a legend of areas shows — and the label
 * rules vary in length so the column reads as text rather than as a
 * repeated bar. A box too short for `LEGEND_MAX_ROWS` rows draws as many as
 * fit at `LEGEND_MIN_ROW` apiece.
 *
 * The title above these rows is not a glyph: it is the legend's own, drawn
 * as real text by whoever draws the layout, which hands this the box left
 * under it.
 */
const legendGlyph = (box: GlyphBox): GlyphShape[] => {
  const padding = glyphInset(box);
  const usableHeight = Math.max(box.h - 2 * padding, 0);
  const usableWidth = Math.max(box.w - 2 * padding, 0);
  if (usableHeight <= 0 || usableWidth <= 0) return [];

  const count = Math.round(clamp(Math.floor(usableHeight / LEGEND_MIN_ROW), 1, LEGEND_MAX_ROWS));
  const row = usableHeight / count;
  // The swatch is sized off the row, and capped against the width so a
  // narrow legend keeps room for its labels.
  const swatch = clamp(Math.min(row * 0.6, usableWidth * 0.22), 1, 12);
  const thickness = Math.max(round(swatch * 0.3), 0.7);
  const labelX = box.x + padding + swatch * 1.65;
  const labelWidth = Math.max(box.x + box.w - padding - labelX, 0);
  const labelShares = [0.94, 0.68, 0.86, 0.56];

  return Array.from({ length: count }, (_unused, index) => index).flatMap((index): GlyphShape[] => {
    const center = box.y + padding + row * (index + 0.5);
    return [
      {
        shape: "rect",
        x: round(box.x + padding),
        y: round(center - swatch / 2),
        w: round(swatch),
        h: round(swatch),
        rx: round(swatch * 0.2),
        role: "tint",
      },
      {
        shape: "rect",
        x: round(labelX),
        y: round(center - thickness / 2),
        w: round(labelWidth * labelShares[index % labelShares.length]),
        h: thickness,
        rx: round(thickness / 2),
        role: "line",
      },
    ];
  });
};

/** A picture: the frame with a mountain and a sun in it — the placeholder an
 * empty image block stands for. */
const imageGlyph = (box: GlyphBox): GlyphShape[] => {
  const { x, y, w, h } = box;
  const padding = glyphInset(box);
  const frame = {
    x: round(x + padding),
    y: round(y + padding),
    w: round(Math.max(w - 2 * padding, 0)),
    h: round(Math.max(h - 2 * padding, 0)),
  };
  const at = (fx: number, fy: number): string =>
    `${round(frame.x + frame.w * fx)} ${round(frame.y + frame.h * fy)}`;
  return [
    { shape: "rect", x, y, w, h, role: "wash" },
    {
      shape: "rect",
      ...frame,
      rx: round(Math.min(frame.w, frame.h) * 0.06),
      role: "line",
    },
    {
      shape: "circle",
      cx: round(frame.x + frame.w * 0.28),
      cy: round(frame.y + frame.h * 0.28),
      r: round(Math.min(frame.w, frame.h) * 0.1),
      role: "accent",
    },
    {
      shape: "path",
      d: `M ${at(0.08, 0.86)} L ${at(0.38, 0.44)} L ${at(0.6, 0.72)} L ${at(0.74, 0.56)} L ${at(0.94, 0.86)} Z`,
      role: "tint",
    },
  ];
};

/** A table: the header rule, two body rules and two column rules. */
const tableGlyph = (box: GlyphBox): GlyphShape[] => {
  const { x, y, w, h } = box;
  const width = hairline(box);
  const header = round(y + h * 0.22);
  return [
    { shape: "rect", x, y, w, h: round(h * 0.22), role: "tint" },
    ...[header, round(y + h * 0.48), round(y + h * 0.74)].map(
      (lineY): GlyphShape => ({ shape: "line", x1: x, y1: lineY, x2: x + w, y2: lineY, width, role: "line" })
    ),
    ...[0.36, 0.7].map(
      (fraction): GlyphShape => ({
        shape: "line",
        x1: round(x + w * fraction),
        y1: y,
        x2: round(x + w * fraction),
        y2: y + h,
        width,
        role: "line",
      })
    ),
  ];
};

/** Bars on a baseline: what a histogram, a categories chart and the legacy
 * `chart` type all read as. */
const barsGlyph = (box: GlyphBox): GlyphShape[] => {
  const padding = glyphInset(box);
  const baseline = box.y + box.h - padding;
  const heights = [0.42, 0.72, 0.55, 0.9, 0.34];
  const usableWidth = Math.max(box.w - 2 * padding, 0);
  const usableHeight = Math.max(box.h - 2 * padding, 0);
  const step = usableWidth / heights.length;
  const barWidth = Math.max(step * 0.62, 0.5);
  return [
    ...heights.map((fraction, index): GlyphShape => {
      const barHeight = round(usableHeight * fraction);
      return {
        shape: "rect",
        x: round(box.x + padding + step * index + (step - barWidth) / 2),
        y: round(baseline - barHeight),
        w: round(barWidth),
        h: barHeight,
        rx: round(Math.min(barWidth * 0.25, 1.5)),
        role: "tint",
      };
    }),
    {
      shape: "line",
      x1: round(box.x + padding),
      y1: round(baseline),
      x2: round(box.x + box.w - padding),
      y2: round(baseline),
      width: hairline(box),
      role: "line",
    },
  ];
};

/** A pie: the full disc in the wash, with one slice in the tint. */
const pieGlyph = (box: GlyphBox): GlyphShape[] => {
  const cx = box.x + box.w / 2;
  const cy = box.y + box.h / 2;
  const r = Math.max(Math.min(box.w, box.h) / 2 - glyphInset(box), 0.5);
  // A 100° slice, drawn from twelve o'clock clockwise.
  const angle = (100 * Math.PI) / 180;
  const endX = round(cx + r * Math.sin(angle));
  const endY = round(cy - r * Math.cos(angle));
  return [
    { shape: "circle", cx: round(cx), cy: round(cy), r: round(r), role: "wash" },
    {
      shape: "path",
      d: `M ${round(cx)} ${round(cy)} L ${round(cx)} ${round(cy - r)} A ${round(r)} ${round(
        r
      )} 0 0 1 ${endX} ${endY} Z`,
      role: "tint",
    },
  ];
};

/** A scalebar: the segmented bar, alternate segments filled, on its rule. */
const scalebarGlyph = (box: GlyphBox): GlyphShape[] => {
  const segments = 4;
  const height = clamp(box.h * 0.4, 1.2, 6);
  const y = round(box.y + box.h / 2 - height / 2);
  const step = box.w / segments;
  return [
    { shape: "rect", x: box.x, y, w: box.w, h: round(height), role: "line" },
    ...Array.from({ length: segments }, (_unused, index) => index)
      .filter((index) => index % 2 === 0)
      .map(
        (index): GlyphShape => ({
          shape: "rect",
          x: round(box.x + step * index),
          y,
          w: round(step),
          h: round(height),
          role: "tint",
        })
      ),
  ];
};

/** A north arrow: the needle with its own tail, pointing up. */
const northArrowGlyph = (box: GlyphBox): GlyphShape[] => {
  const side = Math.min(box.w, box.h);
  const cx = box.x + box.w / 2;
  const cy = box.y + box.h / 2;
  const half = side / 2 - glyphInset(box) / 2;
  const at = (fx: number, fy: number): string => `${round(cx + half * fx)} ${round(cy + half * fy)}`;
  return [
    { shape: "path", d: `M ${at(0, -1)} L ${at(0.62, 0.9)} L ${at(0, 0.42)} Z`, role: "tint" },
    { shape: "path", d: `M ${at(0, -1)} L ${at(-0.62, 0.9)} L ${at(0, 0.42)} Z`, role: "line" },
  ];
};

/** A QR code: a fixed 5×5 checker with the three corner eyes, so it reads as
 * a code rather than as noise. */
const qrGlyph = (box: GlyphBox): GlyphShape[] => {
  // `glyphInset` never goes below 1, so a box smaller than that has no room for
  // the code at all — element sizes carry no minimum.
  const side = Math.min(box.w, box.h) - glyphInset(box);
  if (side <= 0) return [];
  const cell = side / 5;
  const originX = box.x + (box.w - side) / 2;
  const originY = box.y + (box.h - side) / 2;
  // Row-major: the eyes at three corners plus a fixed scatter between them.
  const cells: [number, number][] = [
    [0, 0],
    [0, 4],
    [4, 0],
    [1, 2],
    [2, 1],
    [2, 3],
    [3, 2],
    [2, 2],
    [4, 3],
    [3, 4],
  ];
  return cells.map(([row, column]) => ({
    shape: "rect" as const,
    x: round(originX + cell * column),
    y: round(originY + cell * row),
    w: round(cell * 0.82),
    h: round(cell * 0.82),
    rx: round(cell * 0.14),
    role: "tint" as const,
  }));
};

/** A divider: the rule it is. */
const dividerGlyph = (box: GlyphBox): GlyphShape[] => [
  {
    shape: "line",
    x1: box.x,
    y1: round(box.y + box.h / 2),
    x2: box.x + box.w,
    y2: round(box.y + box.h / 2),
    width: clamp(box.h * 0.4, 0.6, 2),
    role: "line",
  },
];

/**
 * The glyph a layout element type is drawn as, inside `box`. An element type
 * nothing is known about gets no glyph — the scaffold draws its outline
 * alone rather than inventing a picture for it.
 */
export const layoutGlyph = (type: string, box: GlyphBox): GlyphShape[] => {
  if (box.w <= 0 || box.h <= 0) return [];
  switch (type) {
    case "map":
      return mapGlyph(box);
    case "legend":
      return legendGlyph(box);
    case "text":
      return textLines(box, 4);
    case "metadata":
      return textLines(box, 3);
    case "image":
      return imageGlyph(box);
    case "table":
      return tableGlyph(box);
    case "chart":
    case "histogram_chart":
    case "categories_chart":
      return barsGlyph(box);
    case "pie_chart":
      return pieGlyph(box);
    case "scalebar":
      return scalebarGlyph(box);
    case "north_arrow":
      return northArrowGlyph(box);
    case "qr_code":
      return qrGlyph(box);
    case "divider":
      return dividerGlyph(box);
    default:
      return [];
  }
};
