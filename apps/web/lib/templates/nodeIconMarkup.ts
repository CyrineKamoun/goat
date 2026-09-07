import type { ReactElement } from "react";

import { NODE_ICON_TONES, nodeIconElement } from "@/lib/templates/nodeIcons";

/**
 * The string side of a node icon: the same glyph the canvas renders, written
 * out as SVG markup for the stored PNG. It needs React's server renderer,
 * which is half a megabyte of SSR machinery — so it is loaded on demand
 * rather than imported, and nothing that only draws the in-page scaffold
 * pulls it into a page bundle. `TemplatePreviewFallback` renders the same
 * icons as real React elements instead.
 */

/** What MUI puts on an `SvgIcon` for the DOM it normally renders into: the
 * emotion class that carries `fill: currentColor` and the icon's own size,
 * plus its accessibility and test hooks. None of it survives outside that
 * DOM — the class is not in the drawing, and the test id would collide with
 * the drawing's own — so the attributes are dropped and the geometry is
 * sized and coloured explicitly instead. */
const HOST_ONLY_ATTRIBUTES = /\s(?:class|focusable|aria-hidden|data-testid)="[^"]*"/g;

const round = (value: number): number => Math.round(value * 100) / 100;

/** One `renderToStaticMarkup` per node type and icon: the markup is the same
 * every time, and a drawing asks for it once per node. */
const sourceCache = new Map<string, string>();

/** What React's server renderer resolves to, once. The module is only ever
 * asked for while a snapshot is being drawn. */
let serverRenderer: Promise<(element: ReactElement) => string> | null = null;

const loadServerRenderer = (): Promise<(element: ReactElement) => string> => {
  serverRenderer ??= import("react-dom/server").then((module) => module.renderToStaticMarkup);
  return serverRenderer;
};

/** The icon's own `<svg>…</svg>`, stripped of everything that only meant
 * something in the DOM. Empty for a node type that shows no icon. */
const iconSource = (
  renderToStaticMarkup: (element: ReactElement) => string,
  type: string,
  icon?: string | null
): string => {
  const key = `${type}|${icon ?? ""}`;
  const cached = sourceCache.get(key);
  if (cached !== undefined) return cached;

  const element = nodeIconElement(type, icon);
  const markup = element ? renderToStaticMarkup(element) : "";
  const start = markup.indexOf("<svg");
  const end = markup.lastIndexOf("</svg>");
  const source = start >= 0 && end > start ? markup.slice(start, end + "</svg>".length) : "";
  const cleaned = source.replace(HOST_ONLY_ATTRIBUTES, "");
  sourceCache.set(key, cleaned);
  return cleaned;
};

export interface NodeIconPlacement {
  x: number;
  y: number;
  /** The glyph's box, already scaled: the icon is drawn square in it. */
  size: number;
  /** What `currentColor` resolves to — the colour a single-tone glyph takes. */
  color: string;
  /** The five tool-icon tones, in order. */
  tones?: readonly string[];
}

/** Writes one node's icon into a drawing. */
export type NodeIconMarkup = (
  type: string,
  icon: string | null | undefined,
  placement: NodeIconPlacement
) => string;

/**
 * The markup writer, once React's server renderer has been loaded. Awaiting
 * this is what pulls `react-dom/server` in, so only the PNG path does.
 */
export const loadNodeIconMarkup = async (): Promise<NodeIconMarkup> => {
  const renderToStaticMarkup = await loadServerRenderer();
  return (type, icon, placement) => nodeIconMarkup(renderToStaticMarkup, type, icon, placement);
};

/**
 * A node's icon as a nested `<svg>`: the same glyph the canvas renders, at
 * (`x`, `y`) in a `size`×`size` box, in its own viewBox so nothing has to be
 * re-projected. Self-contained — no custom property, no class, no external
 * reference — so it renders the same in an SVG string rasterised through an
 * `<img>` as it does in the DOM.
 */
const nodeIconMarkup = (
  renderToStaticMarkup: (element: ReactElement) => string,
  type: string,
  icon: string | null | undefined,
  placement: NodeIconPlacement
): string => {
  const source = iconSource(renderToStaticMarkup, type, icon);
  if (!source) return "";
  const openTag = /^<svg([^>]*)>/.exec(source);
  if (!openTag) return "";
  const viewBox = /viewBox="([^"]*)"/.exec(openTag[1])?.[1] ?? "0 0 24 24";
  const inner = source.slice(openTag[0].length, source.lastIndexOf("</svg>"));
  const tones = placement.tones ?? NODE_ICON_TONES.light;
  const toned = inner.replace(
    /var\(--icon-color-(\d+),\s*([^)]*)\)/g,
    (_match, index: string, fallback: string) => tones[Number(index) - 1] ?? fallback
  );

  return [
    `<svg x="${round(placement.x)}" y="${round(placement.y)}" width="${round(
      placement.size
    )}" height="${round(placement.size)}" viewBox="${viewBox}" fill="currentColor" color="${
      placement.color
    }">`,
    toned,
    `</svg>`,
  ].join("");
};
