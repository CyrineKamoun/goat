import type { Theme } from "@mui/material";

import { ICON_NAME } from "@p4b/ui/components/Icon";

import type { CatalogItem } from "@/lib/validations/catalog";

/** What kind of thing a catalog result is. */

/**
 * `vector` covers point, line and polygon alike; `unknown` is for an item whose
 * kind genuinely cannot be told from what the catalog publishes.
 */
export type CatalogKind = "bundle" | "raster" | "table" | "vector" | "unknown";

/** Pass `asMember` when the item is one layer of a bundle that is already on screen. */
export const catalogKindOf = (item: CatalogItem, asMember?: boolean): CatalogKind => {
  const props = item.properties;
  if (!asMember && (props["goat:member_count"] ?? 1) > 1) return "bundle";
  switch (props["goat:layerType"]) {
    case "raster":
      return "raster";
    case "table":
      return "table";
    case "feature":
      return "vector";
    default:
      if (props["goat:geometryType"]) return "vector";
      if (typeof props["table:row_count"] === "number") return "table";
      return "unknown";
  }
};

/**
 * What a mark can be drawn for. The catalog's five kinds, plus the three
 * things only Content holds — a project, a folder and a template have no
 * catalog kind, but they are marked from the same mapping so one type looks
 * the same on both pages.
 */
export type MarkKind = CatalogKind | "project" | "folder" | "template";

/** The subject of a mark: its kind, plus the shape detail a vector layer
 * states (`point` / `line` / `polygon`). */
export type MarkSubject = {
  kind: MarkKind;
  geometryType?: string | null;
};

/**
 * The one glyph mapping for a piece of content, wherever it is marked — a
 * catalog thumbnail, a feed card's placeholder, a list row's icon block, a
 * details panel. The most specific thing the caller knows wins: a table or a
 * raster is marked by its kind, a vector layer by its geometry, and anything
 * whose shape is genuinely unknown falls back to the generic layers glyph.
 */
export const markFor = ({ kind, geometryType }: MarkSubject): ICON_NAME => {
  if (kind === "folder") return ICON_NAME.FOLDER;
  if (kind === "project") return ICON_NAME.MAP;
  if (kind === "template") return ICON_NAME.CLONE;
  if (kind === "table") return ICON_NAME.TABLE;
  if (kind === "raster") return ICON_NAME.IMAGE;
  switch (geometryType) {
    case "point":
      return ICON_NAME.POINT_FEATURE;
    case "line":
      return ICON_NAME.LINE_FEATURE;
    case "polygon":
      return ICON_NAME.POLYGON_FEATURE;
    default:
      return ICON_NAME.LAYERS;
  }
};

/**
 * The palette keys a kind can be tinted with. `primary-dark` is the one that
 * is not a `<key>.main` colour — read every tone through `toneColorOf` rather
 * than indexing the palette directly.
 */
export type ContentTone = "primary" | "primary-dark" | "info" | "warning" | "secondary";

/**
 * The tone a kind carries wherever it is tinted — a list row's icon block, a
 * card with no picture of its own, a shortcut tile — so one kind looks the same
 * in every layout.
 *
 * A bundle takes `primary-dark` rather than `success`: `success` is a status
 * colour everywhere else in the app, and its green is close enough to the brand
 * mint that a bundle and a project were hard to tell apart in a mixed list.
 */
export const toneFor = (kind: MarkKind): ContentTone => {
  if (kind === "folder" || kind === "project") return "primary";
  if (kind === "bundle") return "primary-dark";
  if (kind === "raster") return "warning";
  if (kind === "table" || kind === "template") return "secondary";
  return "info";
};

/** A tone's colour. The tinted ground behind it is `alpha(this, 0.12)`. */
export const toneColorOf = (theme: Theme, tone: ContentTone): string =>
  tone === "primary-dark" ? theme.palette.primary.dark : theme.palette[tone].main;
