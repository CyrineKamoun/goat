/** The colour a category tag reads in: a hue derived from the tag's own
 * name, so the same tag looks the same everywhere it is rendered — the save
 * dialog's autocomplete, the browser's filter popover and chips, the preview
 * panel, the rows and the cards — without anyone storing a colour. Read the
 * way the backend groups categories: trimmed and case-insensitively, so
 * "Mobility" and "mobility" are one colour as they are one facet row. */

/** Saturation and the two lightnesses: the foreground carries 11px bold text
 * on its own `alpha .14` wash, so it has to clear ~4.5:1 there on the worst
 * hue of the wheel — hence 30% on light paper and 78% on dark. The background
 * is that same colour at the wash. */
const SATURATION = 55;
const LIGHTNESS = { light: 30, dark: 78 };
const BACKGROUND_ALPHA = 0.14;

/** 32-bit FNV-1a, folded onto the colour wheel. Cheap, stable across
 * reloads and machines, and spread enough that neighbouring names land on
 * visibly different hues. */
const hueOf = (value: string): number => {
  let hash = 0x811c9dc5;
  for (let index = 0; index < value.length; index += 1) {
    hash ^= value.charCodeAt(index);
    hash = Math.imul(hash, 0x01000193) >>> 0;
  }
  return hash % 360;
};

export interface TagColor {
  /** Text/border colour. */
  fg: string;
  /** The wash it sits on. */
  bg: string;
}

export const tagColor = (name: string, mode: "light" | "dark" = "light"): TagColor => {
  const hue = hueOf(name.trim().toLowerCase());
  const lightness = LIGHTNESS[mode];
  return {
    fg: `hsl(${hue}, ${SATURATION}%, ${lightness}%)`,
    bg: `hsla(${hue}, ${SATURATION}%, ${lightness}%, ${BACKGROUND_ALPHA})`,
  };
};
