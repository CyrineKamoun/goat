/**
 * Border and background of a layout element, resolved into the CSS values
 * the editor canvas and the print page paint on the element box.
 */
import { mmToPx, SCREEN_DPI } from "@/lib/print/units";

export interface ElementFrameStyle {
  border?: { enabled?: boolean; color?: string; width?: number };
  background?: { enabled?: boolean; color?: string; opacity?: number };
}

export interface ElementFrame {
  /** CSS `border` shorthand, or `none`. */
  border: string;
  /** CSS colour, or `transparent`. */
  backgroundColor: string;
}

const DEFAULT_BORDER_COLOR = "#000000";
const DEFAULT_BORDER_WIDTH_MM = 0.5;
const DEFAULT_BACKGROUND_COLOR = "#ffffff";

const hexToRgba = (hex: string, opacity: number): string => {
  const r = parseInt(hex.slice(1, 3), 16);
  const g = parseInt(hex.slice(3, 5), 16);
  const b = parseInt(hex.slice(5, 7), 16);
  return `rgba(${r}, ${g}, ${b}, ${opacity})`;
};

/**
 * Resolve an element's frame. `scale` multiplies the border width, for a
 * host that draws at a canvas zoom without a CSS transform.
 */
export function resolveElementFrame(style: ElementFrameStyle | undefined, scale = 1): ElementFrame {
  const border = style?.border;
  const background = style?.background;

  const borderCss = border?.enabled
    ? `${mmToPx(border.width ?? DEFAULT_BORDER_WIDTH_MM, SCREEN_DPI) * scale}px solid ${border.color ?? DEFAULT_BORDER_COLOR}`
    : "none";

  const backgroundColor = background?.enabled
    ? hexToRgba(background.color ?? DEFAULT_BACKGROUND_COLOR, background.opacity ?? 1)
    : "transparent";

  return { border: borderCss, backgroundColor };
}
