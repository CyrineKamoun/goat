/**
 * Moving a layout's elements onto a differently sized sheet.
 */
import type { PageMm } from "@/lib/print/units";
import type { ReportElement } from "@/lib/validations/reportLayout";

/** `font-size` as it appears in a typography value ("12pt") or an inline style ("font-size: 14px"). */
const FONT_SIZE_VALUE = /^(\d+(?:\.\d+)?)\s*(pt|mm|px)$/i;
const INLINE_FONT_SIZE = /(font-size:\s*)(\d+(?:\.\d+)?)(pt|mm|px)/gi;

const round1 = (n: number): number => Math.round(n * 10) / 10;

/** A deep copy of `value` with every font size in it multiplied by `factor`. */
function scaleFontSizes(value: unknown, factor: number): unknown {
  if (typeof value === "string") {
    const plain = value.match(FONT_SIZE_VALUE);
    if (plain) return `${round1(Number(plain[1]) * factor)}${plain[2]}`;
    if (value.includes("font-size")) {
      return value.replace(INLINE_FONT_SIZE, (_m, key: string, size: string, unit: string) => {
        return `${key}${round1(Number(size) * factor)}${unit}`;
      });
    }
    return value;
  }
  if (Array.isArray(value)) return value.map((item) => scaleFontSizes(item, factor));
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>).map(([key, item]) => [
        key,
        key === "fontSize" || typeof item !== "string" || item.includes("font-size")
          ? scaleFontSizes(item, factor)
          : item,
      ])
    );
  }
  return value;
}

/**
 * The elements as they sit on the `to` sheet when the layout is stretched
 * from the `from` sheet: x and width follow the width ratio, y and height the
 * height ratio, so the composition keeps its proportions on the page; font
 * sizes follow the smaller ratio so text never outgrows its box. An unchanged
 * sheet returns the elements as they are.
 */
export function scaleElementsToPage(elements: ReportElement[], from: PageMm, to: PageMm): ReportElement[] {
  const rx = to.width / from.width;
  const ry = to.height / from.height;
  if (rx === 1 && ry === 1) return elements;
  const rFont = Math.min(rx, ry);

  return elements.map((element) => ({
    ...element,
    position: {
      ...element.position,
      x: element.position.x * rx,
      y: element.position.y * ry,
      width: element.position.width * rx,
      height: element.position.height * ry,
    },
    config: scaleFontSizes(element.config, rFont) as ReportElement["config"],
  }));
}
