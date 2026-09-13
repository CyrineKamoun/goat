import { describe, expect, it } from "vitest";

import type { ReportElement } from "@/lib/validations/reportLayout";

import { scaleElementsToPage } from "../pageResize";

const A4 = { width: 210, height: 297 };
const A3 = { width: 297, height: 420 };
const A4_LANDSCAPE = { width: 297, height: 210 };

const element = (overrides: Partial<ReportElement> = {}): ReportElement =>
  ({
    id: "el-1",
    type: "map",
    position: { x: 15, y: 32, width: 180, height: 150, z_index: 2 },
    config: {},
    ...overrides,
  }) as ReportElement;

describe("scaleElementsToPage", () => {
  it("scales x and width by the width ratio, y and height by the height ratio", () => {
    const [scaled] = scaleElementsToPage([element()], A4, A3);
    expect(scaled.position.x).toBeCloseTo(15 * (297 / 210), 3);
    expect(scaled.position.width).toBeCloseTo(180 * (297 / 210), 3);
    expect(scaled.position.y).toBeCloseTo(32 * (420 / 297), 3);
    expect(scaled.position.height).toBeCloseTo(150 * (420 / 297), 3);
    expect(scaled.position.z_index).toBe(2);
  });

  it("keeps the composition's proportions when the sheet turns", () => {
    const [scaled] = scaleElementsToPage([element()], A4, A4_LANDSCAPE);
    expect(scaled.position.x).toBeCloseTo(15 * (297 / 210), 3);
    expect(scaled.position.y).toBeCloseTo(32 * (210 / 297), 3);
  });

  it("scales a typography font size by the smaller ratio, to one decimal", () => {
    const [scaled] = scaleElementsToPage(
      [element({ type: "scalebar", config: { typography: { fontSize: "12pt", color: "#000" } } })],
      A4,
      A3
    );
    // min(297/210, 420/297) = 1.4141…; 12 × 1.4141 = 16.97 → 17
    expect(scaled.config.typography.fontSize).toBe("17pt");
    expect(scaled.config.typography.color).toBe("#000");
  });

  it("scales font sizes nested one level deeper, as a legend's per-role typography is", () => {
    const [scaled] = scaleElementsToPage(
      [
        element({
          type: "legend",
          config: { typography: { title: { fontSize: "10mm" }, label: { fontSize: "5pt" } } },
        }),
      ],
      A4,
      A3
    );
    expect(scaled.config.typography.title.fontSize).toBe("14.1mm");
    expect(scaled.config.typography.label.fontSize).toBe("7.1pt");
  });

  it("scales inline font-size declarations inside rich text markup", () => {
    const html =
      '<p><span style="font-size: 14px">Title</span> and <span style="font-size:10.5pt;">sub</span></p>';
    const [scaled] = scaleElementsToPage([element({ type: "text", config: { text: html } })], A4, A3);
    expect(scaled.config.text).toBe(
      '<p><span style="font-size: 19.8px">Title</span> and <span style="font-size:14.8pt;">sub</span></p>'
    );
  });

  it("returns the elements untouched when the sheet does not change", () => {
    const input = [element({ config: { typography: { fontSize: "12pt" } } })];
    expect(scaleElementsToPage(input, A4, { ...A4 })).toEqual(input);
  });

  it("leaves numbers and strings that are not font sizes alone", () => {
    const [scaled] = scaleElementsToPage(
      [element({ config: { padding: 4, label: "Scale 1:5000", typography: { fontWeight: "700" } } })],
      A4,
      A3
    );
    expect(scaled.config.padding).toBe(4);
    expect(scaled.config.label).toBe("Scale 1:5000");
    expect(scaled.config.typography.fontWeight).toBe("700");
  });
});
