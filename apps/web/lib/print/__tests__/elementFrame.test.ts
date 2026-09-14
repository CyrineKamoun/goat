import { describe, expect, it } from "vitest";

import { resolveElementFrame } from "@/lib/print/elementFrame";

describe("resolveElementFrame", () => {
  it("draws nothing when neither border nor background is enabled", () => {
    expect(resolveElementFrame(undefined)).toEqual({ border: "none", backgroundColor: "transparent" });
    expect(resolveElementFrame({ border: { enabled: false }, background: { enabled: false } })).toEqual({
      border: "none",
      backgroundColor: "transparent",
    });
  });

  it("converts the border width from millimetres and scales it by the canvas zoom", () => {
    const frame = resolveElementFrame({ border: { enabled: true, color: "#ff0000", width: 1 } }, 2);
    // 1 mm at 96 dpi = 3.7795 px, doubled by the zoom
    expect(frame.border).toBe(`${(1 / 25.4) * 96 * 2}px solid #ff0000`);
  });

  it("falls back to the schema defaults for a border without colour or width", () => {
    const frame = resolveElementFrame({ border: { enabled: true } });
    expect(frame.border).toBe(`${(0.5 / 25.4) * 96}px solid #000000`);
  });

  it("renders the background as rgba with the configured opacity", () => {
    const frame = resolveElementFrame({ background: { enabled: true, color: "#336699", opacity: 0.5 } });
    expect(frame.backgroundColor).toBe("rgba(51, 102, 153, 0.5)");
  });
});
