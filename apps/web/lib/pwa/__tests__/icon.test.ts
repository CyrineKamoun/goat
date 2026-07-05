import sharp from "sharp";
import { describe, expect, it } from "vitest";

import { isAllowedIconSize, rasterizeToPng, resolveAppIconUrl } from "@/lib/pwa/icon";

const SVG = Buffer.from(
  `<svg xmlns="http://www.w3.org/2000/svg" width="100" height="100"><circle cx="50" cy="50" r="40" fill="#2BB381"/></svg>`
);

describe("isAllowedIconSize", () => {
  it("accepts 180, 192, 512", () => {
    expect(isAllowedIconSize(180)).toBe(true);
    expect(isAllowedIconSize(192)).toBe(true);
    expect(isAllowedIconSize(512)).toBe(true);
  });
  it("rejects other sizes", () => {
    expect(isAllowedIconSize(0)).toBe(false);
    expect(isAllowedIconSize(256)).toBe(false);
    expect(isAllowedIconSize(NaN)).toBe(false);
  });
});

describe("resolveAppIconUrl", () => {
  it("returns the configured app icon", () => {
    expect(resolveAppIconUrl({ app_icon_url: "https://x.test/icon.svg" })).toBe(
      "https://x.test/icon.svg"
    );
  });
  it("returns null for absent/empty settings (GOAT fallback, never the favicon)", () => {
    expect(resolveAppIconUrl({ app_icon_url: "" })).toBeNull();
    expect(resolveAppIconUrl({})).toBeNull();
    expect(resolveAppIconUrl(undefined)).toBeNull();
    expect(resolveAppIconUrl(null)).toBeNull();
  });
});

describe("rasterizeToPng", () => {
  it("produces a square PNG of the requested size from SVG", async () => {
    const png = await rasterizeToPng(SVG, 192);
    const meta = await sharp(png).metadata();
    expect(meta.format).toBe("png");
    expect(meta.width).toBe(192);
    expect(meta.height).toBe(192);
  });
  it("throws on garbage input", async () => {
    await expect(rasterizeToPng(Buffer.from("not an image"), 192)).rejects.toThrow();
  });
});
