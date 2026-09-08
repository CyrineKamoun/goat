import { describe, expect, it } from "vitest";

import { tagColor } from "@/lib/utils/tagColor";

describe("tagColor", () => {
  it("gives the same name the same colour every time", () => {
    expect(tagColor("Mobility")).toEqual(tagColor("Mobility"));
    expect(tagColor("Mobility").fg).toMatch(/^hsl\(\d{1,3}, \d{1,3}%, \d{1,3}%\)$/);
    expect(tagColor("Mobility").bg).toMatch(/^hsla\(\d{1,3}, \d{1,3}%, \d{1,3}%, 0\.14\)$/);
  });

  it("reads a name the way the backend groups one: trimmed and case-insensitively", () => {
    expect(tagColor("mobility")).toEqual(tagColor("Mobility"));
    expect(tagColor("  Mobility ")).toEqual(tagColor("Mobility"));
  });

  it("gives different names different colours", () => {
    const names = [
      "Mobility",
      "Transit",
      "Reporting",
      "Environment",
      "Accessibility",
      "Cycling",
      "Population",
      "Housing",
    ];
    const hues = new Set(names.map((name) => tagColor(name).fg));
    expect(hues.size).toBe(names.length);
  });

  it("keeps the foreground far enough from the paper it sits on, hue by hue", () => {
    // 11px bold text on its own wash: dark enough on light paper, light
    // enough on dark, for every hue the hash can produce.
    const lightnessOf = (colour: string) => Number(colour.split(", ")[2].replace("%)", ""));
    const names = Array.from({ length: 200 }, (_, index) => `tag-${index}`);
    for (const name of names) {
      expect(lightnessOf(tagColor(name).fg)).toBeLessThanOrEqual(32);
      expect(lightnessOf(tagColor(name, "dark").fg)).toBeGreaterThanOrEqual(76);
    }
  });

  it("lightens the foreground on a dark palette, keeping the hue", () => {
    const light = tagColor("Mobility");
    const dark = tagColor("Mobility", "dark");
    expect(dark.fg).not.toBe(light.fg);
    expect(dark.fg.split(",")[0]).toBe(light.fg.split(",")[0]);
  });
});
