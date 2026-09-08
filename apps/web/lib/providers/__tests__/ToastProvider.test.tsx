import { render } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import ThemeProvider from "@p4b/ui/theme/ThemeProvider";

import ToastProvider from "@/lib/providers/ToastProvider";

/** Every `--toastify-*` declaration emotion has injected, in injection order. */
const toastDeclarations = (name: string): string[] => {
  const found: string[] = [];
  for (const sheet of Array.from(document.styleSheets)) {
    for (const rule of Array.from(sheet.cssRules ?? [])) {
      const text = rule.cssText;
      if (!text.includes(name)) continue;
      const match = text.match(new RegExp(`${name}:\\s*([^;}]+)`));
      if (match) found.push(match[1].trim());
    }
  }
  return found;
};

describe("ToastProvider", () => {
  it("keeps the toast palette when a page mounts a fixed-mode theme inside it", () => {
    /**
     * The print preview wraps its paper in a light theme because the paper is
     * always white, and public dashboards do the same. Those nested providers
     * mount a second CssBaseline, injected after the root one — so anything
     * they declare about the toast wins globally. When the app's own
     * `--toastify-color-*` lived there, a dark-mode user got a white toast on
     * the print page while the text colour still came from the dark theme:
     * white on white.
     */
    render(
      <ThemeProvider settings={{ mode: "dark", locale: "en" }}>
        <ToastProvider>
          <ThemeProvider settings={{ mode: "light", locale: "en" }}>
            <div />
          </ThemeProvider>
        </ToastProvider>
      </ThemeProvider>
    );

    const backgrounds = toastDeclarations("--toastify-color-dark");
    expect(backgrounds.length, "the toast palette has exactly one owner").toBe(1);
    expect(backgrounds[0]?.toUpperCase()).not.toBe("#FFF");
  });
});
