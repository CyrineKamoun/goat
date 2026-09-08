import { render } from "@testing-library/react";

import ThemeProvider from "@p4b/ui/theme/ThemeProvider";
import { describe, expect, it, vi } from "vitest";

import CatalogBundleMembers from "@/components/dashboard/catalog/CatalogBundleMembers";

vi.mock("react-i18next", () => ({ useTranslation: () => ({ t: (key: string) => key }) }));
vi.mock("@/hooks/catalog/useCatalogLabels", () => ({
  useCatalogLabels: () => ({
    kindLabel: (v: string) => v ?? "",
    languageLabel: (v: string) => v ?? "",
    licenseLabel: (v: string) => v ?? "",
    formatPeriod: () => "",
    geometryLabel: (v: string) => v ?? "",
    rowCountLabel: (v: unknown) => String(v ?? ""),
  }),
}));

const items = vi.fn();
vi.mock("@/lib/api/catalog", () => ({
  useCatalogCollectionItems: () => ({ items: items(), isLoading: false, isError: false }),
}));

const members = (count: number) =>
  Array.from({ length: count }, (_, i) => ({
    id: `member-${i}`,
    properties: { title: `Layer ${i}`, "table:row_count": 10 },
  }));

/** `sx` compiles to a class, so the computed style is what to assert on. */
const withTheme = (ui: React.ReactNode) =>
  render(<ThemeProvider settings={{ mode: "dark", locale: "en" }}>{ui}</ThemeProvider>);

const listStyle = (container: HTMLElement) => {
  const list = container.querySelector<HTMLElement>(".MuiStack-root");
  if (!list) throw new Error("member list not rendered");
  return getComputedStyle(list);
};

describe("CatalogBundleMembers", () => {
  it("renders a small bundle inline, with no scroll box", () => {
    // 75% of multi-layer datasets have five layers or fewer — they must look
    // exactly as they did.
    items.mockReturnValue(members(5));

    const { container } = withTheme(<CatalogBundleMembers collectionId="c1" />);

    const style = listStyle(container);
    expect(style.overflowY).not.toBe("auto");
    expect(style.maxHeight).toBe("");
  });

  it("scrolls as soon as there are more than five", () => {
    items.mockReturnValue(members(6));

    const { container } = withTheme(<CatalogBundleMembers collectionId="c1" />);
    const style = listStyle(container);

    expect(style.overflowY).toBe("auto");
    /**
     * The cap must be a value a browser can use. This theme's spacing is rem
     * ("0.25rem"), so arithmetic on that string yields NaN, which MUI happily
     * serialises as `NaNpx` — the browser drops it and the list grows to full
     * height. Asserting "not none" or "contains px" passes on that; asserting
     * no NaN is what actually catches it.
     */
    expect(style.maxHeight).not.toMatch(/NaN/);
    expect(style.maxHeight).not.toBe("none");
    expect(style.maxHeight).not.toBe("");
  });

  it("still renders every fetched layer, so none can be missed in a picker", () => {
    items.mockReturnValue(members(40));

    const { getByTitle } = withTheme(
      <CatalogBundleMembers
        collectionId="c1"
        selection={{ isSelected: () => false, onToggle: vi.fn() }}
      />
    );

    expect(getByTitle("Layer 0")).toBeDefined();
    expect(getByTitle("Layer 39")).toBeDefined();
  });
});
