import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import CatalogSpatialDialog from "@/components/dashboard/catalog/CatalogSpatialDialog";

vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    t: (key: string, vars?: Record<string, unknown>) => (vars ? `${key}:${JSON.stringify(vars)}` : key),
  }),
}));

vi.mock("react-map-gl/maplibre", () => ({
  Map: ({ children }: { children?: React.ReactNode }) => <div data-testid="map">{children}</div>,
  Layer: () => null,
  Source: ({ children }: { children?: React.ReactNode }) => <div>{children}</div>,
}));
vi.mock("@/components/dashboard/common/DetailMapAttribution", () => ({
  default: () => <div data-testid="attribution" />,
}));
vi.mock("@/hooks/catalog/useCatalogBasemapStyle", () => ({
  useCatalogBasemapStyle: () => undefined,
}));
vi.mock("@/hooks/catalog/useCatalogNutsGeometries", () => ({
  useCatalogNutsGeometries: () => ({ geometries: {}, names: {} }),
}));
vi.mock("@/lib/api/catalog", () => ({
  useCatalogNutsRegions: () => ({ regions: [], isLoading: false }),
}));

describe("CatalogSpatialDialog", () => {
  it("shows the title and keeps Apply disabled with no shape and no existing filter", () => {
    render(<CatalogSpatialDialog open initial={null} onClose={vi.fn()} onApply={vi.fn()} />);

    expect(screen.getByText("catalog_set_spatial_filter")).toBeInTheDocument();
    expect((screen.getByRole("button", { name: "apply" }) as HTMLButtonElement).disabled).toBe(true);
  });

  it("enables Apply once a region is already in force, so the dialog can be used to clear it", () => {
    render(
      <CatalogSpatialDialog
        open
        initial={{ kind: "region", nutsIds: ["DE1"], names: { DE1: "Baden-Württemberg" } }}
        onClose={vi.fn()}
        onApply={vi.fn()}
      />
    );

    expect((screen.getByRole("button", { name: "apply" }) as HTMLButtonElement).disabled).toBe(false);
  });

  it("applies null once Clear all is pressed on an existing filter", async () => {
    const onApply = vi.fn();
    render(
      <CatalogSpatialDialog
        open
        initial={{ kind: "region", nutsIds: ["DE1"], names: { DE1: "Baden-Württemberg" } }}
        onClose={vi.fn()}
        onApply={onApply}
      />
    );

    await userEvent.click(screen.getByRole("button", { name: "clear_all" }));
    await userEvent.click(screen.getByRole("button", { name: "apply" }));

    expect(onApply).toHaveBeenCalledWith(null);
  });
});
