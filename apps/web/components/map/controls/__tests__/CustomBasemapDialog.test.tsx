import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { CustomBasemapDialog } from "@/components/map/controls/CustomBasemapDialog";

vi.mock("react-i18next", () => ({ useTranslation: () => ({ t: (k: string) => k }) }));
vi.mock("react-map-gl/maplibre", () => ({ useMap: () => ({ map: undefined }) }));
vi.mock("@/hooks/store/ContextHooks", () => ({ useAppDispatch: () => vi.fn() }));

describe("CustomBasemapDialog", () => {
  it("renders the add title with its tabs and the add action", () => {
    render(<CustomBasemapDialog open onClose={vi.fn()} onSubmit={vi.fn()} />);

    expect(screen.getByRole("tab", { name: "basemap_tab_label" })).toBeInTheDocument();
    expect(screen.getByRole("tab", { name: "solid_color" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "add_basemap" })).toBeInTheDocument();
  });

  it("refuses to submit a basemap without a name", () => {
    const onSubmit = vi.fn();
    render(<CustomBasemapDialog open onClose={vi.fn()} onSubmit={onSubmit} />);

    fireEvent.click(screen.getByRole("button", { name: "add_basemap" }));
    expect(onSubmit).not.toHaveBeenCalled();
    expect(screen.getByText("basemap_name_required")).toBeInTheDocument();
  });
});
