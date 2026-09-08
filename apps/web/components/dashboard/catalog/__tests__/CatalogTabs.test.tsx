import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import CatalogTabs from "@/components/dashboard/catalog/CatalogTabs";

vi.mock("react-i18next", () => ({
  useTranslation: () => ({ t: (key: string) => key, i18n: { language: "en" } }),
}));

describe("CatalogTabs", () => {
  it("renders exactly the Datasets and Templates tabs", () => {
    render(<CatalogTabs active="datasets" onChange={vi.fn()} datasetCount={1234} />);

    const buttons = screen.getAllByRole("button");
    expect(buttons.map((button) => button.textContent)).toEqual(["catalog_tab_datasets1,234", "templates"]);
  });

  it("reports the Templates tab id when clicked", () => {
    const onChange = vi.fn();
    render(<CatalogTabs active="datasets" onChange={onChange} />);

    fireEvent.click(screen.getByRole("button", { name: "templates" }));

    expect(onChange).toHaveBeenCalledWith("templates");
  });

  it("reports the Datasets tab id when clicked from Templates", () => {
    const onChange = vi.fn();
    render(<CatalogTabs active="templates" onChange={onChange} />);

    fireEvent.click(screen.getByRole("button", { name: "catalog_tab_datasets" }));

    expect(onChange).toHaveBeenCalledWith("datasets");
  });
});
