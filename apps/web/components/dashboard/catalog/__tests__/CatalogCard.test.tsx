import { StrictMode } from "react";
import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import CatalogCard from "@/components/dashboard/catalog/CatalogCard";

vi.mock("react-i18next", () => ({
  useTranslation: () => ({ t: (key: string) => key }),
}));
vi.mock("@/hooks/catalog/useCatalogLabels", () => ({
  useCatalogLabels: () => ({
    kindLabel: (value: string) => value ?? "",
    languageLabel: (value: string) => value ?? "",
    licenseLabel: (value: string) => value ?? "",
    formatPeriod: () => "",
  }),
}));
vi.mock("@/components/dashboard/catalog/CatalogBundleMembers", () => ({
  default: () => null,
}));
vi.mock("@/components/dashboard/common/ContentThumbnail", () => ({ default: () => null }));

/** The strip only renders for a bundle: `kind` decides, `bundleId` lists it. */
const bundleCard = {
  href: "/catalog/dataset-1",
  title: "Sanitäre Anlagen Basel-Stadt",
  kind: "bundle",
  memberCount: 3,
  bundleId: "dataset-1",
} as never;

describe("CatalogCard", () => {
  it("reports an expand exactly once, from the click and not from a render", () => {
    /**
     * The notification used to be sent from inside the `setExpanded` updater.
     * React may run an updater during the render phase — and runs it twice
     * under StrictMode — so the parent's `setArmed` landed mid-render:
     * "Cannot update a component (CatalogPickerCard) while rendering a
     * different component (CatalogCard)".
     */
    const onExpandedChange = vi.fn();
    render(
      <StrictMode>
        <CatalogCard card={bundleCard} onExpandedChange={onExpandedChange} />
      </StrictMode>
    );

    fireEvent.click(screen.getByText("expand"));

    expect(onExpandedChange).toHaveBeenCalledTimes(1);
    expect(onExpandedChange).toHaveBeenCalledWith(true);
  });

  it("reports the collapse too", () => {
    const onExpandedChange = vi.fn();
    render(<CatalogCard card={bundleCard} onExpandedChange={onExpandedChange} />);

    fireEvent.click(screen.getByText("expand"));
    fireEvent.click(screen.getByText("collapse"));

    expect(onExpandedChange.mock.calls).toEqual([[true], [false]]);
  });
});
