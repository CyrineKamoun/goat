import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import DatasetSummary from "@/components/dashboard/dataset/DatasetSummary";

vi.mock("react-i18next", () => ({
  useTranslation: () => ({ t: (key: string) => key }),
}));

/**
 * Real text from the catalog: the provider's record indents its second
 * paragraph with a tab, which CommonMark reads as an indented code block. The
 * whole description lands in a `<pre>`, and `<pre>` does not wrap — one
 * 170-character line pushed the metadata panel sideways.
 */
const INDENTED_DESCRIPTION =
  "Der Datensatz ist die Datengrundlage der INSPIRE-Dienste:\n\n" +
  "  \tGroßfeuerungsanlagen in Brandenburg - Interoperabler INSPIRE View-Service (WMS-PF-GFA) " +
  "Großfeuerungsanlagen in Brandenburg - Interoperabler INSPIRE Download-Service (WFS-PF-GFA)\n";

const datasetWith = (description: string, catalogItem: Record<string, unknown> = {}) =>
  ({
    id: "layer-1",
    description,
    other_properties: { catalog_item: catalogItem },
  }) as never;

describe("DatasetSummary", () => {
  it("lets a provider's indented paragraph wrap instead of scrolling the panel", () => {
    const { container } = render(
      <DatasetSummary dataset={datasetWith(INDENTED_DESCRIPTION)} hideEmpty hideMainSection />
    );

    const pre = container.querySelector("pre");
    expect(pre, "this description really does become a code block").not.toBeNull();
    expect(getComputedStyle(pre as Element).whiteSpace).toBe("pre-wrap");
  });

  it("breaks a long slash-joined value that has nowhere to wrap", () => {
    render(
      <DatasetSummary
        dataset={datasetWith("", {
          publisher: "Regierung und Verwaltung/Bau- und Verkehrsdepartement/Tiefbauamt/Leitung",
        })}
        hideEmpty
        hideMainSection
      />
    );

    const value = screen.getByText(/Tiefbauamt\/Leitung/);
    expect(getComputedStyle(value).overflowWrap).toBe("anywhere");
  });
});
