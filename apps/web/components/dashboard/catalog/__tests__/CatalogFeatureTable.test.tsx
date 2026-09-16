import { render, screen } from "@testing-library/react";
import { beforeAll, describe, expect, it } from "vitest";

import type { CatalogColumn } from "@/lib/validations/catalog";

import CatalogFeatureTable from "@/components/dashboard/catalog/CatalogFeatureTable";

beforeAll(() => {
  // The table frame measures itself; jsdom has no ResizeObserver.
  globalThis.ResizeObserver = class {
    observe() {}
    unobserve() {}
    disconnect() {}
  } as unknown as typeof ResizeObserver;
});

const COLUMNS: CatalogColumn[] = [
  { name: "name", type: "string" },
  { name: "value", type: "number" },
] as CatalogColumn[];

const features = (count: number): GeoJSON.Feature<GeoJSON.Geometry | null>[] =>
  Array.from({ length: count }, (_, index) => ({
    type: "Feature",
    geometry: null,
    properties: { name: `row-${index}`, value: index },
  }));

describe("CatalogFeatureTable", () => {
  it("renders every row of a small sample", () => {
    render(<CatalogFeatureTable features={features(12)} columns={COLUMNS} />);
    expect(screen.getByText("row-0")).toBeInTheDocument();
    expect(screen.getByText("row-11")).toBeInTheDocument();
  });

  it("caps the rows it puts in the DOM", () => {
    /* The preview sample is sized for the map, which draws thousands of
       features cheaply. This table does not virtualise -- every row is a
       TableRow of one cell per column -- so handing it the whole sample laid
       out tens of thousands of cells at once and hung the page. */
    render(<CatalogFeatureTable features={features(5000)} columns={COLUMNS} />);

    expect(screen.getByText("row-0")).toBeInTheDocument();
    expect(screen.getByText("row-99")).toBeInTheDocument();
    expect(screen.queryByText("row-100")).not.toBeInTheDocument();
    expect(screen.queryByText("row-4999")).not.toBeInTheDocument();
  });

  it("leaves how much of the dataset this is to the card heading", () => {
    /* The sample size and the dataset size belong side by side. Split across a
       heading and a footer, the reader meets 10,949 above the table and 100
       below it and reads the pair as a contradiction. */
    render(<CatalogFeatureTable features={features(5000)} columns={COLUMNS} />);
    expect(screen.queryByText(/limited|of 5000|begrenzt/i)).not.toBeInTheDocument();
  });
});
