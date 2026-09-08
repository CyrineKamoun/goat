import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import PropertiesPanel from "@/components/map/panels/properties/Properties";

vi.mock("react-i18next", () => ({
  useTranslation: () => ({ t: (key: string) => key }),
}));
vi.mock("@/i18n/utils", () => ({ useDateFnsLocale: () => undefined }));

const CATALOG_ITEM = {
  id: "da6143c8-519a-4f10-8095-b87c8f34ce74",
  publisher: "Landesamt für Umwelt (LfU)",
  updated: "2026-08-16T10:55:42Z",
};

const layer = (other_properties?: unknown) =>
  ({
    id: 1,
    layer_id: "layer-1",
    updated_at: "2026-08-28T18:50:55Z",
    other_properties,
  }) as never;

describe("PropertiesPanel", () => {
  it("says a catalog layer came from the catalog", () => {
    render(<PropertiesPanel activeLayer={layer({ catalog_item: CATALOG_ITEM })} />);

    expect(screen.getByText("source")).toBeDefined();
    expect(screen.getByText("catalog")).toBeDefined();
  });

  it("does not link to the catalog entry", () => {
    // The mirror is rebuilt on every sync, so a promoted layer outlives the
    // entry it came from — a link back would eventually 404.
    render(<PropertiesPanel activeLayer={layer({ catalog_item: CATALOG_ITEM })} />);

    expect(screen.queryByRole("link")).toBeNull();
  });

  it("says nothing about a source for an ordinary layer", () => {
    render(<PropertiesPanel activeLayer={layer({})} />);

    expect(screen.queryByText("source")).toBeNull();
  });

  it("marks a layer whose snapshot carries only the materialize state", () => {
    render(<PropertiesPanel activeLayer={layer({ catalog_materialize: { status: "ready" } })} />);

    expect(screen.getByText("source")).toBeDefined();
  });
});

describe("PropertiesPanel last updated", () => {
  it("shows the provider's date for a catalog layer, not when GOAT copied it", () => {
    render(<PropertiesPanel activeLayer={layer({ catalog_item: CATALOG_ITEM })} />);

    expect(screen.getByText("Aug 16, 2026")).toBeDefined();
  });

  it("omits the row when only the copy's timestamps are known", () => {
    render(<PropertiesPanel activeLayer={layer({ catalog_item: { id: "x" } })} />);

    expect(screen.queryByText("last_updated")).toBeNull();
  });
});
