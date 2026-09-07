import { render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import AddLayerDialog from "@/components/addLayer/AddLayerDialog";

vi.mock("react-i18next", () => ({ useTranslation: () => ({ t: (key: string) => key }) }));
vi.mock("react-map-gl/maplibre", () => ({ useMap: () => ({ map: undefined }) }));

const controller = { action: { label: "add", disabled: false, run: vi.fn() }, isBusy: false, reset: vi.fn() };
const selection = { selectedItems: [], toggle: vi.fn(), clear: vi.fn() };

vi.mock("@/hooks/addLayer/useDatasetPickerFlow", () => ({
  useDatasetPickerFlow: () => ({ ...controller, selection }),
}));
vi.mock("@/hooks/addLayer/useCatalogFlow", () => ({ useCatalogFlow: () => controller }));
vi.mock("@/hooks/addLayer/useCreateFlow", () => ({ useCreateFlow: () => controller }));
vi.mock("@/hooks/addLayer/useUploadFlow", () => ({ useUploadFlow: () => controller }));

/** The shelf as the frame renders it, reduced to the one prop under test. */
const shelfProps: { fullHeight?: boolean }[] = [];
vi.mock("@/components/addLayer/DatasetPickerBody", () => ({
  default: (props: { fullHeight?: boolean }) => {
    shelfProps.push(props);
    return <div data-testid="shelf">{String(props.fullHeight)}</div>;
  },
}));
vi.mock("@/components/addLayer/CatalogBody", () => ({ default: () => <div /> }));
vi.mock("@/components/addLayer/CreateBody", () => ({ default: () => <div /> }));
vi.mock("@/components/addLayer/UploadBody", () => ({ default: () => <div /> }));

/** jsdom has no matchMedia; the frame full-screens below `sm` and the shelf
 * reads the same split, so one answer drives both. */
const setViewport = (mobile: boolean) => {
  window.matchMedia = ((query: string) =>
    ({
      matches: mobile,
      media: query,
      onchange: null,
      addListener: vi.fn(),
      removeListener: vi.fn(),
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
      dispatchEvent: vi.fn(),
    }) as unknown as MediaQueryList) as typeof window.matchMedia;
};

describe("AddLayerDialog — My datasets", () => {
  beforeEach(() => {
    shelfProps.length = 0;
  });

  it("fills a full-screen dialog with the shelf, so nothing is left empty above the footer", () => {
    setViewport(true);
    render(<AddLayerDialog source="explorer" projectId="p1" onClose={vi.fn()} />);

    expect(screen.getByTestId("shelf")).toBeInTheDocument();
    expect(shelfProps.at(-1)?.fullHeight).toBe(true);
  });

  it("leaves the shelf at its own height on a windowed dialog", () => {
    setViewport(false);
    render(<AddLayerDialog source="explorer" projectId="p1" onClose={vi.fn()} />);

    expect(shelfProps.at(-1)?.fullHeight).toBe(false);
  });
});
