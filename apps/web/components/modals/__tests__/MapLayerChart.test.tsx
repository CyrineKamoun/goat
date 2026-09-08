import { render, screen, within } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import MapLayerChartModal from "@/components/modals/MapLayerChart";

const { useProjectLayerChartDataMock, useLayerFieldsMock } = vi.hoisted(() => ({
  useProjectLayerChartDataMock: vi.fn(),
  useLayerFieldsMock: vi.fn(),
}));

vi.mock("react-i18next", () => ({ useTranslation: () => ({ t: (key: string) => key }) }));
vi.mock("next/navigation", () => ({ useParams: () => ({ lng: "en" }) }));
vi.mock("@/lib/api/projects", () => ({ useProjectLayerChartData: useProjectLayerChartDataMock }));
vi.mock("@/hooks/map/CommonHooks", () => ({ default: useLayerFieldsMock }));
vi.mock("@/components/common/PlotlyPlot", () => ({ Plot: () => <div data-testid="plot" /> }));
vi.mock("@/components/map/panels/common/Selector", () => ({ default: () => null }));

// eslint-disable-next-line @typescript-eslint/no-explicit-any
const layer = { id: "project-layer-1", layer_id: "layer-1", name: "Roads", charts: null } as any;

describe("MapLayerChart", () => {
  beforeEach(() => {
    useLayerFieldsMock.mockReturnValue({ layerFields: [], isLoading: false });
    useProjectLayerChartDataMock.mockReturnValue({
      chartData: undefined,
      isLoading: false,
      isError: false,
    });
  });

  it("titles the dialog with the layer and offers the way out", () => {
    render(<MapLayerChartModal open layer={layer} projectId="project-1" onClose={() => {}} />);

    expect(screen.getByText("Roads - chart")).toBeInTheDocument();
    const actions = document.querySelector(".MuiDialogActions-root") as HTMLElement;
    expect(within(actions).getByRole("button", { name: "close" })).toBeInTheDocument();
  });

  it("says so when the layer has no chart data", () => {
    render(<MapLayerChartModal open layer={layer} projectId="project-1" onClose={() => {}} />);

    expect(screen.getByText("no_chart_data")).toBeInTheDocument();
    expect(screen.queryByTestId("plot")).not.toBeInTheDocument();
  });
});
