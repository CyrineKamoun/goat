import { render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import DatasetTableModal from "@/components/modals/DatasetTable";

const { useFeaturePageMock } = vi.hoisted(() => ({ useFeaturePageMock: vi.fn() }));

vi.mock("react-i18next", () => ({ useTranslation: () => ({ t: (key: string) => key }) }));
vi.mock("@/hooks/useFeaturePage", () => ({ useFeaturePage: useFeaturePageMock }));
vi.mock("@/components/common/FeatureTable", () => ({
  default: () => <div data-testid="feature-table" />,
}));

// eslint-disable-next-line @typescript-eslint/no-explicit-any
const dataset = { id: "layer-1", name: "Roads" } as any;

const featurePage = (data: unknown) => ({
  fields: [],
  areFieldsLoading: false,
  data,
  rowsPerPage: 50,
  page: 0,
  totalCount: 120,
  onPageChange: vi.fn(),
  onRowsPerPageChange: vi.fn(),
});

describe("DatasetTable", () => {
  beforeEach(() => {
    useFeaturePageMock.mockReset();
  });

  it("titles the dialog with the dataset and shows its table behind a close action", () => {
    useFeaturePageMock.mockReturnValue(featurePage({ features: [] }));

    render(<DatasetTableModal open dataset={dataset} onClose={() => {}} />);

    expect(screen.getByText("Roads")).toBeInTheDocument();
    expect(screen.getByTestId("feature-table")).toBeInTheDocument();
    // The header X carries the same label, so the way out is read off the
    // action row itself.
    expect(document.querySelector(".MuiDialogActions-root")?.textContent).toContain("close");
  });

  it("pages the rows only once there are rows to page", () => {
    useFeaturePageMock.mockReturnValue(featurePage(undefined));

    render(<DatasetTableModal open dataset={dataset} onClose={() => {}} />);

    expect(screen.queryByText("Rows per page:")).not.toBeInTheDocument();
  });
});
