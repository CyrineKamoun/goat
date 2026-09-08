import { fireEvent, render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type { ContentItem, ContentPage, Space } from "@/lib/validations/content";

import RecentDatasets from "@/components/dashboard/home/RecentDatasets";

const { useContentMock, useSpacesMock, pushMock } = vi.hoisted(() => ({
  useContentMock: vi.fn(),
  useSpacesMock: vi.fn(),
  pushMock: vi.fn(),
}));

vi.mock("react-i18next", () => ({
  useTranslation: () => ({ t: (key: string) => key }),
}));
vi.mock("@/i18n/utils", () => ({ useDateFnsLocale: () => undefined }));
vi.mock("@/lib/api/users", () => ({
  useUserProfile: () => ({
    userProfile: { id: "u-me", firstname: "Marco", lastname: "Albrecht", avatar: "" },
  }),
}));
vi.mock("@/lib/api/content", () => ({
  useContent: (...args: unknown[]) => useContentMock(...args),
  useSpaces: () => useSpacesMock(),
}));
vi.mock("next/navigation", () => ({ useRouter: () => ({ push: pushMock }) }));
vi.mock("@/components/dashboard/content/ContentPreviewDialog", () => ({
  default: ({ layerId, onClose }: { layerId: string; onClose: () => void }) => (
    <button type="button" data-testid="preview-dialog" onClick={onClose}>
      preview {layerId}
    </button>
  ),
}));

const space: Space = {
  id: "s1",
  kind: "personal",
  name: "My Content",
  default_role: "viewer",
  my_role: "owner",
  team_id: null,
  organization_id: null,
};

const datasetItem = (overrides: Partial<ContentItem>): ContentItem => ({
  type: "layer",
  id: "l1",
  name: "Bike network",
  space_id: "s1",
  folder_id: null,
  updated_at: new Date().toISOString(),
  created_at: new Date().toISOString(),
  my_role: "owner",
  created_by: null,
  shared_with: null,
  thumbnail_url: null,
  is_public: false,
  layer_type: "feature",
  feature_layer_geometry_type: "line",
  is_shortcut: false,
  restricted: false,
  restricted_inherited: false,
  ...overrides,
});

const page = (items: ContentItem[]): ContentPage => ({ items, total: items.length, page: 1, size: 5 });

beforeEach(() => {
  useContentMock.mockReset();
  useSpacesMock.mockReset();
  pushMock.mockReset();
  useSpacesMock.mockReturnValue({ spaces: [space] });
});

describe("RecentDatasets", () => {
  it("renders the header with row skeletons on first load, no cached page yet", () => {
    useContentMock.mockReturnValue({ page: undefined, isLoading: true });

    const { container } = render(<RecentDatasets />);

    expect(screen.getByText("recent_datasets")).toBeInTheDocument();
    expect(container.querySelectorAll(".MuiSkeleton-root")).toHaveLength(5);
  });

  it("does not flash skeletons on a background refetch once a page is cached", () => {
    useContentMock.mockReturnValue({ page: page([datasetItem({})]), isLoading: true });

    const { container } = render(<RecentDatasets />);

    expect(container.querySelectorAll(".MuiSkeleton-root")).toHaveLength(0);
    expect(screen.getByText("Bike network")).toBeInTheDocument();
  });

  it("renders nothing once loaded when the feed has no datasets", () => {
    useContentMock.mockReturnValue({ page: page([]), isLoading: false });

    const { container } = render(<RecentDatasets />);

    expect(container).toBeEmptyDOMElement();
  });

  it("asks the feed for the five most recent layers and bundles", () => {
    useContentMock.mockReturnValue({ page: page([datasetItem({})]), isLoading: false });

    render(<RecentDatasets />);

    expect(useContentMock).toHaveBeenCalledWith({ view: "recent", types: "layer,bundle", size: 5 });
  });

  it("renders one row per dataset with no select circle", () => {
    const items = [
      datasetItem({ id: "l1", name: "Bike network" }),
      datasetItem({ id: "l2", name: "Bundle A" }),
    ];
    useContentMock.mockReturnValue({ page: page(items), isLoading: false });

    render(<RecentDatasets />);

    expect(screen.getByText("Bike network")).toBeInTheDocument();
    expect(screen.getByText("Bundle A")).toBeInTheDocument();
    expect(screen.queryByRole("checkbox")).not.toBeInTheDocument();
  });

  it("opens a dataset row in the preview dialog rather than leaving Home", async () => {
    const item = datasetItem({ id: "l1", name: "Bike network", space_id: "s1" });
    useContentMock.mockReturnValue({ page: page([item]), isLoading: false });

    render(<RecentDatasets />);
    fireEvent.click(screen.getByText("Bike network"));

    expect(await screen.findByTestId("preview-dialog")).toHaveTextContent("preview l1");
    expect(pushMock).not.toHaveBeenCalled();
  });

  it("sends a row that is not a dataset to its Content location", () => {
    const item = datasetItem({ id: "b1", type: "bundle", name: "Bundle A", space_id: "s1" });
    useContentMock.mockReturnValue({ page: page([item]), isLoading: false });

    render(<RecentDatasets />);
    fireEvent.click(screen.getByText("Bundle A"));

    expect(pushMock).toHaveBeenCalledWith("/content/s1");
  });

  it("routes both open and show_in_content kebab actions into Content", () => {
    const item = datasetItem({ id: "l1", name: "Bike network", space_id: "s1" });
    useContentMock.mockReturnValue({ page: page([item]), isLoading: false });

    render(<RecentDatasets />);
    fireEvent.click(screen.getByRole("button", { name: "more" }));
    fireEvent.click(screen.getByText("show_in_content"));

    expect(pushMock).toHaveBeenCalledWith("/content/s1");
  });

  it("routes all_datasets to the Content page's recent view", () => {
    useContentMock.mockReturnValue({ page: page([datasetItem({})]), isLoading: false });

    render(<RecentDatasets />);
    fireEvent.click(screen.getByText("all_datasets"));

    expect(pushMock).toHaveBeenCalledWith("/content/recent");
  });
});
