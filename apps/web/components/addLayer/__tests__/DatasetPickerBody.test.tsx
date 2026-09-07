import { fireEvent, render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type { ActiveScope } from "@/hooks/dashboard/content/useContentPageState";

import DatasetPickerBody from "@/components/addLayer/DatasetPickerBody";

vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    t: (key: string, opts?: { count?: number }) => (opts?.count !== undefined ? `${key}:${opts.count}` : key),
  }),
  Trans: ({ i18nKey }: { i18nKey: string }) => <span>{i18nKey}</span>,
}));

const spaces = [{ id: "p1", kind: "personal", name: "Me" }];
const items = [
  {
    type: "folder",
    id: "f1",
    name: "Surveys",
    space_id: "p1",
    folder_id: null,
    updated_at: "2026-01-01",
    is_shortcut: false,
  },
  {
    type: "layer",
    id: "l1",
    name: "roads",
    space_id: "p1",
    folder_id: null,
    updated_at: "2026-01-01",
    is_shortcut: false,
    layer_type: "feature",
    feature_layer_geometry_type: "line",
  },
  {
    type: "bundle",
    id: "b1",
    name: "Network",
    space_id: "p1",
    folder_id: null,
    updated_at: "2026-01-01",
    is_shortcut: false,
  },
];
/** What the picker asks the feed for: one fixed-size page at a time, which
 * "Load more" advances. */
const BASE_FEED_PARAMS: Record<string, unknown> = {
  view: "space",
  space_id: "p1",
  types: "folder,layer,bundle",
  page: 1,
  size: 50,
};
const state = {
  active: { kind: "space", spaceId: "p1" } as ActiveScope,
  folderId: null,
  goSpace: vi.fn(),
  goFolder: vi.fn(),
  goFolderIn: vi.fn(),
  goView: vi.fn(),
  search: "",
  setSearch: vi.fn(),
  orderBy: "updated_at",
  order: "descendent",
  setSort: vi.fn(),
  layout: "grid",
  setLayout: vi.fn(),
  spaces,
  spacesLoading: false,
  feedParams: { ...BASE_FEED_PARAMS },
  loadMore: vi.fn(),
  scopeKey: '{"space":"p1","folder":null}',
};
const useContentMock = vi.fn();
const pickerStateMock = vi.fn();
vi.mock("@/hooks/addLayer/useDatasetPickerState", () => ({
  // The feed params carry the types the body asked for, so a mode that
  // narrows them is visible to `useContent` here as it is in the app.
  useDatasetPickerState: (options?: { types?: string }) => {
    pickerStateMock(options);
    return {
      ...state,
      feedParams: { ...state.feedParams, types: options?.types ?? "folder,layer,bundle" },
    };
  },
  PICKER_TYPES: "folder,layer,bundle",
}));
vi.mock("@/lib/api/content", () => ({
  useContent: (params: unknown) => useContentMock(params),
  useSpaces: () => ({ spaces, isLoading: false }),
}));
vi.mock("@/lib/api/folders", () => ({ useFolders: () => ({ folders: [] }) }));
vi.mock("@/lib/api/users", () => ({ useUserProfile: () => ({ userProfile: undefined }) }));
vi.mock("@/components/dashboard/content/ContentSpacesPanel", () => ({
  default: (props: { onSelectSpace: (id: string) => void }) => (
    <button onClick={() => props.onSelectSpace("p1")}>spaces-panel</button>
  ),
}));

describe("DatasetPickerBody", () => {
  beforeEach(() => {
    useContentMock.mockReset();
    // The feed answers with the types it was asked for, as the API does.
    useContentMock.mockImplementation((params?: { types?: string }) => {
      const listed = items.filter((item) => (params?.types ?? "").split(",").includes(item.type));
      return { page: { items: listed, total: listed.length }, isLoading: false };
    });
    pickerStateMock.mockReset();
    state.goFolder.mockReset();
    state.goFolderIn.mockReset();
    state.active = { kind: "space", spaceId: "p1" };
    state.layout = "grid";
    state.scopeKey = '{"space":"p1","folder":null}';
    state.feedParams = { ...BASE_FEED_PARAMS };
    state.loadMore.mockReset();
  });

  it("asks the feed for folders, datasets and bundles only", () => {
    render(
      <DatasetPickerBody mode="add" selection={{ ids: [], items: [], toggle: vi.fn(), clear: vi.fn() }} />
    );
    expect(useContentMock).toHaveBeenCalledWith(expect.objectContaining({ types: "folder,layer,bundle" }));
  });

  it("navigates into a folder and toggles a dataset in add mode", () => {
    const toggle = vi.fn();
    render(<DatasetPickerBody mode="add" selection={{ ids: [], items: [], toggle, clear: vi.fn() }} />);
    fireEvent.click(screen.getByText("Surveys"));
    // The folder's own space, so a shortcut lands where the folder lives.
    expect(state.goFolderIn).toHaveBeenCalledWith("p1", "f1");
    fireEvent.click(screen.getByText("roads"));
    // The item itself, so the host can hold on to it after the page changes.
    expect(toggle).toHaveBeenCalledWith(items[1]);
  });

  it("gives the datasets a select circle and the folders none", () => {
    render(
      <DatasetPickerBody mode="add" selection={{ ids: [], items: [], toggle: vi.fn(), clear: vi.fn() }} />
    );
    // One per dataset and bundle; the folder tile carries none.
    expect(screen.getAllByRole("checkbox")).toHaveLength(2);
  });

  it("renders folders as rows in list layout, still only navigable", () => {
    state.layout = "list";
    const toggle = vi.fn();
    render(<DatasetPickerBody mode="add" selection={{ ids: [], items: [], toggle, clear: vi.fn() }} />);

    // A row layout throughout — no folder grid above a dataset list.
    expect(screen.getAllByRole("checkbox")).toHaveLength(2);
    fireEvent.click(screen.getByText("Surveys"));
    expect(state.goFolderIn).toHaveBeenCalledWith("p1", "f1");
    fireEvent.click(screen.getByText("roads"));
    expect(toggle).toHaveBeenCalledWith(items[1]);
  });

  it("renders a cross-space view as one flat list", () => {
    state.active = { kind: "view", view: "recent" };
    render(
      <DatasetPickerBody mode="add" selection={{ ids: [], items: [], toggle: vi.fn(), clear: vi.fn() }} />
    );
    expect(screen.queryByText("folders")).not.toBeInTheDocument();
    expect(screen.queryByText("datasets")).not.toBeInTheDocument();
    expect(screen.getByText("Surveys")).toBeInTheDocument();
    expect(screen.getByText("roads")).toBeInTheDocument();
  });

  it("shows the count and a clear control once something is selected", () => {
    const clear = vi.fn();
    render(
      <DatasetPickerBody
        mode="add"
        selection={{ ids: ["l1", "b1"], items: [items[1], items[2]] as never, toggle: vi.fn(), clear }}
      />
    );
    // Two of the three listed items are datasets; the folder is not one.
    expect(screen.getByText(/n_datasets:2/)).toBeInTheDocument();
    expect(screen.getByText(/catalog_n_selected:2/)).toBeInTheDocument();
    fireEvent.click(screen.getByText("catalog_clear_selection"));
    expect(clear).toHaveBeenCalled();
  });

  it("renders no kebab and no add-new control", () => {
    render(
      <DatasetPickerBody mode="add" selection={{ ids: [], items: [], toggle: vi.fn(), clear: vi.fn() }} />
    );
    expect(screen.queryByRole("button", { name: /more|actions|add_new/i })).not.toBeInTheDocument();
  });

  it("picks exactly one item in pick mode", () => {
    const rails = { ...items[1], id: "l2", name: "rails" };
    useContentMock.mockReturnValue({
      page: { items: [items[0], items[1], rails], total: 3 },
      isLoading: false,
    });
    const onPickedChange = vi.fn();
    render(<DatasetPickerBody mode="pick" picked={items[1] as never} onPickedChange={onPickedChange} />);
    fireEvent.click(screen.getByText("rails"));
    expect(onPickedChange).toHaveBeenCalledWith(rails);
    fireEvent.click(screen.getByText("roads"));
    // Clicking the picked item again unpicks it.
    expect(onPickedChange).toHaveBeenCalledWith(null);
  });

  it("offers no bundle in pick mode", () => {
    render(<DatasetPickerBody mode="pick" picked={null} onPickedChange={vi.fn()} />);
    // Both single-pick hosts take a layer, so the shelf never lists a bundle
    // for them to pick.
    expect(pickerStateMock).toHaveBeenCalledWith({ types: "folder,layer" });
    expect(useContentMock).toHaveBeenCalledWith(expect.objectContaining({ types: "folder,layer" }));
    expect(screen.getByText("roads")).toBeInTheDocument();
    expect(screen.queryByText("Network")).not.toBeInTheDocument();
  });

  it("offers the rest of the feed only when the page is not all of it", () => {
    const { rerender } = render(
      <DatasetPickerBody mode="add" selection={{ ids: [], items: [], toggle: vi.fn(), clear: vi.fn() }} />
    );
    // The page holds every item there is.
    expect(screen.queryByText("load_more")).not.toBeInTheDocument();

    useContentMock.mockReturnValue({ page: { items, total: 9 }, isLoading: false });
    rerender(
      <DatasetPickerBody mode="add" selection={{ ids: [], items: [], toggle: vi.fn(), clear: vi.fn() }} />
    );
    fireEvent.click(screen.getByText("load_more"));
    expect(state.loadMore).toHaveBeenCalled();
  });

  it("keeps the pages already listed as further ones are asked for", () => {
    const pageOf = (number: number) => [{ ...items[1], id: `l${number}`, name: `roads-${number}` }];
    useContentMock.mockImplementation((params: { page?: number }) => ({
      page: { items: pageOf(params.page ?? 1), total: 150, page: params.page ?? 1, size: 50 },
      isLoading: false,
    }));
    const selection = { ids: [], items: [], toggle: vi.fn(), clear: vi.fn() };
    const { rerender } = render(<DatasetPickerBody mode="add" selection={selection} />);
    expect(screen.getByText("roads-1")).toBeInTheDocument();

    // "Load more" advances the page rather than growing the size, so the
    // items already listed are kept here instead of being re-read.
    for (const page of [2, 3]) {
      state.feedParams = { ...BASE_FEED_PARAMS, page };
      rerender(<DatasetPickerBody mode="add" selection={selection} />);
    }

    expect(screen.getByText("roads-1")).toBeInTheDocument();
    expect(screen.getByText("roads-2")).toBeInTheDocument();
    // The third page is where a growing `size` would have hit the feed's
    // cap of 100 and 422'd, leaving the rest of the space unreachable.
    expect(screen.getByText("roads-3")).toBeInTheDocument();
    for (const [params] of useContentMock.mock.calls) {
      expect((params as { size: number }).size).toBeLessThanOrEqual(100);
    }
  });

  it("drops the pages of the collection just left", () => {
    const selection = { ids: [], items: [], toggle: vi.fn(), clear: vi.fn() };
    const { rerender } = render(<DatasetPickerBody mode="add" selection={selection} />);
    expect(screen.getByText("roads")).toBeInTheDocument();

    // A new sort re-reads the same collection from its first page, so the
    // pages loaded under the old order are not kept alongside it.
    state.feedParams = { ...BASE_FEED_PARAMS, order_by: "name" };
    useContentMock.mockReturnValue({
      page: { items: [items[0]], total: 1, page: 1, size: 50 },
      isLoading: false,
    });
    rerender(<DatasetPickerBody mode="add" selection={selection} />);

    expect(screen.queryByText("roads")).not.toBeInTheDocument();
    expect(screen.getByText("Surveys")).toBeInTheDocument();
  });

  it("keeps the loaded page on screen while the next one is fetched", () => {
    const selection = { ids: [], items: [], toggle: vi.fn(), clear: vi.fn() };
    const { container, rerender } = render(<DatasetPickerBody mode="add" selection={selection} />);
    expect(screen.getByText("roads")).toBeInTheDocument();

    // "Load more" and a sort change both re-key the feed, so SWR reports no
    // data while the bigger page is in flight.
    useContentMock.mockReturnValue({ page: undefined, isLoading: true });
    rerender(<DatasetPickerBody mode="add" selection={selection} />);
    expect(screen.getByText("roads")).toBeInTheDocument();
    expect(container.querySelectorAll(".MuiSkeleton-root")).toHaveLength(0);
  });

  it("drops the held page when another collection is listed", () => {
    const selection = { ids: [], items: [], toggle: vi.fn(), clear: vi.fn() };
    const { container, rerender } = render(<DatasetPickerBody mode="add" selection={selection} />);
    expect(screen.getByText("roads")).toBeInTheDocument();

    // Another space: the page in hand belongs to the one just left, so it is
    // not shown while the new one loads.
    state.active = { kind: "space", spaceId: "t1" };
    state.scopeKey = '{"space":"t1","folder":null}';
    useContentMock.mockReturnValue({ page: undefined, isLoading: true });
    rerender(<DatasetPickerBody mode="add" selection={selection} />);
    expect(screen.queryByText("roads")).not.toBeInTheDocument();
    expect(container.querySelectorAll(".MuiSkeleton-root").length).toBeGreaterThan(0);
  });

  it("waits for a page rather than claiming the space is empty", () => {
    // `feedParams` is null until the personal space is known, so SWR reports
    // neither data nor loading.
    useContentMock.mockReturnValue({ page: undefined, isLoading: false });
    const { container } = render(
      <DatasetPickerBody mode="add" selection={{ ids: [], items: [], toggle: vi.fn(), clear: vi.fn() }} />
    );
    expect(container.querySelectorAll(".MuiSkeleton-root").length).toBeGreaterThan(0);
    expect(screen.queryByText("nothing_here_yet")).not.toBeInTheDocument();
  });

  it("shows the feed skeleton while loading and the empty state when nothing is there", () => {
    useContentMock.mockReturnValue({ page: undefined, isLoading: true });
    const { container, rerender } = render(
      <DatasetPickerBody mode="add" selection={{ ids: [], items: [], toggle: vi.fn(), clear: vi.fn() }} />
    );
    expect(container.querySelectorAll(".MuiSkeleton-root").length).toBeGreaterThan(0);
    useContentMock.mockReturnValue({ page: { items: [], total: 0 }, isLoading: false });
    rerender(
      <DatasetPickerBody mode="add" selection={{ ids: [], items: [], toggle: vi.fn(), clear: vi.fn() }} />
    );
    expect(screen.getByText("nothing_here_yet")).toBeInTheDocument();
  });
});
