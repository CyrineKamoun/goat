import { act, renderHook } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { ICON_NAME } from "@p4b/ui/components/Icon";

import type { CatalogCollection } from "@/lib/validations/catalog";
import type { ContentItem } from "@/lib/validations/content";

import { type SearchRow, useHomeSearch } from "@/hooks/dashboard/home/useHomeSearch";

const { pushMock } = vi.hoisted(() => ({ pushMock: vi.fn() }));

vi.mock("next/navigation", () => ({ useRouter: () => ({ push: pushMock }) }));
vi.mock("react-i18next", () => ({
  useTranslation: () => ({ t: (key: string) => key }),
}));

const useContentMock = vi.fn();
const useCatalogDatasetsMock = vi.fn();
vi.mock("@/lib/api/content", () => ({ useContent: (...args: unknown[]) => useContentMock(...args) }));
vi.mock("@/lib/api/catalog", () => ({
  useCatalogDatasets: (...args: unknown[]) => useCatalogDatasetsMock(...args),
}));

const item = (overrides: Partial<ContentItem>): ContentItem => ({
  type: "layer",
  id: "item-1",
  name: "Item",
  space_id: "space-1",
  updated_at: "2026-01-01T00:00:00Z",
  created_at: "2026-01-01T00:00:00Z",
  my_role: "owner",
  shared_with: null,
  is_shortcut: false,
  is_public: false,
  restricted: false,
  restricted_inherited: false,
  ...overrides,
});

const collection = (overrides: Partial<CatalogCollection>): CatalogCollection => ({
  type: "Collection",
  id: "coll-1",
  links: [],
  ...overrides,
});

const itemRows = (rows: SearchRow[]) =>
  rows.filter((row): row is Extract<SearchRow, { kind: "item" }> => row.kind === "item");
const headLabels = (rows: SearchRow[]) =>
  rows
    .filter((row): row is Extract<SearchRow, { kind: "head" }> => row.kind === "head")
    .map((row) => row.label);
const noteLabels = (rows: SearchRow[]) =>
  rows
    .filter((row): row is Extract<SearchRow, { kind: "note" }> => row.kind === "note")
    .map((row) => row.label);

describe("useHomeSearch", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    pushMock.mockReset();
    useContentMock.mockReset();
    useCatalogDatasetsMock.mockReset();
    useContentMock.mockReturnValue({ page: undefined, isLoading: false });
    useCatalogDatasetsMock.mockReturnValue({ datasets: [], isLoading: false });
    window.localStorage.clear();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("does not query under 2 characters", () => {
    const { result, rerender } = renderHook(({ raw }) => useHomeSearch(raw), { initialProps: { raw: "b" } });
    act(() => void vi.advanceTimersByTime(200));
    rerender({ raw: "b" });

    expect(useContentMock).toHaveBeenCalledWith(null);
    expect(useCatalogDatasetsMock).toHaveBeenCalledWith(expect.anything(), false);
    expect(itemRows(result.current.rows)).toHaveLength(0);
  });

  it("groups projects, datasets and catalog rows once both sources answer", () => {
    useContentMock.mockReturnValue({
      page: {
        items: [
          item({ type: "project", id: "p1", name: "Bus network" }),
          item({ type: "layer", id: "l1", name: "Bus stops", layer_type: "feature" }),
        ],
        total: 2,
        page: 1,
        size: 8,
      },
      isLoading: false,
    });
    useCatalogDatasetsMock.mockReturnValue({
      datasets: [collection({ id: "c1", title: "Bus routes" })],
      isLoading: false,
    });

    const { result, rerender } = renderHook(({ raw }) => useHomeSearch(raw), {
      initialProps: { raw: "bus" },
    });
    act(() => void vi.advanceTimersByTime(200));
    rerender({ raw: "bus" });

    expect(headLabels(result.current.rows)).toEqual(["projects", "datasets", "catalog"]);
    const rows = itemRows(result.current.rows);
    expect(rows).toHaveLength(3);
    expect(rows[0]).toMatchObject({ id: "p1", label: "Bus network", icon: ICON_NAME.MAP });
    expect(rows[1]).toMatchObject({ id: "l1", label: "Bus stops" });
    expect(rows[2]).toMatchObject({ id: "c1", label: "Bus routes", icon: ICON_NAME.GLOBE });
  });

  it("a leading project: prefix asks the feed only and drops the catalog", () => {
    useContentMock.mockReturnValue({
      page: {
        items: [
          item({ type: "project", id: "p1", name: "Bus network" }),
          item({ type: "layer", id: "l1", name: "Bus stops" }),
        ],
        total: 2,
        page: 1,
        size: 8,
      },
      isLoading: false,
    });
    useCatalogDatasetsMock.mockReturnValue({
      datasets: [collection({ id: "c1", title: "Bus routes" })],
      isLoading: false,
    });

    const { result, rerender } = renderHook(({ raw }) => useHomeSearch(raw), {
      initialProps: { raw: "project: bus" },
    });
    act(() => void vi.advanceTimersByTime(200));
    rerender({ raw: "project: bus" });

    expect(headLabels(result.current.rows)).toEqual(["projects"]);
    expect(useCatalogDatasetsMock).toHaveBeenLastCalledWith({ q: "bus", limit: 5 }, false);
    expect(useContentMock).toHaveBeenLastCalledWith(
      expect.objectContaining({ view: "recent", search: "bus", size: 8 })
    );
  });

  it("a leading catalog: prefix skips the feed entirely", () => {
    useCatalogDatasetsMock.mockReturnValue({
      datasets: [collection({ id: "c1", title: "Bus routes" })],
      isLoading: false,
    });

    const { result, rerender } = renderHook(({ raw }) => useHomeSearch(raw), {
      initialProps: { raw: "catalog: bus" },
    });
    act(() => void vi.advanceTimersByTime(200));
    rerender({ raw: "catalog: bus" });

    expect(headLabels(result.current.rows)).toEqual(["catalog"]);
    expect(useContentMock).toHaveBeenLastCalledWith(null);
    expect(useCatalogDatasetsMock).toHaveBeenLastCalledWith({ q: "bus", limit: 5 }, true);
  });

  it("a leading dataset: prefix keeps the feed but drops the projects group", () => {
    useContentMock.mockReturnValue({
      page: {
        items: [
          item({ type: "project", id: "p1", name: "Bus network" }),
          item({ type: "layer", id: "l1", name: "Bus stops" }),
        ],
        total: 2,
        page: 1,
        size: 8,
      },
      isLoading: false,
    });

    const { result, rerender } = renderHook(({ raw }) => useHomeSearch(raw), {
      initialProps: { raw: "dataset:bus" },
    });
    act(() => void vi.advanceTimersByTime(200));
    rerender({ raw: "dataset:bus" });

    expect(headLabels(result.current.rows)).toEqual(["datasets"]);
    expect(itemRows(result.current.rows)).toHaveLength(1);
  });

  it("a colon further into the query is just text and searches everything", () => {
    const { rerender } = renderHook(({ raw }) => useHomeSearch(raw), {
      initialProps: { raw: "bus: line" },
    });
    act(() => void vi.advanceTimersByTime(200));
    rerender({ raw: "bus: line" });

    expect(useContentMock).toHaveBeenLastCalledWith(
      expect.objectContaining({ search: "bus: line", size: 8 })
    );
    expect(useCatalogDatasetsMock).toHaveBeenLastCalledWith({ q: "bus: line", limit: 5 }, true);
  });

  it("never narrows the feed by type", () => {
    const { rerender } = renderHook(({ raw }) => useHomeSearch(raw), { initialProps: { raw: "bus" } });
    act(() => void vi.advanceTimersByTime(200));
    rerender({ raw: "bus" });

    const params = useContentMock.mock.lastCall?.[0] as Record<string, unknown>;
    expect(params).toMatchObject({ view: "recent", search: "bus", size: 8 });
    expect("types" in params).toBe(false);
  });

  it("a prefix on its own names its group and asks for a query", () => {
    const { result, rerender } = renderHook(({ raw }) => useHomeSearch(raw), {
      initialProps: { raw: "catalog: " },
    });
    act(() => void vi.advanceTimersByTime(200));
    rerender({ raw: "catalog: " });

    expect(headLabels(result.current.rows)).toEqual(["catalog"]);
    expect(noteLabels(result.current.rows)).toEqual(["search_type_to_search_catalog"]);
    expect(itemRows(result.current.rows)).toHaveLength(0);
    expect(useContentMock).toHaveBeenLastCalledWith(null);
    expect(useCatalogDatasetsMock).toHaveBeenLastCalledWith(expect.anything(), false);
  });

  it("a prefix with a single character still counts as prefix-only", () => {
    const { result, rerender } = renderHook(({ raw }) => useHomeSearch(raw), {
      initialProps: { raw: "project:b" },
    });
    act(() => void vi.advanceTimersByTime(200));
    rerender({ raw: "project:b" });

    expect(headLabels(result.current.rows)).toEqual(["projects"]);
    expect(noteLabels(result.current.rows)).toEqual(["search_type_to_search_projects"]);
  });

  it("a prefix on its own drops the recents rather than mixing scopes", () => {
    window.localStorage.setItem(
      "goat.home.recent",
      JSON.stringify([{ id: "p1", label: "Bus network", meta: "project", href: "/map/p1" }])
    );

    const { result, rerender } = renderHook(({ raw }) => useHomeSearch(raw), {
      initialProps: { raw: "dataset:" },
    });
    act(() => void vi.advanceTimersByTime(200));
    rerender({ raw: "dataset:" });

    expect(headLabels(result.current.rows)).toEqual(["datasets"]);
    expect(noteLabels(result.current.rows)).toEqual(["search_type_to_search_datasets"]);
    expect(itemRows(result.current.rows)).toHaveLength(0);
  });

  it("a too-short unscoped query gets no group of its own", () => {
    const { result, rerender } = renderHook(({ raw }) => useHomeSearch(raw), {
      initialProps: { raw: "b" },
    });
    act(() => void vi.advanceTimersByTime(200));
    rerender({ raw: "b" });

    expect(noteLabels(result.current.rows)).toEqual([]);
    expect(headLabels(result.current.rows)).toEqual([]);
  });

  it("empty query with no recents produces no rows at all — not even a header", () => {
    const { result } = renderHook(({ raw }) => useHomeSearch(raw), { initialProps: { raw: "" } });
    act(() => void vi.advanceTimersByTime(200));

    expect(result.current.rows).toHaveLength(0);
  });

  it("empty query shows the recent group once there is something in it", () => {
    window.localStorage.setItem(
      "goat.home.recent",
      JSON.stringify([{ id: "p1", label: "Bus network", meta: "project", href: "/map/p1" }])
    );

    const { result } = renderHook(({ raw }) => useHomeSearch(raw), { initialProps: { raw: "" } });
    act(() => void vi.advanceTimersByTime(200));

    expect(headLabels(result.current.rows)).toEqual(["recent"]);
    expect(itemRows(result.current.rows)).toMatchObject([{ id: "p1", label: "Bus network" }]);
  });

  it("drops a recent entry stored under the removed dataset route", () => {
    window.localStorage.setItem(
      "goat.home.recent",
      JSON.stringify([
        { id: "l1", label: "Bus stops", meta: "feature_layer", href: "/datasets/l1" },
        { id: "p1", label: "Bus network", meta: "project", href: "/map/p1" },
      ])
    );

    const { result } = renderHook(({ raw }) => useHomeSearch(raw), { initialProps: { raw: "" } });
    act(() => void vi.advanceTimersByTime(200));

    expect(itemRows(result.current.rows)).toMatchObject([{ id: "p1" }]);
  });

  it("a layer row opens the preview in place instead of navigating", () => {
    useContentMock.mockReturnValue({
      page: { items: [item({ type: "layer", id: "l1", name: "Bus stops" })], total: 1, page: 1, size: 8 },
      isLoading: false,
    });

    const { result, rerender } = renderHook(({ raw }) => useHomeSearch(raw), {
      initialProps: { raw: "bus" },
    });
    act(() => void vi.advanceTimersByTime(200));
    rerender({ raw: "bus" });

    const [row] = itemRows(result.current.rows);
    act(() => row.go());

    expect(pushMock).not.toHaveBeenCalled();
    expect(result.current.previewLayerId).toBe("l1");
    act(() => result.current.closePreview());
    expect(result.current.previewLayerId).toBeNull();
  });

  it("a bundle row opens the bundle page and a catalog row its detail page", () => {
    useContentMock.mockReturnValue({
      page: { items: [item({ type: "bundle", id: "b1", name: "Bus GTFS" })], total: 1, page: 1, size: 8 },
      isLoading: false,
    });
    useCatalogDatasetsMock.mockReturnValue({
      datasets: [collection({ id: "c1", title: "Bus routes" })],
      isLoading: false,
    });

    const { result, rerender } = renderHook(({ raw }) => useHomeSearch(raw), {
      initialProps: { raw: "bus" },
    });
    act(() => void vi.advanceTimersByTime(200));
    rerender({ raw: "bus" });

    const [bundle, catalog] = itemRows(result.current.rows);
    act(() => bundle.go());
    expect(pushMock).toHaveBeenLastCalledWith("/bundles/b1");
    act(() => catalog.go());
    expect(pushMock).toHaveBeenLastCalledWith("/catalog/c1");
  });

  it("go() pushes the route and records the entry in goat.home.recent", () => {
    useContentMock.mockReturnValue({
      page: { items: [item({ type: "project", id: "p1", name: "Bus network" })], total: 1, page: 1, size: 8 },
      isLoading: false,
    });

    const { result, rerender } = renderHook(({ raw }) => useHomeSearch(raw), {
      initialProps: { raw: "bus" },
    });
    act(() => void vi.advanceTimersByTime(200));
    rerender({ raw: "bus" });

    const [row] = itemRows(result.current.rows);
    act(() => row.go());

    expect(pushMock).toHaveBeenCalledWith("/map/p1");
    const stored = JSON.parse(window.localStorage.getItem("goat.home.recent") ?? "[]");
    expect(stored[0]).toMatchObject({ id: "p1", label: "Bus network", href: "/map/p1" });
  });
});
