import { act, renderHook } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import {
  PICKER_PAGE_SIZE,
  PICKER_TYPES,
  useDatasetPickerState,
} from "@/hooks/addLayer/useDatasetPickerState";

const spaces = [
  { id: "p1", kind: "personal", name: "Me" },
  { id: "t1", kind: "team", name: "Team" },
];
vi.mock("@/lib/api/content", () => ({
  useSpaces: () => ({ spaces, isLoading: false }),
}));
vi.mock("@/hooks/dashboard/home/useDebouncedValue", () => ({
  useDebouncedValue: <T>(value: T) => value,
}));

describe("useDatasetPickerState", () => {
  beforeEach(() => window.localStorage.clear());

  it("starts in the personal space with the picker's fixed types", () => {
    const { result } = renderHook(() => useDatasetPickerState());
    expect(result.current.active).toEqual({ kind: "space", spaceId: "p1" });
    expect(result.current.feedParams).toMatchObject({
      view: "space",
      space_id: "p1",
      types: PICKER_TYPES,
      size: 50,
      order_by: "updated_at",
      order: "descendent",
    });
    expect(result.current.feedParams).not.toHaveProperty("folder_id");
  });

  it("asks for the types the host can use", () => {
    const { result } = renderHook(() => useDatasetPickerState({ types: "folder,layer" }));
    expect(result.current.feedParams).toMatchObject({ types: "folder,layer" });
  });

  it("navigates into a folder and drops it when the space changes", () => {
    const { result } = renderHook(() => useDatasetPickerState());
    act(() => result.current.goFolder("f1"));
    expect(result.current.feedParams).toMatchObject({ folder_id: "f1" });
    act(() => result.current.goSpace("t1"));
    expect(result.current.active).toEqual({ kind: "space", spaceId: "t1" });
    expect(result.current.folderId).toBeNull();
  });

  it("crosses into another space and its folder in one step", () => {
    const { result } = renderHook(() => useDatasetPickerState());
    act(() => result.current.goFolderIn("t1", "f2"));
    expect(result.current.active).toEqual({ kind: "space", spaceId: "t1" });
    expect(result.current.folderId).toBe("f2");
    expect(result.current.feedParams).toMatchObject({
      view: "space",
      space_id: "t1",
      folder_id: "f2",
    });
  });

  it("leaves a cross-space view when a folder in it is entered", () => {
    const { result } = renderHook(() => useDatasetPickerState());
    act(() => result.current.goView("recent"));
    act(() => result.current.goFolderIn("t1", "f2"));
    expect(result.current.feedParams).toMatchObject({ view: "space", space_id: "t1", folder_id: "f2" });
  });

  it("switches to a cross-space view without a space id", () => {
    const { result } = renderHook(() => useDatasetPickerState());
    act(() => result.current.goView("recent"));
    expect(result.current.feedParams).toMatchObject({ view: "recent", types: PICKER_TYPES });
    expect(result.current.feedParams).not.toHaveProperty("space_id");
  });

  it("sends the search only when set, and sorts on request", () => {
    const { result } = renderHook(() => useDatasetPickerState());
    act(() => result.current.setSearch("roads"));
    expect(result.current.feedParams).toMatchObject({ search: "roads" });
    act(() => result.current.setSort("name", "ascendent"));
    expect(result.current.feedParams).toMatchObject({ order_by: "name", order: "ascendent" });
  });

  it("advances the page when more is asked for, at a size the feed accepts", () => {
    const { result } = renderHook(() => useDatasetPickerState());
    expect(result.current.feedParams).toMatchObject({ page: 1, size: PICKER_PAGE_SIZE });

    // The content endpoint caps `size` at 100, so a request that grew the
    // size instead would 422 from the third page on and leave the rest of a
    // large space unreachable.
    for (let click = 1; click <= 5; click += 1) {
      act(() => result.current.loadMore());
      expect(result.current.feedParams).toMatchObject({ page: click + 1, size: PICKER_PAGE_SIZE });
      expect(result.current.feedParams?.size ?? 0).toBeLessThanOrEqual(100);
    }
  });

  it("goes back to the first page when the scope changes underneath it", () => {
    const { result } = renderHook(() => useDatasetPickerState());
    act(() => result.current.loadMore());
    expect(result.current.feedParams).toMatchObject({ page: 2 });
    act(() => result.current.goSpace("t1"));
    expect(result.current.feedParams).toMatchObject({ space_id: "t1", page: 1, size: PICKER_PAGE_SIZE });
  });

  it("goes back to the first page when the sort changes, so pages are never mixed", () => {
    const { result } = renderHook(() => useDatasetPickerState());
    act(() => result.current.loadMore());
    act(() => result.current.loadMore());
    expect(result.current.feedParams).toMatchObject({ page: 3 });
    act(() => result.current.setSort("name", "ascendent"));
    expect(result.current.feedParams).toMatchObject({ order_by: "name", page: 1 });
  });

  it("keys the listed collection on scope and search, not on sort or page", () => {
    const { result } = renderHook(() => useDatasetPickerState());
    const inPersonalSpace = result.current.scopeKey;

    // The same collection, rearranged or extended.
    act(() => result.current.setSort("name", "ascendent"));
    expect(result.current.scopeKey).toBe(inPersonalSpace);
    act(() => result.current.loadMore());
    expect(result.current.scopeKey).toBe(inPersonalSpace);

    // Another collection each time.
    act(() => result.current.goFolder("f1"));
    expect(result.current.scopeKey).not.toBe(inPersonalSpace);
    const inFolder = result.current.scopeKey;
    act(() => result.current.setSearch("roads"));
    expect(result.current.scopeKey).not.toBe(inFolder);
    act(() => result.current.goSpace("t1"));
    act(() => result.current.goView("recent"));
    expect(result.current.scopeKey).toContain("recent");
  });

  it("persists the layout under the Content page's storage key", () => {
    const { result } = renderHook(() => useDatasetPickerState());
    act(() => result.current.setLayout("list"));
    expect(window.localStorage.getItem("goat.content.layout")).toBe("list");
  });
});
