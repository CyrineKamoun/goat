import { act, renderHook } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import {
  LAYOUT_STORAGE_KEY,
  contentPath,
  useContentPageState,
} from "@/hooks/dashboard/content/useContentPageState";

/**
 * The browsed location comes from the route (the hook's own argument, which
 * the route segment fills in); the searched-for state lives in the query, so
 * `useSearchParams` is the one thing mocked as URL state. Location changes
 * `push` a path; query changes write the current one with the native
 * `history.replaceState`, which is what the spy below records. Grid-vs-list is
 * in neither — it is read from and written to `localStorage`.
 */
let currentParams = new URLSearchParams();
const push = vi.fn();
const replace = vi.fn();

vi.mock("next/navigation", () => ({
  useRouter: () => ({ push, replace }),
  useSearchParams: () => currentParams,
}));

const replaceState = vi.spyOn(window.history, "replaceState");

/** The URL the hook reads when it strips a stale `?layout=`. */
const setLocation = (url: string) => window.history.replaceState(null, "", url);

describe("contentPath", () => {
  it("addresses spaces, folders and views", () => {
    expect(contentPath({})).toBe("/content");
    expect(contentPath({ spaceId: "s1" })).toBe("/content/s1");
    expect(contentPath({ spaceId: "s1", folderId: "f1" })).toBe("/content/s1/f1");
    expect(contentPath({ view: "recent" })).toBe("/content/recent");
    expect(contentPath({ view: "shared_with_me" })).toBe("/content/shared");
  });

  it("lets the view win over a space, so neither reserved segment can be a space id", () => {
    expect(contentPath({ spaceId: "s1", folderId: "f1", view: "recent" })).toBe("/content/recent");
  });
});

describe("useContentPageState", () => {
  beforeEach(() => {
    setLocation("/content");
    push.mockClear();
    replace.mockClear();
    replaceState.mockClear();
    currentParams = new URLSearchParams();
    window.localStorage.clear();
  });

  it("defaults to the (unresolved) personal space scope with no feed params", () => {
    const { result } = renderHook(() => useContentPageState());
    expect(result.current.active).toEqual({ kind: "space", spaceId: null });
    expect(result.current.feedParams).toBeNull();
  });

  it("reads the space and folder from the route", () => {
    const { result } = renderHook(() => useContentPageState({ spaceId: "s1", folderId: "f1" }));
    expect(result.current.active).toEqual({ kind: "space", spaceId: "s1" });
    expect(result.current.folderId).toBe("f1");
    expect(result.current.feedParams).toEqual({
      view: "space",
      space_id: "s1",
      folder_id: "f1",
      size: 50,
      order_by: "updated_at",
      order: "descendent",
    });
  });

  it("goSpace pushes the space's path, leaving the folder behind", () => {
    const { result } = renderHook(() => useContentPageState({ spaceId: "s0", folderId: "f1" }));

    act(() => result.current.goSpace("s1"));

    expect(push).toHaveBeenCalledWith("/content/s1", { scroll: false });
  });

  it("goFolder pushes the folder's path under the space in force", () => {
    const { result } = renderHook(() => useContentPageState({ spaceId: "s1" }));

    act(() => result.current.goFolder("f1"));
    expect(push).toHaveBeenCalledWith("/content/s1/f1", { scroll: false });

    act(() => result.current.goFolder(null));
    expect(push).toHaveBeenLastCalledWith("/content/s1", { scroll: false });
  });

  it("goFolderIn crosses into another space in a single navigation", () => {
    const { result } = renderHook(() => useContentPageState({ view: "recent" }));

    act(() => result.current.goFolderIn("s2", "f9"));

    expect(push).toHaveBeenCalledTimes(1);
    expect(push).toHaveBeenCalledWith("/content/s2/f9", { scroll: false });
  });

  it("goView pushes the view's own path and computes its feedParams", () => {
    const { result } = renderHook(() => useContentPageState({ spaceId: "s1", folderId: "f1" }));

    act(() => result.current.goView("recent"));
    expect(push).toHaveBeenCalledWith("/content/recent", { scroll: false });

    const view = renderHook(() => useContentPageState({ view: "recent" }));
    expect(view.result.current.active).toEqual({ kind: "view", view: "recent" });
    expect(view.result.current.feedParams).toEqual({
      view: "recent",
      size: 50,
      order_by: "updated_at",
      order: "descendent",
    });
  });

  it("carries the current query along when the location changes", () => {
    currentParams = new URLSearchParams("q=bus&types=project");
    const { result } = renderHook(() => useContentPageState({ spaceId: "s1" }));

    act(() => result.current.goFolder("f1"));

    expect(push).toHaveBeenCalledWith("/content/s1/f1?q=bus&types=project", { scroll: false });
  });

  it("reads q, types and sort back off the URL on mount", () => {
    currentParams = new URLSearchParams("q=roads&types=layer&sort=name.asc");
    const { result } = renderHook(() => useContentPageState({ spaceId: "s1" }));

    expect(result.current.search).toBe("roads");
    expect(result.current.types).toEqual(["layer"]);
    expect(result.current.orderBy).toBe("name");
    expect(result.current.order).toBe("ascendent");
    expect(result.current.feedParams).toEqual({
      view: "space",
      space_id: "s1",
      size: 50,
      order_by: "name",
      order: "ascendent",
      search: "roads",
      types: "layer",
    });
  });

  it("setTypes narrows the query and the feed params without navigating", () => {
    const { result, rerender } = renderHook(() => useContentPageState({ spaceId: "s1" }));

    act(() => result.current.setTypes(["project"]));
    expect(replaceState).toHaveBeenCalledWith(null, "", "/content/s1?types=project");
    expect(push).not.toHaveBeenCalled();
    expect(replace).not.toHaveBeenCalled();

    currentParams = new URLSearchParams("types=project");
    rerender();

    expect(result.current.types).toEqual(["project"]);
    expect(result.current.feedParams?.types).toBe("project");
  });

  it("keeps an empty type selection in the URL, and reads it back as itself", () => {
    const { result, rerender } = renderHook(() => useContentPageState({ spaceId: "s1" }));

    act(() => result.current.setTypes([]));
    // An empty value would be dropped from the query, and an absent `types`
    // means every type — so the checkboxes would come back all ticked and the
    // feed would list everything.
    expect(replaceState).toHaveBeenCalledWith(null, "", "/content/s1?types=none");

    currentParams = new URLSearchParams("types=none");
    rerender();

    expect(result.current.types).toEqual([]);
    // Nothing can match, so the feed is not asked: the endpoint reads an
    // empty `types` as no filter at all.
    expect(result.current.feedParams).toBeNull();
  });

  it("drops the key again when every type is ticked back on", () => {
    currentParams = new URLSearchParams("types=none");
    const { result, rerender } = renderHook(() => useContentPageState({ spaceId: "s1" }));

    act(() => result.current.setTypes(result.current.ALL_TYPES));
    expect(replaceState).toHaveBeenLastCalledWith(null, "", "/content/s1");

    currentParams = new URLSearchParams();
    rerender();

    expect(result.current.types).toEqual(["folder", "project", "layer", "bundle", "template"]);
    expect(result.current.feedParams?.types).toBeUndefined();
  });

  it("setSort writes sort=by.dir on the current path without navigating", () => {
    const { result } = renderHook(() => useContentPageState({ spaceId: "s1", folderId: "f1" }));

    act(() => result.current.setSort("name", "ascendent"));
    expect(replaceState).toHaveBeenCalledWith(null, "", "/content/s1/f1?sort=name.asc");
    expect(replace).not.toHaveBeenCalled();
  });

  it("setSearch writes q, and clears it again from the URL", () => {
    const { result, rerender } = renderHook(() => useContentPageState({ view: "recent" }));

    act(() => result.current.setSearch("bus"));
    expect(replaceState).toHaveBeenLastCalledWith(null, "", "/content/recent?q=bus");

    currentParams = new URLSearchParams("q=bus");
    rerender();

    act(() => result.current.setSearch(""));
    expect(replaceState).toHaveBeenLastCalledWith(null, "", "/content/recent");
  });

  it("defaults to grid and remembers the layout across a remount", () => {
    const first = renderHook(() => useContentPageState({ spaceId: "s1" }));
    expect(first.result.current.layout).toBe("grid");

    act(() => first.result.current.setLayout("list"));
    expect(first.result.current.layout).toBe("list");
    expect(window.localStorage.getItem(LAYOUT_STORAGE_KEY)).toBe("list");
    first.unmount();

    const second = renderHook(() => useContentPageState({ spaceId: "s1" }));
    expect(second.result.current.layout).toBe("list");
  });

  it("keeps the layout out of the URL", () => {
    const { result } = renderHook(() => useContentPageState({ spaceId: "s1" }));

    act(() => result.current.setLayout("list"));

    expect(replaceState).not.toHaveBeenCalled();
    expect(push).not.toHaveBeenCalled();
    expect(replace).not.toHaveBeenCalled();
  });

  it("falls back to grid when storage holds something else, and when it throws", () => {
    window.localStorage.setItem(LAYOUT_STORAGE_KEY, "{corrupt");
    expect(renderHook(() => useContentPageState({ spaceId: "s1" })).result.current.layout).toBe("grid");

    const getItem = vi.spyOn(window.localStorage, "getItem").mockImplementation(() => {
      throw new Error("storage disabled");
    });
    const setItem = vi.spyOn(window.localStorage, "setItem").mockImplementation(() => {
      throw new Error("storage disabled");
    });

    const blocked = renderHook(() => useContentPageState({ spaceId: "s1" }));
    expect(blocked.result.current.layout).toBe("grid");
    // A blocked write must not throw out of the setter either — the choice
    // just holds for this page only.
    act(() => blocked.result.current.setLayout("list"));
    expect(blocked.result.current.layout).toBe("list");

    getItem.mockRestore();
    setItem.mockRestore();
  });

  it("strips a stale ?layout= off the incoming URL without navigating", () => {
    setLocation("/content/s1?q=bus&layout=list");
    replaceState.mockClear();
    currentParams = new URLSearchParams("q=bus&layout=list");

    renderHook(() => useContentPageState({ spaceId: "s1" }));

    expect(replaceState).toHaveBeenCalledWith(null, "", "/content/s1?q=bus");
    expect(replace).not.toHaveBeenCalled();
    expect(push).not.toHaveBeenCalled();
  });

  it("leaves a layout-free URL alone", () => {
    setLocation("/content/s1?q=bus");
    replaceState.mockClear();
    currentParams = new URLSearchParams("q=bus");

    renderHook(() => useContentPageState({ spaceId: "s1" }));

    expect(replaceState).not.toHaveBeenCalled();
  });
});
