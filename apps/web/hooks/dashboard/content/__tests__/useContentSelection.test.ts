import { renderHook } from "@testing-library/react";
import { act } from "react";
import { describe, expect, it } from "vitest";

import type { ContentItem } from "@/lib/validations/content";

import { useContentSelection } from "@/hooks/dashboard/content/useContentSelection";

const item = (id: string, overrides: Partial<ContentItem> = {}): ContentItem => ({
  type: "layer",
  id,
  name: `item-${id}`,
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

describe("useContentSelection", () => {
  it("toggle adds an id, and toggling it again removes it", () => {
    const items = [item("a"), item("b")];
    const { result } = renderHook(() => useContentSelection(items));

    act(() => result.current.toggle("a"));
    expect(result.current.selected.has("a")).toBe(true);
    expect(result.current.anySelected).toBe(true);

    act(() => result.current.toggle("a"));
    expect(result.current.selected.has("a")).toBe(false);
    expect(result.current.anySelected).toBe(false);
  });

  it("clear empties the whole selection", () => {
    const items = [item("a"), item("b")];
    const { result } = renderHook(() => useContentSelection(items));

    act(() => {
      result.current.toggle("a");
      result.current.toggle("b");
    });
    expect(result.current.selected.size).toBe(2);

    act(() => result.current.clear());
    expect(result.current.selected.size).toBe(0);
    expect(result.current.anySelected).toBe(false);
  });

  it("prunes a selected id once it drops out of the items prop (deleted/moved/filtered away)", () => {
    let items = [item("a"), item("b")];
    const { result, rerender } = renderHook(({ items }) => useContentSelection(items), {
      initialProps: { items },
    });

    act(() => result.current.toggle("a"));
    act(() => result.current.toggle("b"));
    expect(result.current.selected.size).toBe(2);

    items = [item("b")];
    rerender({ items });

    expect(result.current.selected.has("a")).toBe(false);
    expect(result.current.selected.has("b")).toBe(true);
  });

  it("selectedItems preserves the feed's order, not selection (insertion) order", () => {
    const items = [item("a"), item("b"), item("c")];
    const { result } = renderHook(() => useContentSelection(items));

    // Select c before a — feed order should still win.
    act(() => result.current.toggle("c"));
    act(() => result.current.toggle("a"));

    expect(result.current.selectedItems.map((i) => i.id)).toEqual(["a", "c"]);
  });

  it("selectOnly replaces the whole selection with a single id", () => {
    const items = [item("a"), item("b")];
    const { result } = renderHook(() => useContentSelection(items));

    act(() => {
      result.current.toggle("a");
      result.current.toggle("b");
    });
    act(() => result.current.selectOnly("b"));

    expect([...result.current.selected]).toEqual(["b"]);
  });
});
