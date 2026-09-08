/**
 * `useLazyTabs` is the bookkeeping the layer and bundle settings panels share:
 * a tab body mounts on first visit and then stays mounted, and switching to
 * another subject starts over so its tabs are not mounted before they are
 * looked at.
 */
import { renderHook } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { useLazyTabs } from "@/hooks/map/useLazyTabs";

describe("useLazyTabs", () => {
  it("keeps a visited tab live after moving away from it", () => {
    const { result, rerender } = renderHook(({ tab }) => useLazyTabs(tab, "layer-1"), {
      initialProps: { tab: 0 },
    });

    expect(result.current(0)).toBe(true);
    expect(result.current(1)).toBe(false);

    rerender({ tab: 1 });

    expect(result.current(1)).toBe(true);
    expect(result.current(0)).toBe(true);
  });

  it("forgets what was visited when the subject changes", () => {
    const { result, rerender } = renderHook(({ tab, key }) => useLazyTabs(tab, key), {
      initialProps: { tab: 1, key: "layer-1" },
    });
    rerender({ tab: 0, key: "layer-1" });
    expect(result.current(1)).toBe(true);

    rerender({ tab: 0, key: "layer-2" });

    expect(result.current(0)).toBe(true);
    expect(result.current(1)).toBe(false);
  });
});
