import { useRef } from "react";

/**
 * Tab panels that mount on first visit and then stay mounted.
 *
 * Remounting a settings panel's tab tree on every switch costs ~500ms of pure
 * mount work (LayerStyle, Filter), so a visited tab is kept in the tree and
 * only hidden. `resetKey` is what the tabs are about — the selected layer or
 * bundle — and a change of it starts the bookkeeping over, so a freshly
 * selected subject mounts a tab when it is looked at rather than because the
 * previous subject had been there.
 *
 * Returns the predicate a panel guards its tab bodies with.
 */
export function useLazyTabs(activeTab: number, resetKey: unknown): (tab: number) => boolean {
  const visitedTabsRef = useRef(new Set<number>());
  const resetKeyRef = useRef(resetKey);
  if (resetKeyRef.current !== resetKey) {
    resetKeyRef.current = resetKey;
    visitedTabsRef.current = new Set();
  }
  visitedTabsRef.current.add(activeTab);
  return (tab: number) => activeTab === tab || visitedTabsRef.current.has(tab);
}

export default useLazyTabs;
