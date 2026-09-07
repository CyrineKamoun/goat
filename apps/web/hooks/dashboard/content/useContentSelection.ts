"use client";

import { useCallback, useEffect, useMemo, useState } from "react";

import type { ContentItem } from "@/lib/validations/content";

/**
 * Multi-select state for the feed: a set of selected item ids plus the
 * derived list of the selected items themselves. Selection is keyed by
 * `item.id` alone (ids are UUIDs, unique across every content type), so a
 * card and a row for the same item always agree on whether it's selected.
 *
 * `Escape` clears the whole selection, and an id is dropped the moment the
 * `items` it was selected from no longer contains it — the feed re-fetching
 * after a move/delete, or the user narrowing a filter, never leaves a
 * "ghost" selected id that no longer has a row to show it.
 */
export const useContentSelection = (items: ContentItem[]) => {
  const [selected, setSelected] = useState<Set<string>>(new Set());

  const toggle = useCallback((id: string) => {
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(id)) {
        next.delete(id);
      } else {
        next.add(id);
      }
      return next;
    });
  }, []);

  const clear = useCallback(() => setSelected(new Set()), []);

  const selectOnly = useCallback((id: string) => setSelected(new Set([id])), []);

  useEffect(() => {
    setSelected((prev) => {
      if (prev.size === 0) return prev;
      const liveIds = new Set(items.map((item) => item.id));
      let changed = false;
      const next = new Set<string>();
      prev.forEach((id) => {
        if (liveIds.has(id)) {
          next.add(id);
        } else {
          changed = true;
        }
      });
      return changed ? next : prev;
    });
  }, [items]);

  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") clear();
    };
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [clear]);

  const selectedItems = useMemo(() => items.filter((item) => selected.has(item.id)), [items, selected]);

  return { selected, toggle, clear, selectOnly, selectedItems, anySelected: selected.size > 0 };
};
