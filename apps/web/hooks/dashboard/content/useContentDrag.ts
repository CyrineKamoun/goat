"use client";

import { useCallback, useState } from "react";

import type { ContentItem, Space } from "@/lib/validations/content";

export interface ContentDragState {
  /** Ids currently being dragged: the single card/row that started the
   * drag, or the whole current selection when the dragged id was already
   * part of it — the page decides which on `startDrag`. */
  dragIds: string[];
  startDrag: (items: ContentItem[]) => void;
  /** Clears both `dragIds` and `dropTarget` — call on drag end and after a
   * drop, successful or not. */
  endDrag: () => void;
  /** The folder id (or "root" for the breadcrumb space name, or a space id
   * for the spaces panel) currently hovered as a drop target, so that
   * target can highlight itself. */
  dropTarget: string | null;
  setDropTarget: (id: string | null) => void;
  /** Whether the items currently being dragged may be dropped on
   * `spaceId` to open the transfer dialog (D3, promote-only): every
   * dragged item must be owned and live in the caller's own personal
   * space, `spaceId` must not be a personal space, and it must not be the
   * items' own (source) space. */
  canDropOnSpace: (spaceId: string, spaces: Space[]) => boolean;
}

/**
 * Holds the transient state of an HTML5 drag in progress across the Content
 * feed. It only tracks *what* is being dragged (the full items, not just
 * their ids, so `canDropOnSpace` can check ownership and space kind) and
 * *where* it is hovering — the page decides what counts as a draggable item
 * and what a drop actually does (move to a folder, or open the transfer
 * dialog for a space).
 */
export const useContentDrag = (): ContentDragState => {
  const [dragItems, setDragItems] = useState<ContentItem[]>([]);
  const [dropTarget, setDropTarget] = useState<string | null>(null);

  const startDrag = useCallback((items: ContentItem[]) => setDragItems(items), []);
  const endDrag = useCallback(() => {
    setDragItems([]);
    setDropTarget(null);
  }, []);

  const canDropOnSpace = useCallback(
    (spaceId: string, spaces: Space[]): boolean => {
      if (dragItems.length === 0) return false;
      if (spaceId === dragItems[0].space_id) return false;
      const targetSpace = spaces.find((s) => s.id === spaceId);
      if (!targetSpace || targetSpace.kind === "personal") return false;
      return dragItems.every((item) => {
        if (item.my_role !== "owner") return false;
        const itemSpace = spaces.find((s) => s.id === item.space_id);
        return itemSpace?.kind === "personal";
      });
    },
    [dragItems]
  );

  return {
    dragIds: dragItems.map((item) => item.id),
    startDrag,
    endDrag,
    dropTarget,
    setDropTarget,
    canDropOnSpace,
  };
};
