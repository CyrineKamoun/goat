"use client";

import type { ReactNode } from "react";

import { ContentUiStateProvider } from "@/lib/providers/ContentUiStateProvider";

/**
 * `content` is a static segment, so this layout stays mounted while the
 * catch-all page below it moves between spaces, folders and the cross-space
 * views. The page's view preferences (expanded sections, details panel) live
 * here for that reason — see `ContentUiStateProvider`.
 */
const ContentLayout = ({ children }: { children: ReactNode }) => (
  <ContentUiStateProvider>{children}</ContentUiStateProvider>
);

export default ContentLayout;
