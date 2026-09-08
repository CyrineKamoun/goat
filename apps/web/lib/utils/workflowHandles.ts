import type { Edge } from "@xyflow/react";

/** The generic input handle a node without named inputs exposes. */
const DEFAULT_INPUT_HANDLE = "input";

/**
 * The named input handles a node must render, read off its incoming edges.
 *
 * Used until the process description loads and the real input list is known.
 * Two edges may target the same handle — a user can wire two sources into one
 * input — so the names are deduplicated: they become React keys and handle ids,
 * and a repeated one renders two handles with the same key at the same position.
 * First occurrence wins, so the order the edges were made in is kept.
 */
export function edgeDerivedInputHandles(edges: Edge[], nodeId: string): string[] {
  const seen = new Set<string>();
  const handles: string[] = [];
  for (const edge of edges) {
    if (edge.target !== nodeId) continue;
    const handle = edge.targetHandle;
    if (!handle || handle === DEFAULT_INPUT_HANDLE || seen.has(handle)) continue;
    seen.add(handle);
    handles.push(handle);
  }
  return handles;
}
