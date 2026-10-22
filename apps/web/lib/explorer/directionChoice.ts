/**
 * The one direction choice the route's views share (02-panel-behaviour-spec ›
 * Direction): the trip strip, the Stops tab and the stop timetable. A choice
 * is all directions, one direction whole (its main pattern and every
 * variant), or one variant; none yet is all directions (22 Oct 2026: it
 * had been the first direction, which the strip showed while the map still
 * drew the whole route, and picking it changed nothing).
 */
import type { Direction } from "@/lib/explorer/derive/directions";
import type { ExplorerPatternChoice } from "@/lib/store/explorer/slice";

export interface ResolvedChoice {
  /** All directions. */
  all: boolean;
  /** The direction chosen; null for all, or a route with none. */
  direction: Direction | null;
  /** The variant chosen; null for a whole direction (or all). */
  pattern_idx: number | null;
  /** Every pattern the choice shows. */
  patterns: Set<number>;
}

export function resolveChoice(choice: ExplorerPatternChoice | null, directions: Direction[]): ResolvedChoice {
  const every = (ds: Direction[]) => new Set(ds.flatMap((d) => d.patterns.map((p) => p.pattern_idx)));
  if ((!choice || choice.direction === "all") && directions.length > 1)
    return { all: true, direction: null, pattern_idx: null, patterns: every(directions) };
  const direction = directions.find((d) => d.key === choice?.direction) ?? directions[0] ?? null;
  if (!direction) return { all: false, direction: null, pattern_idx: null, patterns: new Set() };
  const variant = direction.patterns.find((p) => p.pattern_idx === choice?.pattern_idx);
  return variant
    ? { all: false, direction, pattern_idx: variant.pattern_idx, patterns: new Set([variant.pattern_idx]) }
    : { all: false, direction, pattern_idx: null, patterns: every([direction]) };
}

/** One line for a view that draws one (the Stops tab, the map's highlight):
 * the variant chosen, else the direction's main pattern — the first
 * direction's when all are. */
export function shownLine(
  r: ResolvedChoice,
  directions: Direction[]
): { direction: Direction; pattern_idx: number } | null {
  const direction = r.direction ?? directions[0];
  if (!direction) return null;
  return { direction, pattern_idx: r.pattern_idx ?? direction.key };
}

/** The patterns the map highlights over the rest of the route, from the
 * drawing's directions (the map has them, not the full Direction); null,
 * none: the route as a whole. On the Stops tab, the one line it draws
 * (`shownPattern`). Elsewhere, what was picked — in the trip strip or the
 * Stops tab before: a variant alone, or a whole direction with all its
 * variants; nothing picked yet, or all directions, the whole route. */
export function highlightedPatterns(
  choice: ExplorerPatternChoice | null,
  directions: { key: number; patterns: number[] }[],
  stopsTab: boolean
): number[] | null {
  if (stopsTab) {
    const p = shownPattern(choice, directions);
    return p === null ? null : [p];
  }
  if (!choice || choice.direction === "all") return null;
  const direction = directions.find((d) => d.key === choice.direction);
  if (!direction) return null;
  return choice.pattern_idx !== null && direction.patterns.includes(choice.pattern_idx)
    ? [choice.pattern_idx]
    : direction.patterns;
}

/** The pattern `shownLine` draws, from the route drawing's directions (the
 * map has them, not the full Direction). */
export function shownPattern(
  choice: ExplorerPatternChoice | null,
  directions: { key: number; patterns: number[] }[]
): number | null {
  const direction = directions.find((d) => d.key === choice?.direction) ?? directions[0];
  if (!direction) return null;
  return choice?.pattern_idx !== null &&
    choice?.pattern_idx !== undefined &&
    direction.patterns.includes(choice.pattern_idx)
    ? choice.pattern_idx
    : direction.key;
}

/** The picker's value for a choice, and back. */
export function choiceValue(r: ResolvedChoice): string {
  return r.all ? "all" : `${r.direction?.key ?? ""}:${r.pattern_idx ?? ""}`;
}

export function choiceFromValue(value: string): ExplorerPatternChoice {
  if (value === "all") return { direction: "all", pattern_idx: null };
  const [d, p] = value.split(":");
  return { direction: Number(d), pattern_idx: p === "" ? null : Number(p) };
}
