/**
 * The Explorer tab's filter (board 4): which lists show, and which routes —
 * by route type and agency — they hold. Stops follow the routes: with a route
 * filter on, a stop is listed when a matching route calls there.
 */
import type { PlaceRow, PlatformRow } from "@/lib/explorer/derive/stops";
import { type ModeKey, modeOf } from "@/lib/explorer/modes";
import type { ExplorerFilters } from "@/lib/store/explorer/slice";

export interface FilterableRoute {
  route_idx: number;
  route_type: number | null;
  agency_idx: number | null;
}

export function filterCount(filters: ExplorerFilters): number {
  return filters.types.length + filters.modes.length + filters.agencies.length;
}

/** Whether a list shows: both do until one is picked. */
export function shows(filters: ExplorerFilters, type: "routes" | "stops"): boolean {
  return filters.types.length === 0 || filters.types.includes(type);
}

export function routeMatches(route: FilterableRoute, filters: ExplorerFilters): boolean {
  if (filters.modes.length && !filters.modes.includes(modeOf(route.route_type))) return false;
  if (filters.agencies.length && (route.agency_idx === null || !filters.agencies.includes(route.agency_idx)))
    return false;
  return true;
}

/** The routes a stop list is narrowed to, or null when no route filter is on. */
export function matchingRouteIdxs(routes: FilterableRoute[], filters: ExplorerFilters): Set<number> | null {
  if (!filters.modes.length && !filters.agencies.length) return null;
  return new Set(routes.filter((r) => routeMatches(r, filters)).map((r) => r.route_idx));
}

export function toggle<T>(list: T[], value: T): T[] {
  return list.includes(value) ? list.filter((v) => v !== value) : [...list, value];
}

/** Route counts per mode, in a stable order, for the modes the feed has. */
export function modeCounts(routes: FilterableRoute[]): [ModeKey, number][] {
  const counts = new Map<ModeKey, number>();
  for (const r of routes) counts.set(modeOf(r.route_type), (counts.get(modeOf(r.route_type)) ?? 0) + 1);
  return [...counts.entries()].sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]));
}

export function agencyCounts(routes: FilterableRoute[]): Map<number, number> {
  const counts = new Map<number, number>();
  for (const r of routes)
    if (r.agency_idx !== null) counts.set(r.agency_idx, (counts.get(r.agency_idx) ?? 0) + 1);
  return counts;
}

export interface ListedPlace {
  place: PlaceRow;
  /** The platforms to show: all, or those the search or filter kept. */
  platforms: PlatformRow[];
  /** Opened by a search that matched one of its platforms. */
  opened: boolean;
}

const has = (v: string | null, q: string) => !!v && v.toLowerCase().includes(q);

/** The places a search and a route filter keep, with the platforms they keep. */
export function listPlaces(places: PlaceRow[], query: string, via: Set<number> | null): ListedPlace[] {
  const q = query.toLowerCase();
  const calledAt = (routes: number[]) => !via || routes.some((i) => via.has(i));
  const out: ListedPlace[] = [];
  for (const place of places) {
    if (!calledAt(place.route_idxs)) continue;
    const platforms = place.platforms.filter((p) => calledAt(p.route_idxs));
    if (!q || has(place.name, q) || has(place.stop_id, q)) {
      out.push({ place, platforms, opened: false });
      continue;
    }
    const found = platforms.filter((p) => has(p.name, q) || has(p.platform_code, q) || has(p.stop_id, q));
    if (found.length) out.push({ place, platforms: found, opened: true });
  }
  return out;
}
