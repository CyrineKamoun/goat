/**
 * The Explorer's own drawing of the network: every shape of the bundle in
 * its route's colour, whatever GOAT's layers beneath show. The map draws the
 * bundle's shape_lines tiles and styles each shape from here, so this holds
 * what a shape is for — its colour and the routes on it — read once from the
 * snapshot. Kept outside Redux, as the playback clock is: thousands of shapes,
 * read by the map from outside the Explorer's provider.
 */
import type { SqlRunner } from "@/lib/explorer/db/types";
import { modeOf, routeColor } from "@/lib/explorer/modes";

export interface NetworkShape {
  /** `#RRGGBB`: the first route's colour, or its mode's. */
  color: string;
  route_ids: string[];
}

/** What the map needs of a route: to fade it by the Explorer tab's filter,
 * and to name it where a click finds several. */
export interface NetworkRoute {
  route_idx: number;
  route_type: number | null;
  agency_idx: number | null;
  label: string;
  name: string | null;
  color: string;
}

export interface NetworkDrawing {
  /** The bundle's shape_lines layer, which the map draws shapes from. */
  shapeLinesLayerId: string | null;
  /** By `shape_id`, as the tiles carry it. */
  shapes: Map<string, NetworkShape>;
  /** A route's shapes, for highlighting it. */
  byRoute: Map<string, string[]>;
  routes: Map<string, NetworkRoute>;
}

const EMPTY: NetworkDrawing = {
  shapeLinesLayerId: null,
  shapes: new Map(),
  byRoute: new Map(),
  routes: new Map(),
};

let drawing: NetworkDrawing = EMPTY;
const listeners = new Set<() => void>();

export const networkStore = {
  subscribe(listener: () => void): () => void {
    listeners.add(listener);
    return () => listeners.delete(listener);
  },
  getSnapshot(): NetworkDrawing {
    return drawing;
  },
  set(next: NetworkDrawing): void {
    drawing = next;
    listeners.forEach((l) => l());
  },
  reset(): void {
    drawing = EMPTY;
    listeners.forEach((l) => l());
  },
};

/** Every shape trips use, with its routes, read from the snapshot. */
export async function networkShapes(
  db: SqlRunner,
  shapeLinesLayerId: string | null
): Promise<NetworkDrawing> {
  const rows = await db.all<{
    shape_id: string;
    route_id: string;
    route_idx: number;
    agency_idx: number | null;
    short_name: string | null;
    long_name: string | null;
    color: string | null;
    route_type: number | null;
  }>(`
    SELECT m.shape_id, r.route_id, r.route_idx, r.agency_idx, r.short_name, r.long_name,
           r.color, r.route_type
    FROM shape_route sr JOIN shapes_meta m USING (shape_idx) JOIN routes r USING (route_idx)
    ORDER BY m.shape_id, r.route_idx`);
  const shapes = new Map<string, NetworkShape>();
  const byRoute = new Map<string, string[]>();
  const routes = new Map<string, NetworkRoute>();
  for (const r of rows) {
    const color = routeColor(r.color, modeOf(r.route_type));
    const s = shapes.get(r.shape_id);
    if (s) s.route_ids.push(r.route_id);
    else shapes.set(r.shape_id, { color, route_ids: [r.route_id] });
    const list = byRoute.get(r.route_id);
    if (list) list.push(r.shape_id);
    else byRoute.set(r.route_id, [r.shape_id]);
    if (!routes.has(r.route_id))
      routes.set(r.route_id, {
        route_idx: r.route_idx,
        route_type: r.route_type,
        agency_idx: r.agency_idx,
        label: r.short_name || r.long_name?.slice(0, 4) || r.route_id,
        name: r.long_name,
        color,
      });
  }
  return { shapeLinesLayerId, shapes, byRoute, routes };
}
