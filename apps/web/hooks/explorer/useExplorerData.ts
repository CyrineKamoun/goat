/**
 * What the Explorer's panels show, queried from the bundle's DuckDB-WASM
 * database. Every query is scoped to what is on screen — the route list, one
 * route's trips — so it stays fast on the largest feeds.
 */
import { useEffect, useState } from "react";

import type { SqlRunner } from "@/lib/explorer/db/types";
import {
  type AnnualFigures,
  annualFigures,
  defaultRate,
  networkAnnualFigures,
} from "@/lib/explorer/derive/cost";
import { type Direction, routeDirections } from "@/lib/explorer/derive/directions";
import {
  type Headway,
  type Operations,
  headways,
  operations,
  vehiclesInService,
} from "@/lib/explorer/derive/operations";
import { routeRuns } from "@/lib/explorer/derive/runs";
import { places } from "@/lib/explorer/derive/stops";
import { stopsLine } from "@/lib/explorer/derive/stopsLine";
import type { ExplorerRouteDrawing } from "@/lib/store/explorer/slice";

export interface RouteRow {
  route_idx: number;
  route_id: string;
  short_name: string | null;
  long_name: string | null;
  route_type: number | null;
  color: string | null;
  text_color: string | null;
  trip_lo: number | null;
  trip_hi: number | null;
  agency_idx: number | null;
  agency: string | null;
}

/**
 * Answers kept per database: going back to the route list, or to a route seen
 * before, must not queue the same work again in the database's one worker —
 * the network's annual cost scans every trip, and a route opened behind it
 * waits for it.
 */
const answers = new WeakMap<SqlRunner, Map<string, Promise<unknown>>>();

function remembered<T>(db: SqlRunner, key: string, query: (db: SqlRunner) => Promise<T>): Promise<T> {
  let byKey = answers.get(db);
  if (!byKey) answers.set(db, (byKey = new Map()));
  let answer = byKey.get(key) as Promise<T> | undefined;
  if (!answer) {
    answer = query(db);
    // A failure is not kept: the next request tries again.
    answer.catch(() => byKey?.delete(key));
    byKey.set(key, answer);
  }
  return answer;
}

/** Run `query` against `db` for `key`; the answer for the latest key wins. */
function useQuery<T>(db: SqlRunner | undefined, key: string, query: (db: SqlRunner) => Promise<T>) {
  const [state, setState] = useState<{ data?: T; error?: string; loading: boolean }>({ loading: true });
  useEffect(() => {
    if (!db) return;
    let current = true;
    setState((s) => ({ ...s, loading: true }));
    remembered(db, key, query)
      .then((data) => current && setState({ data, loading: false }))
      .catch((e: unknown) => current && setState({ error: String(e), loading: false }));
    return () => {
      current = false;
    };
    // The key names everything the query depends on.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [db, key]);
  return state;
}

export function useRoutes(db: SqlRunner | undefined) {
  return useQuery(db, "routes", (d) =>
    d.all<RouteRow>(`
        SELECT r.route_idx, r.route_id, r.short_name, r.long_name, r.route_type, r.color,
               r.text_color, r.trip_lo, r.trip_hi, r.agency_idx, a.name AS agency
        FROM routes r LEFT JOIN agencies a USING (agency_idx)
        WHERE r.trip_lo IS NOT NULL
        ORDER BY r.route_idx`)
  );
}

/** Stations and stops on their own, with the routes calling there. */
export function useStops(db: SqlRunner | undefined) {
  return useQuery(db, "places", places);
}

export interface AgencyRow {
  agency_idx: number;
  name: string | null;
}

export function useAgencies(db: SqlRunner | undefined) {
  return useQuery(db, "agencies", (d) =>
    d.all<AgencyRow>("SELECT agency_idx, name FROM agencies ORDER BY name, agency_idx")
  );
}

export interface DirectionSummary {
  direction: Direction;
  headways: Map<string, Headway>;
  operations: Operations;
}

export interface RouteSummaryData {
  directions: DirectionSummary[];
  /** The whole route on the date. */
  operations: Operations;
  /** Vehicles in service every 5 minutes from midnight. */
  curve: number[];
  n_stops: number;
  annual: AnnualFigures;
  rate: ReturnType<typeof defaultRate>;
  drawing: Omit<ExplorerRouteDrawing, "routeId" | "color" | "shapeLinesLayerId">;
}

async function summarize(
  db: SqlRunner,
  route: RouteRow,
  date: string,
  window: [string, string]
): Promise<RouteSummaryData> {
  const [{ directions, stops, patterns }, runs] = await Promise.all([
    routeDirections(db, route),
    routeRuns(db, route, date),
  ]);
  const directionOfPattern = new Map<number, Direction>();
  for (const d of directions) for (const p of d.patterns) directionOfPattern.set(p.pattern_idx, d);
  const perDirection = directions.map((direction) => {
    const own = runs.filter((r) => directionOfPattern.get(r.pattern_idx) === direction);
    return { direction, headways: headways(own), operations: operations(own) };
  });

  const rate = defaultRate(route.route_type);
  const annual = await annualFigures(db, route, window, rate);

  // The map: the route's shapes, and straight lines for patterns without one.
  const shapes = await db.all<{ shape_id: string; pattern_idx: number }>(`
    SELECT DISTINCT s.shape_id, t.pattern_idx FROM trips t JOIN shapes_meta s USING (shape_idx)
    WHERE t.trip_idx BETWEEN ${route.trip_lo} AND ${route.trip_hi}`);
  const shaped = new Set(shapes.map((s) => s.pattern_idx));
  const coord = (idx: number): [number, number] | null => {
    const s = stops.get(idx);
    return s && s.lon != null && s.lat != null ? [s.lon, s.lat] : null;
  };
  const stopIds = new Set(patterns.flatMap((p) => p.stops));
  return {
    directions: perDirection,
    operations: operations(runs),
    curve: vehiclesInService(runs, 300),
    n_stops: stopIds.size,
    annual,
    rate,
    drawing: {
      shapeIds: [...new Set(shapes.map((s) => s.shape_id))],
      stops: [...stopIds].flatMap((idx) => {
        const c = coord(idx);
        return c ? [{ lon: c[0], lat: c[1], name: stops.get(idx)?.name ?? "" }] : [];
      }),
      stopChains: patterns
        .filter((p) => !shaped.has(p.pattern_idx))
        .map((p) => p.stops.map(coord).filter((c): c is [number, number] => c !== null)),
    },
  };
}

export function useRouteSummary(
  db: SqlRunner | undefined,
  route: RouteRow | undefined,
  date: string | null,
  window: [string, string] | null
) {
  return useQuery(
    route && date && window ? db : undefined,
    `route:${route?.route_idx}:${date}:${window?.[0]}:${window?.[1]}`,
    (d) => summarize(d, route as RouteRow, date as string, window as [string, string])
  );
}

/** The network's annual operating cost; needs the whole snapshot loaded. */
export function useNetworkCost(db: SqlRunner | undefined, window: [string, string] | null) {
  return useQuery(window ? db : undefined, `network-cost:${window?.[0]}:${window?.[1]}`, (d) =>
    networkAnnualFigures(d, window as [string, string])
  );
}

/** A pattern's line diagram for the Stops tab, on the Explorer's date. */
export function useStopsLine(
  db: SqlRunner | undefined,
  route: RouteRow,
  direction: Direction | undefined,
  pattern_idx: number | undefined,
  date: string | null,
  shapeDistUnit: string | null
) {
  return useQuery(
    direction && pattern_idx !== undefined && date ? db : undefined,
    `line:${route.route_idx}:${pattern_idx}:${date}`,
    (d) => stopsLine(d, route, direction as Direction, pattern_idx as number, date as string, shapeDistUnit)
  );
}
