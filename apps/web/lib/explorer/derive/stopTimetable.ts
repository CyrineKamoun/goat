/**
 * A stop's timetable for a date (board 13): every departure from the stop —
 * from each of a station's platforms, when it is a station — grouped by
 * route and by direction, short turns marked.
 *
 * A departure is a run of a trip (a frequency-based trip runs many times)
 * leaving the stop: its time there is the trip's own, moved by as much as
 * the run is from the trip's first departure. A trip's last stop is where
 * it arrives, not where it leaves, and is left out — once, so a loop still
 * leaves from where it starts.
 */
import type { SqlRunner } from "@/lib/explorer/db/types";
import type { Direction, DirectionName } from "@/lib/explorer/derive/directions";
import { ALL_TRIPS, runsSql } from "@/lib/explorer/derive/runs";

export interface Departure {
  /** The stop it leaves from: one of a station's platforms, or the stop. */
  stop_idx: number;
  route_idx: number;
  trip_idx: number;
  pattern_idx: number;
  headsign: string | null;
  /** The run's departure from its first stop: with trip_idx, which run. */
  run_dep_s: number;
  /** Its departure from this stop, seconds from the start of the service day. */
  dep_s: number;
}

export interface StopDetail {
  stop_idx: number;
  stop_id: string;
  name: string | null;
  /** The station, where the stop is one of its platforms. */
  station: string | null;
  platform_code: string | null;
  /** GTFS wheelchair_boarding: 1 possible, 2 not, null or 0 unknown. */
  wheelchair: number | null;
  is_station: boolean;
  /** A station's stops (platforms); 0 for a stop. */
  n_stops: number;
  lat: number | null;
  lon: number | null;
  /** Every route calling at the stop (a station: at its stops), on any day. */
  routes: number[];
}

/** The stops a stop's timetable covers: itself, and a station's platforms. */
const covered = (stop_idx: number) =>
  `SELECT stop_idx FROM stops WHERE stop_idx = ${stop_idx} OR parent_idx = ${stop_idx}`;

export async function stopDetail(db: SqlRunner, stop_idx: number): Promise<StopDetail | null> {
  const [[row], [served]] = await Promise.all([
    db.all<Omit<StopDetail, "routes" | "is_station"> & { location_type: number }>(`
      SELECT s.stop_idx, s.stop_id, s.name, p.name AS station, s.platform_code, s.wheelchair,
             s.location_type, s.lat, s.lon,
             CAST((SELECT count(*) FROM stops c
                   WHERE c.parent_idx = s.stop_idx AND COALESCE(c.location_type, 0) = 0) AS INTEGER) AS n_stops
      FROM stops s LEFT JOIN stops p ON p.stop_idx = s.parent_idx
      WHERE s.stop_idx = ${stop_idx}`),
    db.all<{ routes: ArrayLike<number> | null }>(`
      SELECT list_sort(list_distinct(list(route_idx))) AS routes
      FROM (SELECT route_idx, UNNEST(stops) AS stop_idx FROM patterns)
      WHERE stop_idx IN (${covered(stop_idx)})`),
  ]);
  if (!row) return null;
  const { location_type, ...rest } = row;
  return {
    ...rest,
    is_station: location_type === 1,
    routes: served?.routes ? Array.from(served.routes) : [],
  };
}

export async function stopDepartures(db: SqlRunner, stop_idx: number, date: string): Promise<Departure[]> {
  return db.all<Departure>(`
    WITH calls AS (
      SELECT trip_idx, stop_idx, stop_sequence, COALESCE(departure_s, arrival_s) AS at_s
      FROM stop_times WHERE stop_idx IN (${covered(stop_idx)})
    ),
    last AS (
      SELECT trip_idx, max(stop_sequence) AS last_seq FROM stop_times
      WHERE trip_idx IN (SELECT trip_idx FROM calls) GROUP BY trip_idx
    ),
    r AS (${runsSql(ALL_TRIPS, date, "SELECT trip_idx FROM calls")})
    SELECT c.stop_idx, t.route_idx, r.trip_idx, r.pattern_idx, r.headsign, r.dep_s AS run_dep_s,
           c.at_s + (r.dep_s - t.first_dep_s) AS dep_s
    FROM r JOIN trips t ON t.trip_idx = r.trip_idx
    JOIN calls c ON c.trip_idx = r.trip_idx
    JOIN last l ON l.trip_idx = r.trip_idx
    WHERE c.stop_sequence < l.last_seq AND c.at_s IS NOT NULL
    ORDER BY dep_s, t.route_idx, r.trip_idx`);
}

export interface TimetableEntry {
  dep_s: number;
  trip_idx: number;
  run_dep_s: number;
  /** A short turn of its direction: it ends before the direction does. */
  short: boolean;
}

export interface TimetableDirection {
  /** The direction's key, or the headsign where no direction knows the pattern. */
  key: string;
  name: DirectionName;
  departures: TimetableEntry[];
}

/** A route's departures from the stop, by direction in the route's order.
 * Directions come from `routeDirections`; a pattern none of them has (it
 * should not happen) goes under its headsign. */
export function byDirection(departures: Departure[], directions: Direction[]): TimetableDirection[] {
  const of = new Map<number, { direction: Direction; short: boolean }>();
  for (const d of directions)
    for (const p of d.patterns) of.set(p.pattern_idx, { direction: d, short: p.kind === "short_turn" });
  const groups = new Map<string, TimetableDirection>();
  const order = new Map(directions.map((d, i) => [String(d.key), i]));
  for (const dep of departures) {
    const found = of.get(dep.pattern_idx);
    const key = found ? String(found.direction.key) : `headsign:${dep.headsign ?? ""}`;
    let group = groups.get(key);
    if (!group) {
      group = {
        key,
        name: found?.direction.name ?? { kind: "to", place: dep.headsign ?? "", via: null },
        departures: [],
      };
      groups.set(key, group);
    }
    group.departures.push({
      dep_s: dep.dep_s,
      trip_idx: dep.trip_idx,
      run_dep_s: dep.run_dep_s,
      short: found?.short ?? false,
    });
  }
  return [...groups.values()].sort((a, b) => (order.get(a.key) ?? Infinity) - (order.get(b.key) ?? Infinity));
}

/** Departures by hour of the service day, for the hour × minute grid. */
export function byHour(entries: TimetableEntry[]): { hour: number; entries: TimetableEntry[] }[] {
  const hours = new Map<number, TimetableEntry[]>();
  for (const e of entries) {
    const h = Math.floor(e.dep_s / 3600);
    const list = hours.get(h);
    if (list) list.push(e);
    else hours.set(h, [e]);
  }
  return [...hours.entries()].sort(([a], [b]) => a - b).map(([hour, list]) => ({ hour, entries: list }));
}
