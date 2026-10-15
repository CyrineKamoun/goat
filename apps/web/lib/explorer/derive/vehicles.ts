/**
 * Where the network's vehicles are at a moment of a day, for playback
 * (02-panel-behaviour-spec › Playback): positions interpolated between
 * scheduled stop times, network-wide.
 *
 * A day's runs are worked out once (`prepareDay`) into the database. A window
 * of time is then read as segments (`vehicleWindow`) — a vehicle moving
 * between two stops, or waiting at one — and `positionsAt` places each run's
 * vehicle on its segment at any moment in the window, without the database,
 * so playback can draw many frames from one read.
 *
 * Vehicles move along their trip's shape (the snapshot's simplified
 * `shape_points`, with each stop placed along it in `pattern_shape_stops`),
 * so they follow the lines the map draws; a trip without a shape goes in a
 * straight line from stop to stop.
 */
import type { SqlRunner } from "@/lib/explorer/db/types";
import { runsSql } from "@/lib/explorer/derive/runs";

/** A shape as playback walks it: each vertex's distance along it (metres)
 * and position. */
export interface ShapeLine {
  d: Float32Array;
  lon: Float32Array;
  lat: Float32Array;
}

/** One run's movement over [t0, t1] (service-day seconds): from `a` to `b`,
 * or waiting at `a` where they are the same — along `shape` from `d0` to
 * `d1` where the trip has one. */
export interface Segment {
  run: number;
  route_idx: number;
  trip_idx: number;
  /** When the run left its first stop: with trip_idx, which run it is. */
  run_dep_s: number;
  t0: number;
  t1: number;
  a: [number, number];
  b: [number, number];
  shape?: ShapeLine;
  d0?: number;
  d1?: number;
}

export interface VehicleWindow {
  date: string;
  from_s: number;
  to_s: number;
  /** Each run's segments, in time order. */
  runs: Map<number, Segment[]>;
}

export interface Vehicle {
  run: number;
  route_idx: number;
  trip_idx: number;
  run_dep_s: number;
  lon: number;
  lat: number;
}

/** The day's runs, numbered, in a table playback reads its windows from. */
const DAY_TABLE = "playback_runs";

/** Work out a date's runs once: every trip running that day, every run of a
 * frequency-based one, with the offset from its template's times. */
export async function prepareDay(db: SqlRunner, date: string): Promise<void> {
  await db.all(`
    CREATE OR REPLACE TEMP TABLE ${DAY_TABLE} AS
    SELECT CAST(row_number() OVER (ORDER BY r.dep_s, r.trip_idx) AS INTEGER) AS run,
           r.trip_idx, t.route_idx, t.pattern_idx, t.shape_idx, r.dep_s, r.arr_s,
           r.dep_s - t.first_dep_s AS off
    FROM (${runsSql({ trip_lo: 0, trip_hi: 2147483647 }, date)}) r JOIN trips t USING (trip_idx)`);
}

/** The day's first departure and last arrival: the span playback can show. */
export async function serviceSpan(db: SqlRunner): Promise<{ first_s: number; last_s: number } | null> {
  const [row] = await db.all<{ first_s: number | null; last_s: number | null }>(
    `SELECT min(dep_s) AS first_s, max(arr_s) AS last_s FROM ${DAY_TABLE}`
  );
  return row && row.first_s !== null && row.last_s !== null
    ? { first_s: row.first_s, last_s: row.last_s }
    : null;
}

/** How many vehicles are running at `t`. */
export async function runningAt(db: SqlRunner, t: number): Promise<number> {
  const [row] = await db.all<{ n: number }>(
    `SELECT CAST(count(*) AS INTEGER) AS n FROM ${DAY_TABLE} WHERE dep_s <= ${t} AND arr_s >= ${t}`
  );
  return row?.n ?? 0;
}

/** The segments of the prepared day overlapping [from_s, to_s]. */
export async function vehicleWindow(
  db: SqlRunner,
  date: string,
  from_s: number,
  to_s: number
): Promise<VehicleWindow> {
  const [from, to] = [Math.floor(from_s), Math.ceil(to_s)];
  const rows = await db.all<{
    run: number;
    route_idx: number;
    trip_idx: number;
    run_dep_s: number;
    shape_idx: number | null;
    lon: number;
    lat: number;
    nlon: number | null;
    nlat: number | null;
    arr: number;
    dep: number;
    next_arr: number | null;
    d: number | null;
    next_d: number | null;
  }>(`
    WITH r AS (SELECT * FROM ${DAY_TABLE} WHERE dep_s <= ${to} AND arr_s >= ${from}),
    st AS (
      -- A stop without a position takes its station's; i is its place in
      -- the trip, which is its place in the trip's pattern.
      SELECT st.trip_idx,
             CAST(row_number() OVER (PARTITION BY st.trip_idx ORDER BY st.stop_sequence) - 1 AS INTEGER) AS i,
             COALESCE(st.arrival_s, st.departure_s) AS arr,
             COALESCE(st.departure_s, st.arrival_s) AS dep,
             COALESCE(s.lon, p.lon) AS lon, COALESCE(s.lat, p.lat) AS lat
      FROM stop_times st JOIN stops s USING (stop_idx) LEFT JOIN stops p ON p.stop_idx = s.parent_idx
      WHERE st.trip_idx IN (SELECT DISTINCT trip_idx FROM r)
    ),
    placed AS (
      SELECT st.*, r.run, r.route_idx, r.shape_idx, r.off, r.dep_s AS run_dep_s, ps.d
      FROM st JOIN r USING (trip_idx)
      LEFT JOIN pattern_shape_stops ps
        ON ps.pattern_idx = r.pattern_idx AND ps.shape_idx = r.shape_idx AND ps.i = st.i
      WHERE st.lon IS NOT NULL AND st.lat IS NOT NULL
    ),
    hops AS (
      SELECT *, lead(arr) OVER w AS next_arr, lead(lon) OVER w AS nlon, lead(lat) OVER w AS nlat,
             lead(d) OVER w AS next_d
      FROM placed WINDOW w AS (PARTITION BY run ORDER BY i)
    )
    SELECT run, route_idx, trip_idx, run_dep_s, shape_idx, lon, lat, nlon, nlat, d, next_d,
           arr + off AS arr, dep + off AS dep, next_arr + off AS next_arr
    FROM hops
    WHERE arr + off <= ${to} AND COALESCE(next_arr, dep) + off >= ${from}
    ORDER BY run, arr`);

  const lines = await shapeLines(
    db,
    rows.flatMap((h) => (h.shape_idx !== null && h.d !== null ? [h.shape_idx] : []))
  );
  const runs = new Map<number, Segment[]>();
  const add = (s: Segment) => {
    const list = runs.get(s.run);
    if (list) list.push(s);
    else runs.set(s.run, [s]);
  };
  for (const h of rows) {
    const at: [number, number] = [h.lon, h.lat];
    // Waiting at the stop, then (unless it is the last) on the way to the next.
    const who = { run: h.run, route_idx: h.route_idx, trip_idx: h.trip_idx, run_dep_s: h.run_dep_s };
    if (h.dep > h.arr) add({ ...who, t0: h.arr, t1: h.dep, a: at, b: at });
    if (h.next_arr === null || h.nlon === null || h.nlat === null) continue;
    const shape = h.shape_idx !== null ? lines.get(h.shape_idx) : undefined;
    add({
      ...who,
      t0: h.dep,
      t1: h.next_arr,
      a: at,
      b: [h.nlon, h.nlat],
      ...(shape && h.d !== null && h.next_d !== null ? { shape, d0: h.d, d1: h.next_d } : {}),
    });
  }
  return { date, from_s, to_s, runs };
}

/** Shapes read once per database: a window needs the shapes of the trips it
 * holds, and the next window mostly the same ones. */
const shapeCache = new WeakMap<SqlRunner, Map<number, ShapeLine>>();

async function shapeLines(db: SqlRunner, wanted: number[]): Promise<Map<number, ShapeLine>> {
  let cache = shapeCache.get(db);
  if (!cache) shapeCache.set(db, (cache = new Map()));
  const missing = [...new Set(wanted)].filter((s) => !cache?.has(s));
  if (missing.length) {
    const rows = await db.all<{ shape_idx: number; d: number; lon: number; lat: number }>(`
      SELECT shape_idx, d, lon, lat FROM shape_points
      WHERE shape_idx IN (${missing.join(",")}) ORDER BY shape_idx, i`);
    let start = 0;
    for (let k = 1; k <= rows.length; k++) {
      if (k < rows.length && rows[k].shape_idx === rows[start].shape_idx) continue;
      const part = rows.slice(start, k);
      cache.set(rows[start].shape_idx, {
        d: Float32Array.from(part, (r) => r.d),
        lon: Float32Array.from(part, (r) => r.lon),
        lat: Float32Array.from(part, (r) => r.lat),
      });
      start = k;
    }
  }
  return cache;
}

/** Forget a database's shapes: playback is stopping for want of memory. */
export function dropShapeCache(db: SqlRunner): void {
  shapeCache.delete(db);
}

/** The point `d` metres along `shape`. */
export function alongShape(shape: ShapeLine, d: number): [number, number] {
  const { d: ds, lon, lat } = shape;
  const n = ds.length;
  if (d <= ds[0]) return [lon[0], lat[0]];
  if (d >= ds[n - 1]) return [lon[n - 1], lat[n - 1]];
  let lo = 0;
  let hi = n - 1;
  while (hi - lo > 1) {
    const mid = (lo + hi) >> 1;
    if (ds[mid] <= d) lo = mid;
    else hi = mid;
  }
  const span = ds[hi] - ds[lo];
  const f = span > 0 ? (d - ds[lo]) / span : 0;
  return [lon[lo] + (lon[hi] - lon[lo]) * f, lat[lo] + (lat[hi] - lat[lo]) * f];
}

/** Each running vehicle's position at `t`, which must lie in the window. */
export function positionsAt(window: VehicleWindow, t: number): Vehicle[] {
  const out: Vehicle[] = [];
  for (const segments of window.runs.values()) {
    // Segments are few per run in a window: a scan is as quick as a search.
    const s = segments.find((x) => x.t0 <= t && t <= x.t1);
    if (!s) continue;
    const f = s.t1 > s.t0 ? (t - s.t0) / (s.t1 - s.t0) : 0;
    const [lon, lat] =
      s.shape && s.d0 !== undefined && s.d1 !== undefined
        ? alongShape(s.shape, s.d0 + (s.d1 - s.d0) * f)
        : [s.a[0] + (s.b[0] - s.a[0]) * f, s.a[1] + (s.b[1] - s.a[1]) * f];
    out.push({ run: s.run, route_idx: s.route_idx, trip_idx: s.trip_idx, run_dep_s: s.run_dep_s, lon, lat });
  }
  return out;
}
