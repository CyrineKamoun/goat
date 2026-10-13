/**
 * What a route runs on a day: headways per time period and direction, first
 * and last trip, vehicle-km, vehicle-hours, peak vehicles and the
 * vehicles-in-service curve (docs/pt-network-editor/docs/05-viewers-and-cost.md).
 *
 * Computed from a day's runs (`routeRuns`), so a frequency-based trip counts
 * every time it runs.
 */
import type { Run } from "@/lib/explorer/derive/runs";

export interface Period {
  key: string;
  /** Seconds since the start of the service day; `end_s` exclusive. */
  start_s: number;
  end_s: number;
}

/** The bundle's time periods. They cover the service day without gaps
 * (goat_time_periods_gap_or_overlap); defining them per bundle comes later. */
export const DEFAULT_PERIODS: Period[] = [
  { key: "early", start_s: 0, end_s: 7 * 3600 },
  { key: "am_peak", start_s: 7 * 3600, end_s: 9 * 3600 },
  { key: "midday", start_s: 9 * 3600, end_s: 15 * 3600 },
  { key: "pm_peak", start_s: 15 * 3600, end_s: 18 * 3600 },
  { key: "evening", start_s: 18 * 3600, end_s: 22 * 3600 },
  { key: "night", start_s: 22 * 3600, end_s: 48 * 3600 },
];

/** Where no `block_id` says which trips one vehicle runs, each run is taken
 * to keep its vehicle this long after arriving. */
export const LAYOVER_S = 6 * 60;

export interface Headway {
  /** Median minutes between departures; null with fewer than two. */
  minutes: number | null;
  departures: number;
}

export interface Operations {
  runs: number;
  first_dep_s: number | null;
  last_dep_s: number | null;
  vehicle_km: number;
  vehicle_hours: number;
  peak_vehicles: number;
  /** When the peak is first reached. */
  peak_at_s: number | null;
  /** Whether peak vehicles came from block_id (else trips plus layover). */
  peak_from_blocks: boolean;
}

function median(values: number[]): number {
  const s = [...values].sort((a, b) => a - b);
  const m = Math.floor(s.length / 2);
  return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
}

/** Headway in each period, for the runs of one direction. */
export function headways(runs: Run[], periods: Period[] = DEFAULT_PERIODS): Map<string, Headway> {
  const out = new Map<string, Headway>();
  for (const p of periods) {
    const deps = runs
      .map((r) => r.dep_s)
      .filter((d) => d >= p.start_s && d < p.end_s)
      .sort((a, b) => a - b);
    const gaps = deps.slice(1).map((d, i) => d - deps[i]);
    out.set(p.key, {
      minutes: gaps.length ? Math.round(median(gaps) / 60) : null,
      departures: deps.length,
    });
  }
  return out;
}

/** The intervals during which vehicles are tied up: one per block where every
 * run has a block, else one per run with a layover after it. */
function vehicleIntervals(runs: Run[]): { intervals: [number, number][]; fromBlocks: boolean } {
  const fromBlocks = runs.length > 0 && runs.every((r) => r.block_idx !== null);
  if (!fromBlocks) return { intervals: runs.map((r) => [r.dep_s, r.arr_s + LAYOVER_S]), fromBlocks };
  const blocks = new Map<number, [number, number]>();
  for (const r of runs) {
    const b = blocks.get(r.block_idx as number);
    blocks.set(
      r.block_idx as number,
      b ? [Math.min(b[0], r.dep_s), Math.max(b[1], r.arr_s)] : [r.dep_s, r.arr_s]
    );
  }
  return { intervals: [...blocks.values()], fromBlocks };
}

/** Most intervals overlapping at once, and when that is first reached. */
function peak(intervals: [number, number][]): { n: number; at: number | null } {
  // Ends sort before starts at the same moment: a vehicle arriving at 08:10
  // can take the 08:10.
  const events = intervals
    .flatMap(([a, b]) => [
      [a, 1],
      [b, -1],
    ])
    .sort((x, y) => x[0] - y[0] || x[1] - y[1]);
  let n = 0;
  let best = 0;
  let at: number | null = null;
  for (const [t, d] of events) {
    n += d;
    if (n > best) {
      best = n;
      at = t;
    }
  }
  return { n: best, at };
}

export function operations(runs: Run[]): Operations {
  const { intervals, fromBlocks } = vehicleIntervals(runs);
  const p = peak(intervals);
  const deps = runs.map((r) => r.dep_s);
  return {
    runs: runs.length,
    first_dep_s: deps.length ? Math.min(...deps) : null,
    last_dep_s: deps.length ? Math.max(...deps) : null,
    vehicle_km: runs.reduce((s, r) => s + r.length_m, 0) / 1000,
    vehicle_hours: runs.reduce((s, r) => s + (r.arr_s - r.dep_s), 0) / 3600,
    peak_vehicles: p.n,
    peak_at_s: p.at,
    peak_from_blocks: fromBlocks,
  };
}

/** Vehicles in service (between departure and arrival) at each `step_s`
 * from 0 until the last arrival, so service past midnight is included. */
export function vehiclesInService(runs: Run[], step_s = 300): number[] {
  if (!runs.length) return [];
  const end = Math.max(...runs.map((r) => r.arr_s));
  const counts: number[] = [];
  for (let t = 0; t <= end; t += step_s) {
    counts.push(runs.filter((r) => r.dep_s <= t && t < r.arr_s).length);
  }
  return counts;
}
