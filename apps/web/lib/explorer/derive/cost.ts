/**
 * Operating cost, an estimate for comparing scenarios
 * (docs/pt-network-editor/docs/05-viewers-and-cost.md):
 *
 *   annual cost = vehicle-km a year × € per km (+ vehicle-hours × € per hour)
 *
 * A year's vehicle-km come from the timetable and the calendar: each service's
 * daily km times the days it runs within the feed's window, scaled to 365 days
 * when the window is shorter. Rates are per operator and route type and set by
 * the user; until the rates dialog exists, the defaults below apply.
 */
import type { SqlRunner } from "@/lib/explorer/db/types";
import { serviceDayCounts } from "@/lib/explorer/derive/calendar";
import type { TripRange } from "@/lib/explorer/derive/runs";

export interface Rate {
  per_km: number;
  /** Empty means the km rate is all-in. */
  per_hour: number | null;
}

/** Default rate by GTFS route type (basic and extended); bus otherwise. */
export function defaultRate(routeType: number | null): Rate {
  const t = routeType ?? 3;
  const basic = t >= 100 ? Math.floor(t / 100) : t;
  // Extended types: 1xx rail, 4xx urban rail, 7xx bus, 9xx tram.
  if (t >= 100) {
    if (basic === 1 || basic === 4) return { per_km: 12, per_hour: null };
    if (basic === 9) return { per_km: 7, per_hour: null };
    return { per_km: 4.5, per_hour: null };
  }
  if (t === 0 || t === 5 || t === 11) return { per_km: 7, per_hour: null }; // tram, cable tram, trolleybus
  if (t === 1 || t === 2 || t === 12) return { per_km: 12, per_hour: null }; // metro, rail, monorail
  return { per_km: 4.5, per_hour: null };
}

export interface AnnualFigures {
  vehicle_km: number;
  vehicle_hours: number;
  cost: number;
  /** Days the year's figures were counted over, before scaling to 365. */
  window_days: number;
}

/** A route's figures for a year, from every service it runs on. */
export async function annualFigures(
  db: SqlRunner,
  route: TripRange,
  window: [string, string],
  rate: Rate
): Promise<AnnualFigures> {
  // A year at most, from the window's start: a feed covering more counts once.
  const startMs = Date.parse(`${window[0]}T00:00:00Z`);
  const yearEnd = new Date(startMs + 364 * 86_400_000).toISOString().slice(0, 10);
  if (window[1] > yearEnd) window = [window[0], yearEnd];
  const days = await serviceDayCounts(db, window[0], window[1]);
  const perService = await db.all<{ service_idx: number; km: number; hours: number }>(`
    WITH t AS (
      SELECT * FROM trips WHERE trip_idx BETWEEN ${route.trip_lo ?? -1} AND ${route.trip_hi ?? -2}
    ),
    runs AS (
      SELECT service_idx, length_m, last_arr_s - first_dep_s AS secs, 1 AS n
      FROM t WHERE trip_idx NOT IN (SELECT trip_idx FROM frequencies)
      UNION ALL
      SELECT t.service_idx, t.length_m, t.last_arr_s - t.first_dep_s,
             len(range(f.start_s, f.end_s, f.headway_s))
      FROM t JOIN frequencies f USING (trip_idx)
    )
    SELECT service_idx, sum(length_m * n) / 1000 AS km, sum(secs * n) / 3600 AS hours
    FROM runs GROUP BY service_idx
  `);
  const windowDays =
    (Date.parse(`${window[1]}T00:00:00Z`) - Date.parse(`${window[0]}T00:00:00Z`)) / 86_400_000 + 1;
  const scale = windowDays < 365 ? 365 / windowDays : 1;
  let km = 0;
  let hours = 0;
  for (const s of perService) {
    const n = days.get(s.service_idx) ?? 0;
    km += s.km * n;
    hours += s.hours * n;
  }
  km *= scale;
  hours *= scale;
  return {
    vehicle_km: km,
    vehicle_hours: hours,
    cost: km * rate.per_km + (rate.per_hour ? hours * rate.per_hour : 0),
    window_days: windowDays,
  };
}
