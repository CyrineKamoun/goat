// @vitest-environment node

/**
 * What the Explorer derives from a snapshot, on the mini feed
 * (packages/python/goatlib/tests/fixtures/explorer_mini_gtfs). Every expected
 * value is worked out by hand from the feed:
 *
 * - stops 0.01° of longitude apart at 52.5° N, 676.9 m;
 * - R1 on weekdays: T1 08:00–08:08 and T2 08:10–08:18 S1→S5, T4 08:30–08:34
 *   S1→S3 (direction 0), T3 08:20–08:28 S5→S1 (direction 1); T1, T2, T4 give
 *   their distance in km (2.72, 2.72, 1.36), T3 none;
 * - R2: U1 23:50–24:10 and U2 24:20–24:50, one block, no direction_id;
 * - R3: the Saturday loop S1→S2→S3→S1;
 * - R4: F1 every 10 min 06:00–09:00 on weekdays, F2 on 25 December only.
 *
 * 2026-10-14 is a Wednesday; 2026-04-06 is Easter Monday, a removed date.
 */
import { beforeAll, describe, expect, it } from "vitest";

import { openFixtureDb } from "@/lib/explorer/__tests__/fixtureDb";
import type { SqlRunner } from "@/lib/explorer/db/types";
import { activeServicesSql, serviceDayCounts } from "@/lib/explorer/derive/calendar";
import { annualFigures, defaultRate } from "@/lib/explorer/derive/cost";
import { type Direction, routeDirections } from "@/lib/explorer/derive/directions";
import { headways, operations, runningTimes, vehiclesInService } from "@/lib/explorer/derive/operations";
import { type TripRange, routeRuns } from "@/lib/explorer/derive/runs";
import { isoWeekday, routeCalendar } from "@/lib/explorer/derive/serviceCalendar";
import { type PlaceRow, places } from "@/lib/explorer/derive/stops";
import { stopsLine } from "@/lib/explorer/derive/stopsLine";
import {
  alongShape,
  positionsAt,
  prepareDay,
  runningAt,
  serviceSpan,
  vehicleWindow,
} from "@/lib/explorer/derive/vehicles";
import {
  filterCount,
  listPlaces,
  matchingRouteIdxs,
  modeCounts,
  routeMatches,
  shows,
} from "@/lib/explorer/filters";

type Route = TripRange & { route_idx: number; route_id: string };

let db: SqlRunner;
const routes = new Map<string, Route>();

beforeAll(async () => {
  db = await openFixtureDb();
  for (const r of await db.all<Route>("SELECT route_idx, route_id, trip_lo, trip_hi FROM routes"))
    routes.set(r.route_id, r);
});

const route = (id: string) => routes.get(id) as Route;
/** A direction's name as the English UI words it. */
const label = (d: Direction) =>
  `${d.name.kind === "loop" ? "Loop via" : "To"} ${d.name.place}${d.name.via ? ` via ${d.name.via}` : ""}`;
const hm = (s: number) =>
  `${String(Math.floor(s / 3600)).padStart(2, "0")}:${String(Math.floor((s % 3600) / 60)).padStart(2, "0")}`;

describe("calendar", () => {
  const services = async (date: string) =>
    (
      await db.all<{ service_id: string }>(
        `SELECT service_id FROM services WHERE service_idx IN ${activeServicesSql(date)} ORDER BY 1`
      )
    ).map((r) => r.service_id);

  it("runs a service on its weekdays within its range", async () => {
    expect(await services("2026-10-14")).toEqual(["WK"]);
    expect(await services("2026-10-17")).toEqual(["SA"]);
  });

  it("applies removed and added dates", async () => {
    expect(await services("2026-04-06")).toEqual([]);
    expect(await services("2026-12-25")).toEqual(["WK", "XMAS"]);
  });

  it("counts a service's days over a window", async () => {
    // 5 Jan–31 Dec 2026: 51 weeks and Mon–Thu, less Easter Monday.
    const days = await serviceDayCounts(db, "2026-01-05", "2026-12-31");
    const ids = new Map(
      (await db.all<{ service_idx: number; service_id: string }>("SELECT * FROM services")).map((s) => [
        s.service_id,
        s.service_idx,
      ])
    );
    expect(days.get(ids.get("WK") as number)).toBe(51 * 5 + 4 - 1);
    expect(days.get(ids.get("SA") as number)).toBe(51);
    expect(days.get(ids.get("XMAS") as number)).toBe(1);
  });
});

describe("runs", () => {
  it("lists a day's timetabled trips", async () => {
    const runs = await routeRuns(db, route("R1"), "2026-10-14");
    expect(runs.map((r) => [hm(r.dep_s), hm(r.arr_s)])).toEqual([
      ["08:00", "08:08"],
      ["08:10", "08:18"],
      ["08:20", "08:28"],
      ["08:30", "08:34"],
    ]);
  });

  it("runs a frequency trip every headway, up to its end", async () => {
    const runs = await routeRuns(db, route("R4"), "2026-10-14");
    expect(runs).toHaveLength(18); // 06:00, 06:10 … 08:50
    expect(hm(runs[0].dep_s)).toBe("06:00");
    expect(hm(runs[17].dep_s)).toBe("08:50");
    expect(hm(runs[17].arr_s)).toBe("08:58");
  });

  it("keeps trips past midnight on their service day", async () => {
    const runs = await routeRuns(db, route("R2"), "2026-10-14");
    expect(runs.map((r) => [hm(r.dep_s), hm(r.arr_s)])).toEqual([
      ["23:50", "24:10"],
      ["24:20", "24:50"],
    ]);
  });
});

describe("directions", () => {
  const summary = async (id: string) =>
    (await routeDirections(db, route(id))).directions.map((d) => ({
      label: label(d),
      patterns: d.patterns.map((p) => `${p.kind} to ${p.destination}`),
    }));

  it("groups by direction_id when every trip has one, and finds short turns", async () => {
    expect(await summary("R1")).toEqual([
      { label: "To Fifth", patterns: ["main to Fifth", "short_turn to Third"] },
      { label: "To Main Station", patterns: ["main to Main Station"] },
    ]);
  });

  it("tells directions apart by stop order when the feed has no direction_id", async () => {
    expect((await summary("R2")).map((d) => d.label).sort()).toEqual(["To Second", "To Sixth"]);
  });

  it("names a loop by its farthest stop", async () => {
    expect(await summary("R3")).toEqual([{ label: "Loop via Third", patterns: ["main to Loop"] }]);
  });

  it("calls a pattern that skips stops of the main one an express", async () => {
    expect(await summary("R4")).toEqual([
      { label: "To Sixth", patterns: ["main to Sixth", "express to Sixth"] },
    ]);
  });
});

describe("operations", () => {
  it("adds up a day", async () => {
    const ops = operations(await routeRuns(db, route("R1"), "2026-10-14"));
    expect(ops.runs).toBe(4);
    expect(hm(ops.first_dep_s as number)).toBe("08:00");
    expect(hm(ops.last_dep_s as number)).toBe("08:30");
    // 2.72 + 2.72 + 1.36 km given, plus T3's four stop-to-stop hops.
    expect(ops.vehicle_km).toBeCloseTo(6.8 + 4 * 0.6769, 2);
    expect(ops.vehicle_hours).toBeCloseTo(28 / 60, 6);
  });

  it("takes peak vehicles from trips plus layover without blocks", async () => {
    // 08:00–08:14, 08:10–08:24, 08:20–08:34, 08:30–08:40: never more than two.
    const ops = operations(await routeRuns(db, route("R1"), "2026-10-14"));
    expect(ops.peak_vehicles).toBe(2);
    expect(hm(ops.peak_at_s as number)).toBe("08:10");
    expect(ops.peak_from_blocks).toBe(false);
  });

  it("takes peak vehicles from blocks where the feed has them", async () => {
    const ops = operations(await routeRuns(db, route("R2"), "2026-10-14"));
    expect(ops.peak_vehicles).toBe(1); // U1 and U2 are one vehicle
    expect(ops.peak_from_blocks).toBe(true);
  });

  it("gives the headway per period", async () => {
    const runs = await routeRuns(db, route("R4"), "2026-10-14");
    const h = headways(runs);
    expect(h.get("early")).toEqual({ minutes: 10, departures: 6 }); // 06:00–06:50
    expect(h.get("am_peak")).toEqual({ minutes: 10, departures: 12 });
    expect(h.get("midday")).toEqual({ minutes: null, departures: 0 });
  });

  it("counts vehicles in service through the day", async () => {
    const curve = vehiclesInService(await routeRuns(db, route("R1"), "2026-10-14"), 60);
    expect(curve[8 * 60 - 1]).toBe(0); // 07:59: nothing out yet
    expect(curve[8 * 60 + 5]).toBe(1); // 08:05: T1 on its way
    expect(curve[8 * 60 + 9]).toBe(0); // 08:09: T1 in, T2 not yet out
    expect(curve).toHaveLength(8 * 60 + 34 + 1); // to the last arrival, 08:34
  });
});

describe("cost", () => {
  it("takes a year's km from the calendar and prices them", async () => {
    const r = route("R1");
    const rate = defaultRate(3);
    const year = await annualFigures(db, r, ["2026-01-05", "2026-12-31"], rate);
    // R1 runs on WK only: 258 days in a 361-day window, scaled to 365.
    const daily = 6.8 + 4 * 0.6769;
    expect(year.window_days).toBe(361);
    expect(year.vehicle_km).toBeCloseTo(daily * 258 * (365 / 361), 0);
    expect(year.cost).toBeCloseTo(year.vehicle_km * 4.5, 6);
  });

  it("prices by route type until rates are set", () => {
    expect(defaultRate(3).per_km).toBe(4.5);
    expect(defaultRate(0).per_km).toBe(7);
    expect(defaultRate(2).per_km).toBe(12);
    expect(defaultRate(700).per_km).toBe(4.5);
    expect(defaultRate(null).per_hour).toBeNull();
  });
});

describe("network cost", () => {
  it("adds every route type at its rate", async () => {
    const { networkAnnualFigures } = await import("@/lib/explorer/derive/cost");
    const window: [string, string] = ["2026-01-05", "2026-12-31"];
    const network = await networkAnnualFigures(db, window);
    let expected = 0;
    for (const id of ["R1", "R2", "R3", "R4"]) {
      const r = route(id);
      const type = (
        await db.all<{ t: number }>(`SELECT route_type AS t FROM routes WHERE route_id = '${id}'`)
      )[0].t;
      expected += (await annualFigures(db, r, window, defaultRate(type))).cost;
    }
    expect(network.cost).toBeCloseTo(expected, 6);
  });
});

describe("stations and platforms", () => {
  const ids = (route_idxs: number[]) => {
    const byIdx = new Map([...routes.values()].map((r) => [r.route_idx, r.route_id]));
    return route_idxs.map((i) => byIdx.get(i)).sort();
  };

  it("puts platforms under their station and lists the routes calling there", async () => {
    const byId = new Map((await places(db)).map((p) => [p.stop_id, p]));
    // S1 is a platform of P: it is listed under the station, not on its own.
    expect([...byId.keys()].sort()).toEqual(["P", "S2", "S3", "S4", "S5", "S6", "X"]);
    const station = byId.get("P") as PlaceRow;
    expect(station.is_station).toBe(true);
    expect(station.platforms.map((p) => p.stop_id)).toEqual(["S1"]);
    expect(ids(station.route_idxs)).toEqual(["R1", "R3"]);
    expect(ids(station.platforms[0].route_idxs)).toEqual(["R1", "R3"]);
    expect(ids((byId.get("S2") as PlaceRow).route_idxs)).toEqual(["R1", "R2", "R3"]);
    expect(ids((byId.get("S6") as PlaceRow).route_idxs)).toEqual(["R2", "R4"]);
    expect(byId.get("X")?.route_idxs).toEqual([]);
    expect(byId.get("S2")?.platforms).toEqual([]);
  });

  it("finds a platform by its name and opens its station", async () => {
    const all = await places(db);
    const found = listPlaces(all, "platform 1", null);
    expect(found.map((f) => [f.place.stop_id, f.opened, f.platforms.map((p) => p.stop_id)])).toEqual([
      ["P", true, ["S1"]],
    ]);
    // A station found by its own name stays folded.
    expect(listPlaces(all, "main station", null)[0].opened).toBe(false);
  });
});

describe("filters", () => {
  const all = [
    { route_idx: 0, route_type: 3, agency_idx: 0 },
    { route_idx: 1, route_type: 0, agency_idx: 0 },
    { route_idx: 2, route_type: 3, agency_idx: 1 },
  ];
  const none = { types: [], modes: [], agencies: [] };

  it("narrows routes by route type and agency, and stops through them", () => {
    const f = { ...none, modes: ["bus" as const], agencies: [1] };
    expect(all.filter((r) => routeMatches(r, f)).map((r) => r.route_idx)).toEqual([2]);
    expect(matchingRouteIdxs(all, f)).toEqual(new Set([2]));
    expect(matchingRouteIdxs(all, { ...none, types: ["stops" as const] })).toBeNull();
  });

  it("shows both lists until one is picked, and counts what the feed has", () => {
    expect(shows(none, "stops")).toBe(true);
    expect(shows({ ...none, types: ["routes"] }, "stops")).toBe(false);
    expect(modeCounts(all)).toEqual([
      ["bus", 2],
      ["tram", 1],
    ]);
    expect(filterCount({ types: ["routes"], modes: ["bus"], agencies: [0, 1] })).toBe(4);
  });
});

describe("stops line", () => {
  // R1 towards Fifth: T1 and T2 run S1→S5, 2 min and 0.68 km a hop (the
  // feed's shape_dist is in km); T4 turns at S3.
  const line = async (date: string) => {
    const r = route("R1");
    const { directions } = await routeDirections(db, r);
    const toFifth = directions.find((d) => label(d) === "To Fifth") as Direction;
    return { toFifth, result: await stopsLine(db, r, toFifth, toFifth.key, date, "km") };
  };

  it("times and measures every hop of the main pattern", async () => {
    const { result } = await line("2026-10-14");
    expect(result.stops.map((s) => s.minutes)).toEqual([0, 2, 4, 6, 8]);
    expect(result.stops.slice(0, -1).map((s) => Math.round(s.next_m ?? 0))).toEqual([680, 680, 680, 680]);
    expect(result.stops.slice(0, -1).map((s) => s.next_min)).toEqual([2, 2, 2, 2]);
    expect(result.stops[4].next_m).toBeNull();
    // A platform is known by its station's name.
    expect(result.stops[0].name).toBe("Main Station");
    expect(result.of_date).toBe(true);
    // Both trips leave at 08:00 and 08:10: AM peak, nothing at Midday.
    expect(result.peak_min).toBe(8);
    expect(result.offpeak_min).toBeNull();
  });

  it("marks where the short turn ends and the other routes at each stop", async () => {
    const { result } = await line("2026-10-14");
    expect(result.stops[2].short_turns).toEqual([{ destination: "Third", trips: 1 }]);
    expect(result.stops.filter((s) => s.short_turns.length)).toHaveLength(1);
    const ids = new Map([...routes.values()].map((r) => [r.route_idx, r.route_id]));
    const others = result.stops.map((s) => s.transfers.map((i) => ids.get(i)).sort());
    expect(others).toEqual([["R3"], ["R2", "R3"], ["R3"], ["R4"], ["R4"]]);
  });

  it("falls back to all the pattern's trips on a day it does not run", async () => {
    const { result } = await line("2026-10-17");
    expect(result.of_date).toBe(false);
    expect(result.stops.map((s) => s.minutes)).toEqual([0, 2, 4, 6, 8]);
  });

  it("measures as the crow flies where the feed gives no distance", async () => {
    const r = route("R1");
    const { directions } = await routeDirections(db, r);
    const back = directions.find((d) => label(d) !== "To Fifth") as Direction;
    const result = await stopsLine(db, r, back, back.key, "2026-10-14", "km");
    expect(Math.round(result.stops[0].next_m ?? 0)).toBe(677);
  });
});

describe("route calendar", () => {
  // The feed runs 5 Jan – 31 Dec 2026. WK runs Mon–Fri but not on Easter
  // Monday (6 Apr); XMAS runs on 25 December only, beside WK.
  const window: [string, string] = ["2026-01-05", "2026-12-31"];

  it("names a route's weekday service and the holiday it skips", async () => {
    const cal = await routeCalendar(db, route("R1"), window);
    expect(cal.types).toHaveLength(1);
    const [weekdays] = cal.types;
    expect(weekdays.weekdays).toEqual([1, 2, 3, 4, 5]);
    expect(weekdays.except).toEqual(["2026-04-06"]);
    expect(weekdays.adds).toEqual([]);
    expect(weekdays.trips).toBe(4);
    expect(cal.days.find((d) => d.date === "2026-04-06")).toMatchObject({ services: [], exception: true });
    expect(cal.days.find((d) => d.date === "2026-10-17")?.trips).toBe(0);
    expect(cal).toMatchObject({ from_calendar: 1, exceptions: 1 });
  });

  it("gives a day with an extra service its own day type", async () => {
    const cal = await routeCalendar(db, route("R4"), window);
    // 259 weekdays from 5 Jan to 31 Dec, less Easter Monday and Christmas.
    expect(cal.types.map((t) => t.days.length)).toEqual([257, 1]);
    const [weekdays, christmas] = cal.types;
    expect(weekdays.except).toEqual(["2026-04-06", "2026-12-25"]);
    expect(christmas.days).toEqual(["2026-12-25"]);
    expect(christmas.weekdays).toEqual([5]);
  });

  it("names a Saturday service by its weekday", async () => {
    const cal = await routeCalendar(db, route("R3"), window);
    expect(cal.types.map((t) => t.weekdays)).toEqual([[6]]);
  });

  it("finds the weekday of a date", () => {
    expect(isoWeekday("2026-10-14")).toBe(3);
    expect(isoWeekday("2026-10-18")).toBe(7);
  });
});

describe("running times", () => {
  it("takes the busiest pattern's median per period, leaving short turns out", async () => {
    const r = route("R1");
    const { directions } = await routeDirections(db, r);
    const toFifth = directions.find((d) => label(d) === "To Fifth") as Direction;
    const own = new Set(toFifth.patterns.map((p) => p.pattern_idx));
    const runs = (await routeRuns(db, r, "2026-10-14")).filter((x) => own.has(x.pattern_idx));
    const times = runningTimes(runs);
    // T1 and T2 leave at 08:00 and 08:10 and take 8 min; T4, the short turn, 4.
    expect(times.get("am_peak")).toBe(8);
    expect(times.get("midday")).toBeNull();
  });
});

describe("route calendar with frequencies", () => {
  it("counts every run of a frequency-based trip", async () => {
    // F1 runs every 10 min from 06:00 to 09:00: 18 trips, as the Overview counts them.
    const cal = await routeCalendar(db, route("R4"), ["2026-01-05", "2026-12-31"]);
    const wednesday = cal.days.find((d) => d.date === "2026-10-14");
    expect(wednesday?.trips).toBe(18);
    expect(wednesday?.trips).toBe((await routeRuns(db, route("R4"), "2026-10-14")).length);
  });
});

describe("settings in the figures", () => {
  it("counts peak vehicles with the layover set", async () => {
    // R1's weekday runs, 08:00–08:34, 2 minutes apart at the ends: with no
    // layover one vehicle can follow another sooner.
    const runs = await routeRuns(db, route("R1"), "2026-10-14");
    expect(operations(runs, 0).peak_vehicles).toBeLessThanOrEqual(operations(runs, 6 * 60).peak_vehicles);
  });
});

describe("vehicles", () => {
  // Stops lie 0.01° of longitude apart along 52.5° N: S1 13.40 … S6 13.45.
  const at = async (date: string, t: number) => {
    await prepareDay(db, date);
    return positionsAt(await vehicleWindow(db, date, t - 600, t + 600), t);
  };
  const hm = (h: number, m: number) => h * 3600 + m * 60;

  it("places a vehicle between the stops it runs between", async () => {
    // T1 leaves S2 at 08:02 and reaches S3 at 08:04: at 08:03 it is halfway.
    const r1 = route("R1");
    const vehicles = (await at("2026-10-14", hm(8, 3))).filter((v) => v.route_idx === r1.route_idx);
    expect(vehicles).toHaveLength(1);
    expect(vehicles[0].lon).toBeCloseTo(13.415, 6);
    expect(vehicles[0].lat).toBeCloseTo(52.5, 6);
  });

  it("follows the trip's shape, not the straight line between stops", async () => {
    // SH1 leaves S1 for 52.505° N and comes back down at S2: T1, halfway
    // from S1 (08:00) to S2 (08:02), is at the top of the detour.
    const r1 = route("R1");
    const [t1] = (await at("2026-10-14", hm(8, 1))).filter((v) => v.route_idx === r1.route_idx);
    expect(t1.lon).toBeCloseTo(13.405, 4);
    expect(t1.lat).toBeCloseTo(52.505, 4);
  });

  it("goes in a straight line where a trip has no shape", async () => {
    // T3 (no shape) runs S5 → S1 from 08:20; at 08:21 it is halfway to S4.
    const r1 = route("R1");
    const vehicles = (await at("2026-10-14", hm(8, 21))).filter((v) => v.route_idx === r1.route_idx);
    expect(vehicles).toHaveLength(1);
    expect(vehicles[0].lon).toBeCloseTo(13.435, 6);
    expect(vehicles[0].lat).toBeCloseTo(52.5, 6);
  });

  it("walks a shape by distance", () => {
    const shape = {
      d: Float32Array.from([0, 100, 300]),
      lon: Float32Array.from([0, 1, 1]),
      lat: Float32Array.from([0, 0, 2]),
    };
    expect(alongShape(shape, 50)).toEqual([0.5, 0]);
    expect(alongShape(shape, 200)).toEqual([1, 1]);
    expect(alongShape(shape, 999)).toEqual([1, 2]);
  });

  it("runs every run of a frequency-based trip", async () => {
    // F1 leaves every 10 min from 06:00 and takes 8 (S4 → S5 in 4, S5 → S6
    // in 4): at 06:15 only the 06:10 run is out, a quarter of the way from
    // S5 (06:14) to S6 (06:18).
    const r4 = route("R4");
    const at0615 = (await at("2026-10-14", hm(6, 15))).filter((v) => v.route_idx === r4.route_idx);
    expect(at0615).toHaveLength(1);
    expect(at0615[0].lon).toBeCloseTo(13.44 + 0.01 * (1 / 4), 6);
    const at0607 = (await at("2026-10-14", hm(6, 7))).filter((v) => v.route_idx === r4.route_idx);
    expect(at0607).toHaveLength(1);
  });

  it("follows service past midnight", async () => {
    // U2 runs 24:20–24:50; its middle stop has no position, so it goes
    // straight from S6 to S2 between 24:20 and 24:50.
    await prepareDay(db, "2026-10-14");
    const span = await serviceSpan(db);
    expect(span?.last_s).toBe(hm(24, 50));
    expect(await runningAt(db, hm(24, 30))).toBe(1);
    const r2 = route("R2");
    const [u2] = (await at("2026-10-14", hm(24, 35))).filter((v) => v.route_idx === r2.route_idx);
    expect(u2.lon).toBeCloseTo(13.45 - 0.04 * 0.5, 6);
  });

  it("holds a vehicle at its stop while it waits there", () => {
    const window = {
      date: "2026-10-14",
      from_s: 0,
      to_s: 100,
      runs: new Map([
        [
          1,
          [
            {
              run: 1,
              route_idx: 0,
              t0: 10,
              t1: 20,
              a: [1, 1] as [number, number],
              b: [1, 1] as [number, number],
            },
            {
              run: 1,
              route_idx: 0,
              t0: 20,
              t1: 30,
              a: [1, 1] as [number, number],
              b: [2, 1] as [number, number],
            },
          ],
        ],
      ]),
    };
    expect(positionsAt(window, 15)).toEqual([{ run: 1, route_idx: 0, lon: 1, lat: 1 }]);
    expect(positionsAt(window, 25)[0].lon).toBe(1.5);
    expect(positionsAt(window, 40)).toEqual([]);
  });
});
