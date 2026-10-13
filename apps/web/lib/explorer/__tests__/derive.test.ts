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
import { routeDirections } from "@/lib/explorer/derive/directions";
import { headways, operations, vehiclesInService } from "@/lib/explorer/derive/operations";
import { type TripRange, routeRuns } from "@/lib/explorer/derive/runs";

type Route = TripRange & { route_idx: number; route_id: string };

let db: SqlRunner;
const routes = new Map<string, Route>();

beforeAll(async () => {
  db = await openFixtureDb();
  for (const r of await db.all<Route>("SELECT route_idx, route_id, trip_lo, trip_hi FROM routes"))
    routes.set(r.route_id, r);
});

const route = (id: string) => routes.get(id) as Route;
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
      label: d.label,
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
