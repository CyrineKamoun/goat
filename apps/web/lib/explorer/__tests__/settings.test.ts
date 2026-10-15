// @vitest-environment node
import { describe, expect, it } from "vitest";

import {
  DEFAULT_SETTINGS,
  SERVICE_DAY_END_S,
  periodsFrom,
  rateFor,
  rateKey,
  settingsKey,
  settingsProblems,
} from "@/lib/explorer/settings";

describe("Explorer settings", () => {
  it("prices a route type at its set rate, else its default", () => {
    const settings = { ...DEFAULT_SETTINGS, rates: { [rateKey("A", 3)]: { per_km: 5, per_hour: 40 } } };
    expect(rateFor(settings, "A", 3)).toEqual({ per_km: 5, per_hour: 40 });
    // Another operator's buses, and the same operator's trams, keep the defaults.
    expect(rateFor(settings, "B", 3)).toEqual({ per_km: 4.5, per_hour: null });
    expect(rateFor(settings, "A", 0)).toEqual({ per_km: 7, per_hour: null });
  });

  it("covers the service day with contiguous periods", () => {
    const periods = periodsFrom([6, 9, 15, 19, 23].map((h) => h * 3600));
    expect(periods[0].start_s).toBe(0);
    expect(periods[periods.length - 1].end_s).toBe(SERVICE_DAY_END_S);
    periods.slice(1).forEach((p, i) => expect(p.start_s).toBe(periods[i].end_s));
  });

  it("refuses periods out of order, and rates or layovers out of range", () => {
    expect(settingsProblems(DEFAULT_SETTINGS)).toEqual([]);
    const backwards = { ...DEFAULT_SETTINGS, periods: periodsFrom([9, 7, 15, 18, 22].map((h) => h * 3600)) };
    expect(settingsProblems(backwards)).toEqual(["explorer_settings_periods_order"]);
    const negative = { ...DEFAULT_SETTINGS, rates: { x: { per_km: -1, per_hour: null } } };
    expect(settingsProblems(negative)).toEqual(["explorer_settings_rate_range"]);
    expect(settingsProblems({ ...DEFAULT_SETTINGS, layover_s: 7200 })).toEqual([
      "explorer_settings_layover_range",
    ]);
  });

  it("keys cached figures by what changes them, not by the currency", () => {
    expect(settingsKey({ ...DEFAULT_SETTINGS, currency: "CHF" })).toBe(settingsKey(DEFAULT_SETTINGS));
    expect(settingsKey({ ...DEFAULT_SETTINGS, layover_s: 0 })).not.toBe(settingsKey(DEFAULT_SETTINGS));
  });
});
