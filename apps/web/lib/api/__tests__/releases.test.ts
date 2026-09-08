import { describe, expect, it } from "vitest";

import { nextSpotlight, releasesIndexUrl, unreadCount } from "@/lib/api/releases";
import type { ReleaseEntry } from "@/lib/validations/home";

const entry = (id: string, date: string, spotlight?: ReleaseEntry["spotlight"]): ReleaseEntry => ({
  id,
  date,
  tag: "new",
  title: `Entry ${id}`,
  summary: "Summary",
  url: `https://docs.plan4better.de/releases/${id}`,
  spotlight,
});

describe("unreadCount", () => {
  it("counts entries newer than seenAt", () => {
    const entries = [entry("a", "2026-09-01"), entry("b", "2026-09-03"), entry("c", "2026-09-05")];
    expect(unreadCount(entries, "2026-09-02")).toBe(2);
  });

  it("counts all entries when seenAt is null", () => {
    const entries = [entry("a", "2026-09-01"), entry("b", "2026-09-03")];
    expect(unreadCount(entries, null)).toBe(2);
  });

  it("counts none when every entry is at or before seenAt", () => {
    const entries = [entry("a", "2026-09-01"), entry("b", "2026-09-02")];
    expect(unreadCount(entries, "2026-09-02")).toBe(0);
  });

  it("compares chronologically, not lexicographically, across mixed date/datetime formats", () => {
    // A plain ISO date parses as that day's UTC midnight, so "2026-09-02" is
    // one hour after "2026-09-01T23:00:00Z" — a case where naive string
    // comparison of the two differently-shaped values cannot be trusted to
    // agree with the actual instants they name.
    const entries = [entry("a", "2026-09-02")];
    expect(unreadCount(entries, "2026-09-01T23:00:00Z")).toBe(1);
  });
});

describe("nextSpotlight", () => {
  const withSpotlight = (id: string, date: string) => entry(id, date, { headline: `Headline ${id}` });

  it("returns the newest flagged entry not yet seen", () => {
    const entries = [
      withSpotlight("a", "2026-09-01"),
      withSpotlight("b", "2026-09-05"),
      withSpotlight("c", "2026-09-03"),
    ];
    expect(nextSpotlight(entries, [])?.id).toBe("b");
  });

  it("skips entries already seen", () => {
    const entries = [withSpotlight("a", "2026-09-01"), withSpotlight("b", "2026-09-05")];
    expect(nextSpotlight(entries, ["b"])?.id).toBe("a");
  });

  it("ignores entries without a spotlight", () => {
    const entries = [entry("a", "2026-09-05"), withSpotlight("b", "2026-09-01")];
    expect(nextSpotlight(entries, [])?.id).toBe("b");
  });

  it("returns undefined when nothing is left to show", () => {
    const entries = [withSpotlight("a", "2026-09-01")];
    expect(nextSpotlight(entries, ["a"])).toBeUndefined();
  });

  it("returns undefined when there are no entries at all", () => {
    expect(nextSpotlight([], [])).toBeUndefined();
  });
});

describe("releasesIndexUrl", () => {
  it("swaps the feed's releases.json filename for the plain releases route", () => {
    expect(releasesIndexUrl("https://docs.plan4better.de/releases.json")).toBe(
      "https://docs.plan4better.de/releases"
    );
  });
});
