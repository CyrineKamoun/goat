import { readFileSync } from "node:fs";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";

import { nextSpotlight, releaseFromRssItem, unreadCount } from "@/lib/api/releases";
import { type RssItem, parseRss } from "@/lib/feeds/rss";
import type { ReleaseEntry } from "@/lib/validations/home";

const entry = (id: string, date: string, spotlight?: { headline: string }): ReleaseEntry => ({
  id,
  date,
  tag: "new",
  title: `Entry ${id}`,
  summary: "Summary",
  url: `https://www.plan4better.de/en/goat/changelog/2026-09#${id}`,
  spotlight: spotlight ? { ...spotlight, highlights: [] } : undefined,
});

const item = (overrides: Partial<RssItem> = {}): RssItem => ({
  title: "Search more than 30,000 datasets in the Catalog",
  link: "https://www.plan4better.de/en/goat/changelog/2026-09#catalog",
  guid: "catalog",
  guidIsPermaLink: false,
  pubDate: "2026-09-14T00:00:00.000Z",
  description: "The Catalog grew from 60 to more than 30,000 datasets.\n\nSecond paragraph.",
  content: null,
  categories: ["enhancement"],
  thumbnail: "https://www.plan4better.de/_astro/05-catalog.webp",
  creator: null,
  spotlight: null,
  ...overrides,
});

const fixture = () =>
  readFileSync(
    path.join(__dirname, "..", "..", "feeds", "__tests__", "fixtures", "changelog-en.xml"),
    "utf8"
  );

describe("releaseFromRssItem", () => {
  it("maps a changelog item onto a release entry", () => {
    expect(releaseFromRssItem(item())).toEqual({
      id: "catalog",
      date: "2026-09-14T00:00:00.000Z",
      tag: "improved",
      title: "Search more than 30,000 datasets in the Catalog",
      summary: "The Catalog grew from 60 to more than 30,000 datasets.",
      url: "https://www.plan4better.de/en/goat/changelog/2026-09#catalog",
      thumbnail: "https://www.plan4better.de/_astro/05-catalog.webp",
      spotlight: undefined,
    });
  });

  it("builds the spotlight from the goat:spotlight element, falling back to the item for missing children", () => {
    const bare = releaseFromRssItem(
      item({ spotlight: { headline: null, image: null, video: null, url: null, highlights: [] } })
    );
    expect(bare?.spotlight).toEqual({
      headline: "Search more than 30,000 datasets in the Catalog",
      image: "https://www.plan4better.de/_astro/05-catalog.webp",
      video: undefined,
      highlights: [],
      cta: { url: "https://www.plan4better.de/en/goat/changelog/2026-09#catalog" },
    });

    const full = releaseFromRssItem(
      item({
        spotlight: {
          headline: "More than 30,000 datasets",
          image: "https://www.plan4better.de/_astro/spot.webp",
          video: "https://videos.plan4better.de/x/play_720p.mp4",
          url: "https://www.plan4better.de/en/post/goat-v3",
          highlights: ["One", "Two"],
        },
      })
    );
    expect(full?.spotlight).toEqual({
      headline: "More than 30,000 datasets",
      image: "https://www.plan4better.de/_astro/spot.webp",
      video: "https://videos.plan4better.de/x/play_720p.mp4",
      highlights: ["One", "Two"],
      cta: { url: "https://www.plan4better.de/en/post/goat-v3" },
    });
  });

  it("falls back to the link anchor as id when the guid is the permalink", () => {
    const entry = releaseFromRssItem(
      item({ guid: "https://www.plan4better.de/en/goat/changelog/2026-09#catalog", guidIsPermaLink: true })
    );
    expect(entry?.id).toBe("catalog");
  });

  it("drops items without a date, a title or an absolute link, and only the thumbnail when that is relative", () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    expect(releaseFromRssItem(item({ pubDate: null }))).toBeNull();
    expect(releaseFromRssItem(item({ title: "" }))).toBeNull();
    expect(releaseFromRssItem(item({ link: "/en/goat/changelog/2026-09#catalog" }))).toBeNull();
    expect(warn).toHaveBeenCalledTimes(3);
    expect(releaseFromRssItem(item({ thumbnail: "/_astro/05-catalog.webp" }))?.thumbnail).toBeUndefined();
    warn.mockRestore();
  });

  it("falls back to Improved for a kind it does not know", () => {
    expect(releaseFromRssItem(item({ categories: ["breaking"] }))?.tag).toBe("improved");
  });

  it("maps every item of the website's changelog feed", () => {
    const entries = parseRss(fixture()).map(releaseFromRssItem);

    expect(entries).toHaveLength(5);
    expect(entries.every((entry) => entry !== null)).toBe(true);
    expect(entries.map((entry) => entry?.id)).toEqual([
      "custom-routing-networks",
      "home",
      "templates",
      "catalog",
      "content-management",
    ]);
    expect(entries.map((entry) => entry?.tag)).toEqual(["new", "new", "new", "improved", "improved"]);
    expect(entries[0]?.summary.startsWith("Until now, accessibility analyses")).toBe(true);
    expect(entries[0]?.spotlight).toBeUndefined();

    const catalog = entries.find((entry) => entry?.id === "catalog");
    expect(catalog?.spotlight?.headline).toBe("More than 30,000 datasets");
    expect(catalog?.spotlight?.image).toMatch(/05-catalog.*\.webp$/);
    expect(catalog?.spotlight?.video).toBeUndefined();
    expect(catalog?.spotlight?.cta?.url).toContain(
      "/en/post/goat-v3-data-catalog-templates-custom-routing-networks"
    );
  });
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

  it("never falls back to an older flagged entry once the newest is seen", () => {
    const entries = [withSpotlight("a", "2026-09-01"), withSpotlight("b", "2026-09-05")];
    expect(nextSpotlight(entries, ["b"])).toBeUndefined();
  });

  it("shows a newer flagged entry even when an older one was seen", () => {
    const entries = [withSpotlight("a", "2026-09-01"), withSpotlight("b", "2026-09-05")];
    expect(nextSpotlight(entries, ["a"])?.id).toBe("b");
  });

  it("ignores entries without a spotlight", () => {
    const entries = [entry("a", "2026-09-05"), withSpotlight("b", "2026-09-01")];
    expect(nextSpotlight(entries, [])?.id).toBe("b");
  });

  it("returns undefined when nothing is left to show", () => {
    expect(nextSpotlight([withSpotlight("a", "2026-09-01")], ["a"])).toBeUndefined();
  });

  it("returns undefined when there are no entries at all", () => {
    expect(nextSpotlight([], [])).toBeUndefined();
  });
});
