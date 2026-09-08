import { describe, expect, it } from "vitest";

import { localized, statusFeedSchema } from "@/lib/validations/home";

/**
 * A status feed: `systems[].name` is an `{en, de}` record, and an incident's
 * `title`/`latestUpdate.body` is either a plain string or an `{en, de}` record.
 */
const sampleStatusFeed = {
  generatedAt: "2026-09-04T17:41:43.139Z",
  overall: "operational",
  url: "https://status.dev.plan4better.de/goat",
  systems: [
    { id: "login", name: { en: "Login", de: "Anmeldung" }, status: "operational" },
    { id: "workspace", name: { en: "Workspace", de: "Arbeitsbereich" }, status: "maintenance" },
  ],
  incidents: [
    {
      id: "inc-1",
      severity: "minor",
      phase: "monitoring",
      title: { en: "Uploads delayed", de: "Uploads verzögert" },
      startedAt: "2026-09-04T10:00:00.000Z",
      affected: ["uploads-exports"],
      latestUpdate: {
        at: "2026-09-04T11:00:00.000Z",
        body: { en: "We are monitoring the fix.", de: "Wir beobachten die Behebung." },
      },
    },
  ],
};

describe("statusFeedSchema", () => {
  it("parses a sample feed", () => {
    const parsed = statusFeedSchema.parse(sampleStatusFeed);
    expect(parsed.overall).toBe("operational");
    expect(parsed.systems).toHaveLength(2);
    expect(parsed.incidents[0]?.title).toEqual({ en: "Uploads delayed", de: "Uploads verzögert" });
  });

  it("accepts a plain-string title and latestUpdate.body", () => {
    const parsed = statusFeedSchema.parse({
      ...sampleStatusFeed,
      incidents: [
        {
          id: "inc-2",
          severity: "major",
          phase: "identified",
          title: "Uploads delayed",
          startedAt: "2026-09-04T10:00:00.000Z",
          affected: [],
          latestUpdate: { at: "2026-09-04T11:00:00.000Z", body: "We are monitoring the fix." },
        },
      ],
    });
    expect(parsed.incidents[0]?.title).toBe("Uploads delayed");
  });

  it("rejects a bad status level", () => {
    expect(() => statusFeedSchema.parse({ ...sampleStatusFeed, overall: "on_fire" })).toThrow();
  });

  it("rejects a bad severity-less incident missing required fields", () => {
    expect(() =>
      statusFeedSchema.parse({
        ...sampleStatusFeed,
        systems: [{ id: "login", name: { en: "Login" }, status: "not_a_status" }],
      })
    ).toThrow();
  });
});

describe("localized", () => {
  it("picks the requested locale out of a {en, de} record", () => {
    expect(localized({ en: "Uploads delayed", de: "Uploads verzögert" }, "de")).toBe("Uploads verzögert");
  });

  it("falls back to en when the requested locale is missing from the record", () => {
    expect(localized({ en: "Uploads delayed" }, "de")).toBe("Uploads delayed");
  });

  it("returns a plain string unchanged regardless of locale", () => {
    expect(localized("Uploads delayed", "de")).toBe("Uploads delayed");
  });

  it("accepts the status page's disrupted level for overall and systems", () => {
    const feed = statusFeedSchema.parse({
      generatedAt: "2026-09-04T10:00:00.000Z",
      overall: "disrupted",
      url: "https://status.plan4better.de/goat",
      systems: [{ id: "analyses", name: { en: "Analyses", de: "Analysen" }, status: "disrupted" }],
      incidents: [],
    });
    expect(feed.overall).toBe("disrupted");
  });
});
