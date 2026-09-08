import { describe, expect, it } from "vitest";

import sample from "@/lib/validations/__tests__/releases-feed-sample.json";
import { releasesFeedSchema } from "@/lib/validations/home";

/**
 * A copy of `apps/docs/static/releases.json` as `build-releases-feed.mjs`
 * actually produced it from `apps/docs/releases/2026-09-04-content-spaces.md`
 * (en) and its `i18n/de` translation — kept as a fixture rather than an
 * import across the docs/web package boundary. Re-copy it whenever the
 * script or the release note changes, so this test keeps validating the
 * real shape rather than a hand-written stand-in.
 */
describe("the real releases.json output", () => {
  it("parses against the feed schema the app reads", () => {
    expect(() => releasesFeedSchema.parse(sample)).not.toThrow();
  });

  it("carries both locales with the content-spaces entry", () => {
    const feed = releasesFeedSchema.parse(sample);
    const en = feed.entries.en ?? [];
    const de = feed.entries.de ?? [];

    expect(en).toHaveLength(1);
    expect(de).toHaveLength(1);
    expect(en[0].id).toBe("2026-09-content-spaces");
    expect(de[0].id).toBe("2026-09-content-spaces");
  });

  it("carries the spotlight block through for both locales", () => {
    const feed = releasesFeedSchema.parse(sample);
    const en = feed.entries.en ?? [];
    const de = feed.entries.de ?? [];

    expect(en[0].spotlight?.headline).toBe("Content Spaces is here");
    expect(de[0].spotlight?.headline).toBe("Content Spaces ist da");
  });
});
