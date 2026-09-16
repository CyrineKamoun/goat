import { readFileSync } from "node:fs";
import path from "node:path";
import { describe, expect, it } from "vitest";

import { alignFeedOrigin, parseRss } from "@/lib/feeds/rss";

const fixture = (name: string) => readFileSync(path.join(__dirname, "fixtures", name), "utf8");

/** Copies of the website's feeds as served on 2026-09-14 (changelog from the dev host, blog from production). Re-copy when the feed shape changes. */
describe("parseRss", () => {
  it("reads every item of the changelog feed with its namespaced fields", () => {
    const items = parseRss(fixture("changelog-en.xml"));

    expect(items).toHaveLength(5);
    const first = items[0];
    expect(first.title).toBe("Run accessibility analyses on a network you bring yourself");
    expect(first.link).toBe(
      "https://website.plan4better.de/en/goat/changelog/2026-09#custom-routing-networks"
    );
    expect(first.guid).toBe("custom-routing-networks");
    expect(first.guidIsPermaLink).toBe(false);
    expect(first.pubDate).toBe("2026-09-14T00:00:00.000Z");
    expect(first.categories).toEqual(["new"]);
    expect(first.thumbnail).toMatch(/^https:\/\/website\.plan4better\.de\/_astro\/.*\.webp$/);
    expect(first.description.startsWith("Until now")).toBe(true);
    expect(first.content?.startsWith("<p>Until now")).toBe(true);
    expect(first.spotlight).toBeNull();
  });

  it("reads the goat:spotlight element with its optional children", () => {
    const items = parseRss(fixture("changelog-en.xml"));
    const catalog = items.find((item) => item.guid === "catalog");

    expect(catalog?.spotlight).toEqual({
      headline: "More than 30,000 datasets",
      image: expect.stringMatching(/05-catalog.*\.webp$/),
      video: null,
      url: "https://website.plan4better.de/en/post/goat-v3-data-catalog-templates-custom-routing-networks",
      highlights: [],
    });
    expect(items.filter((item) => item.spotlight)).toHaveLength(1);
  });

  it("reads the blog feed's slug categories, creator and thumbnail", () => {
    const items = parseRss(fixture("blog-de.xml"));

    expect(items).toHaveLength(20);
    const first = items[0];
    expect(first.categories).toEqual(["goat", "development", "news"]);
    expect(first.creator).toBe("Camila Narbaitz");
    expect(first.thumbnail).toMatch(/\.webp$/);
    expect(first.spotlight).toBeNull();
    expect(items.filter((item) => item.categories.includes("goat"))).toHaveLength(16);
  });

  it("reads a non-permalink guid, content:encoded and spotlight highlights when present", () => {
    const xml = `<?xml version="1.0"?>
<rss version="2.0" xmlns:content="http://purl.org/rss/1.0/modules/content/" xmlns:goat="https://www.plan4better.de/ns/goat-changelog">
  <channel><title>t</title><link>https://example.org/en</link>
    <item>
      <title>A</title>
      <link>https://example.org/en/a</link>
      <guid isPermaLink="false">a-key</guid>
      <pubDate>Mon, 14 Sep 2026 00:00:00 GMT</pubDate>
      <description>Short.</description>
      <content:encoded><![CDATA[<p>Long <b>body</b>.</p>]]></content:encoded>
      <goat:spotlight>
        <goat:video type="video/mp4">https://example.org/v.mp4</goat:video>
        <goat:highlight>One</goat:highlight>
        <goat:highlight>Two</goat:highlight>
      </goat:spotlight>
    </item>
  </channel>
</rss>`;
    const [item] = parseRss(xml);

    expect(item.guid).toBe("a-key");
    expect(item.guidIsPermaLink).toBe(false);
    expect(item.content).toBe("<p>Long <b>body</b>.</p>");
    expect(item.thumbnail).toBeNull();
    expect(item.creator).toBeNull();
    expect(item.spotlight).toEqual({
      headline: null,
      image: null,
      video: "https://example.org/v.mp4",
      url: null,
      highlights: ["One", "Two"],
    });
  });

  it("throws on XML that does not parse", () => {
    expect(() => parseRss("<rss><channel><item>")).toThrow();
  });
});

describe("alignFeedOrigin", () => {
  it("rewrites the baked origin to the one the feed is served from", () => {
    const xml = fixture("changelog-en.xml");
    const aligned = alignFeedOrigin(xml, "https://www.plan4better.de/");

    expect(aligned).not.toContain("https://website.plan4better.de");
    expect(aligned).toContain("https://www.plan4better.de/en/goat/changelog/2026-09#custom-routing-networks");
    expect(aligned).toContain("https://www.plan4better.de/_astro/");
  });

  it("leaves the feed alone when the origins already match", () => {
    const xml = fixture("changelog-en.xml");
    expect(alignFeedOrigin(xml, "https://website.plan4better.de")).toBe(xml);
  });

  it("leaves the feed alone when the channel link is missing", () => {
    const xml = "<rss><channel><title>t</title></channel></rss>";
    expect(alignFeedOrigin(xml, "https://website.plan4better.de")).toBe(xml);
  });
});
