import { readFileSync } from "node:fs";
import path from "node:path";
import { describe, expect, it } from "vitest";

import { GOAT_CATEGORY, blogPostFromRssItem } from "@/lib/api/blog";
import { parseRss } from "@/lib/feeds/rss";

describe("blogPostFromRssItem", () => {
  it("maps the live blog feed and keeps the GOAT category filterable", () => {
    const xml = readFileSync(
      path.join(__dirname, "..", "..", "feeds", "__tests__", "fixtures", "blog-de.xml"),
      "utf8"
    );
    const posts = parseRss(xml).map(blogPostFromRssItem);

    expect(posts).toHaveLength(20);
    expect(posts.every((post) => post !== null)).toBe(true);
    expect(posts.filter((post) => post?.categories.includes(GOAT_CATEGORY))).toHaveLength(16);
    expect(posts[0]).toMatchObject({
      title: "GOAT v3.0: über 30.000 Datensätze, Vorlagen und eigene Netzwerke",
      date: "2026-09-13T00:00:00.000Z",
      url: "https://www.plan4better.de/de/post/goat-v3-datenkatalog-vorlagen-eigene-routing-netzwerke",
    });
    expect(posts[0]?.thumbnail).toMatch(/\.webp$/);
  });
});
