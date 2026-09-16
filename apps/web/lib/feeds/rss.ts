/**
 * RSS 2.0 reader for the plan4better.de feeds (blog and GOAT changelog).
 *
 * Parses with the browser's `DOMParser`, so it runs on the client only. Media
 * RSS thumbnails, `content:encoded` bodies and `dc:creator` are read from
 * their namespaces.
 */

const NS = {
  media: "http://search.yahoo.com/mrss/",
  content: "http://purl.org/rss/1.0/modules/content/",
  dc: "http://purl.org/dc/elements/1.1/",
  /** The changelog's own extension: which entries GOAT announces, and with what. */
  goat: "https://www.plan4better.de/ns/goat-changelog",
} as const;

/**
 * `<goat:spotlight>` on a changelog item. Its presence flags the entry for the
 * one-time announcement; every child is optional and overrides what the
 * plain item would give: `headline` (else title), `image` (else the
 * thumbnail), `video` (an mp4 or YouTube URL), `url` (else the item link),
 * `highlight` (repeatable, a bullet list for release announcements).
 */
export interface RssSpotlight {
  headline: string | null;
  image: string | null;
  video: string | null;
  url: string | null;
  highlights: string[];
}

export interface RssItem {
  title: string;
  link: string;
  /** `<guid>` text, or `null` when the item has none. */
  guid: string | null;
  /** `<guid isPermaLink>` — RSS defaults it to true. */
  guidIsPermaLink: boolean;
  /** `<pubDate>` as an ISO string, or `null` when missing or unparseable. */
  pubDate: string | null;
  description: string;
  /** `<content:encoded>` when the feed carries a full body. */
  content: string | null;
  categories: string[];
  /** `<media:thumbnail url>` first, else `<media:content url>` of an image. */
  thumbnail: string | null;
  creator: string | null;
  /** Present only when the item carries a `<goat:spotlight>` element. */
  spotlight: RssSpotlight | null;
}

const text = (parent: Element, tag: string): string | null => {
  const el = Array.from(parent.children).find((child) => child.tagName === tag);
  return el?.textContent?.trim() ?? null;
};

const nsText = (parent: Element, ns: string, local: string): string | null =>
  parent.getElementsByTagNameNS(ns, local)[0]?.textContent?.trim() ?? null;

const thumbnailOf = (item: Element): string | null => {
  const thumb = item.getElementsByTagNameNS(NS.media, "thumbnail")[0]?.getAttribute("url");
  if (thumb) return thumb;
  const content = Array.from(item.getElementsByTagNameNS(NS.media, "content")).find(
    (el) => el.getAttribute("medium") === "image" || (el.getAttribute("type") ?? "").startsWith("image/")
  );
  return content?.getAttribute("url") ?? null;
};

const spotlightOf = (item: Element): RssSpotlight | null => {
  const el = item.getElementsByTagNameNS(NS.goat, "spotlight")[0];
  if (!el) return null;
  const one = (local: string) => nsText(el, NS.goat, local) || null;
  return {
    headline: one("headline"),
    image: one("image"),
    video: one("video"),
    url: one("url"),
    highlights: Array.from(el.getElementsByTagNameNS(NS.goat, "highlight"))
      .map((h) => h.textContent?.trim() ?? "")
      .filter(Boolean),
  };
};

const isoDate = (value: string | null): string | null => {
  if (!value) return null;
  const ms = Date.parse(value);
  return Number.isNaN(ms) ? null : new Date(ms).toISOString();
};

/** Every `<item>` of an RSS 2.0 document; throws on XML that does not parse. */
export const parseRss = (xml: string): RssItem[] => {
  const doc = new DOMParser().parseFromString(xml, "text/xml");
  if (doc.getElementsByTagName("parsererror").length > 0) {
    throw new Error("Feed is not well-formed XML");
  }
  return Array.from(doc.getElementsByTagName("item")).map((item) => {
    const guidEl = Array.from(item.children).find((child) => child.tagName === "guid");
    return {
      title: text(item, "title") ?? "",
      link: text(item, "link") ?? "",
      guid: guidEl?.textContent?.trim() ?? null,
      guidIsPermaLink: (guidEl?.getAttribute("isPermaLink") ?? "true") !== "false",
      pubDate: isoDate(text(item, "pubDate")),
      description: text(item, "description") ?? "",
      content: nsText(item, NS.content, "encoded"),
      categories: Array.from(item.children)
        .filter((child) => child.tagName === "category")
        .map((child) => child.textContent?.trim() ?? "")
        .filter(Boolean),
      thumbnail: thumbnailOf(item),
      creator: nsText(item, NS.dc, "creator"),
      spotlight: spotlightOf(item),
    };
  });
};

/**
 * Rewrite every absolute URL in a feed from the origin the feed was built
 * for to the origin it is actually served from.
 *
 * The website bakes one public origin into its links and image URLs at build
 * time. When the same build is served from another host (a dev or staging
 * deployment, or a local Astro server), the feed's links point elsewhere.
 * The channel `<link>` names the baked origin, so it can be swapped for the
 * origin the feed was fetched from without the caller knowing either.
 */
export const alignFeedOrigin = (xml: string, servedFrom: string): string => {
  const channelLink = /<channel>[\s\S]*?<link>\s*([^<\s]+)\s*<\/link>/.exec(xml)?.[1];
  if (!channelLink) return xml;
  let baked: string;
  let target: string;
  try {
    baked = new URL(channelLink).origin;
    target = new URL(servedFrom).origin;
  } catch {
    return xml;
  }
  if (baked === target) return xml;
  return xml.split(baked).join(target);
};
