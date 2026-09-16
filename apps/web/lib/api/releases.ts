import { useMemo } from "react";

import { type FeedLocale, useFeed } from "@/lib/api/feeds";
import type { RssItem } from "@/lib/feeds/rss";
import type { ReleaseEntry, ReleaseTag } from "@/lib/validations/home";

/**
 * The changelog feed's kind slugs, mapped onto the three tags the app
 * renders. The website emits `new` and `enhancement`; `fixed` is accepted
 * should it ever add one.
 */
const TAG_BY_CATEGORY: Record<string, ReleaseTag> = {
  new: "new",
  enhancement: "improved",
  fixed: "fixed",
};

const isAbsoluteHttpUrl = (value: string): boolean => /^https?:\/\//.test(value);

const firstParagraph = (value: string): string =>
  value
    .trim()
    .split(/\n\s*\n/)[0]
    .replace(/\s+/g, " ")
    .trim();

/**
 * A stable id across languages: the feed's non-permalink `<guid>` when it
 * has one, otherwise the anchor of the item link (the month page names the
 * feature by the same slug in both locales), otherwise the link itself.
 */
const idOf = (item: RssItem): string => {
  if (item.guid && !item.guidIsPermaLink) return item.guid;
  const anchor = item.link.split("#")[1];
  return anchor || item.link;
};

/**
 * One changelog item as the `ReleaseEntry` the Home surfaces render, or
 * `null` (with a warning, since it disappears from the popper otherwise)
 * when it lacks a date, a title or an absolute link.
 */
export const releaseFromRssItem = (item: RssItem): ReleaseEntry | null => {
  if (!item.pubDate || !item.title || !isAbsoluteHttpUrl(item.link)) {
    console.warn("Changelog feed item skipped: missing date, title or absolute link", item.guid ?? item.link);
    return null;
  }
  const categories = item.categories.map((c) => c.toLowerCase());
  const tag = categories.map((c) => TAG_BY_CATEGORY[c]).find(Boolean) ?? "improved";
  const absolute = (value: string | null | undefined) =>
    value && isAbsoluteHttpUrl(value) ? value : undefined;
  const thumbnail = absolute(item.thumbnail);
  const s = item.spotlight;
  return {
    id: idOf(item),
    date: item.pubDate,
    tag,
    title: item.title,
    summary: firstParagraph(item.description),
    url: item.link,
    thumbnail,
    spotlight: s
      ? {
          headline: s.headline ?? item.title,
          image: absolute(s.image) ?? thumbnail,
          video: absolute(s.video),
          highlights: s.highlights,
          cta: { url: absolute(s.url) ?? item.link },
        }
      : undefined,
  };
};

/** The changelog entries for one locale, newest first, or `[]` when no website URL is configured. */
export const useReleases = (locale: FeedLocale) => {
  const { items, isLoading, isError } = useFeed("changelog", locale);
  const entries = useMemo(
    () =>
      items
        .map(releaseFromRssItem)
        .filter((entry): entry is ReleaseEntry => entry !== null)
        .sort((a, b) => new Date(b.date).getTime() - new Date(a.date).getTime()),
    [items]
  );
  return { entries, isLoading, isError };
};

/** How many entries are newer than the caller's release-notes watermark — all of them when never seen. */
export const unreadCount = (entries: ReleaseEntry[], seenAt: string | null): number =>
  seenAt === null
    ? entries.length
    : entries.filter((entry) => new Date(entry.date).getTime() > new Date(seenAt).getTime()).length;

/**
 * The one spotlight that may show: the newest flagged entry, and only while
 * the caller has not dismissed it. Older flagged entries are never announced,
 * so several flags in the feed still mean one announcement, not a queue.
 */
export const nextSpotlight = (entries: ReleaseEntry[], seen: string[]): ReleaseEntry | undefined => {
  const newest = entries
    .filter((entry) => entry.spotlight)
    .sort((a, b) => new Date(b.date).getTime() - new Date(a.date).getTime())[0];
  return newest && !seen.includes(newest.id) ? newest : undefined;
};
