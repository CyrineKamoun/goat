import useSWR from "swr";

import { FEEDS_ENABLED, type FeedKind, type FeedLocale, websiteFeedUrl } from "@/lib/feeds/config";
import { type RssItem, alignFeedOrigin, parseRss } from "@/lib/feeds/rss";

export * from "@/lib/feeds/config";

/**
 * Fetch one of the website's feeds and parse it. Every URL in the feed is
 * aligned to the origin it was fetched from, so a feed built for the
 * production host still links correctly when GOAT points at a dev or local
 * deployment of the website.
 */
export const fetchFeed = async (url: string): Promise<RssItem[]> => {
  const response = await fetch(url, {
    headers: { accept: "application/rss+xml, application/xml, text/xml" },
  });
  if (!response.ok) {
    throw new Error(`Failed to fetch ${url}: ${response.status}`);
  }
  return parseRss(alignFeedOrigin(await response.text(), url));
};

/** The items of one website feed, or `[]` when no website URL is configured. */
export const useFeed = (kind: FeedKind, locale: FeedLocale) => {
  const { data, isLoading, error } = useSWR(FEEDS_ENABLED ? websiteFeedUrl(kind, locale) : null, fetchFeed, {
    revalidateOnFocus: false,
    dedupingInterval: 5 * 60 * 1000,
  });
  return {
    items: data ?? [],
    isLoading: FEEDS_ENABLED && isLoading,
    isError: error,
  };
};
