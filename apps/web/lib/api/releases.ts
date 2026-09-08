import useSWR from "swr";

import { publicFetcher } from "@/lib/api/publicFetcher";
import { releasesFeedSchema } from "@/lib/validations/home";
import type { ReleaseEntry } from "@/lib/validations/home";

/**
 * Release notes feed, built by the docs site — unset in most environments,
 * so the hook below simply returns no entries rather than fetching an
 * address nobody serves.
 */
export const RELEASES_FEED_URL = process.env.NEXT_PUBLIC_RELEASES_FEED_URL ?? "";

/** The docs site's release index — the feed URL with its `releases.json`
 * filename swapped for the plain route (H7). Shared by the Home card and the
 * header popper, which both link out to it. */
export const releasesIndexUrl = (feedUrl: string): string => feedUrl.replace(/releases\.json$/, "releases");

/** The feed for one locale, or `[]` when no feed URL is configured. */
export const useReleases = (locale: "en" | "de") => {
  const { data, isLoading, error } = useSWR(RELEASES_FEED_URL || null, async (url: string) =>
    releasesFeedSchema.parse(await publicFetcher(url))
  );
  return {
    entries: data?.entries[locale] ?? [],
    isLoading: Boolean(RELEASES_FEED_URL) && isLoading,
    isError: error,
  };
};

/** How many entries are newer than the caller's release-notes watermark — all of them when never seen. */
export const unreadCount = (entries: ReleaseEntry[], seenAt: string | null): number =>
  seenAt === null
    ? entries.length
    : entries.filter((entry) => new Date(entry.date).getTime() > new Date(seenAt).getTime()).length;

/** The newest spotlight-flagged entry the caller has not dismissed, or none. */
export const nextSpotlight = (entries: ReleaseEntry[], seen: string[]): ReleaseEntry | undefined =>
  entries
    .filter((entry) => entry.spotlight && !seen.includes(entry.id))
    .sort((a, b) => new Date(b.date).getTime() - new Date(a.date).getTime())[0];
