import { WEBSITE_FEEDS_ENABLED, WEBSITE_URL } from "@/lib/constants";

/** Where the public website's feeds live; free of React so `lib/feeds` stays pure. */

export type FeedKind = "blog" | "changelog";
export type FeedLocale = "en" | "de";

/** Every feed surface is on only when `NEXT_PUBLIC_WEBSITE_URL` names a real deployment. */
export const FEEDS_ENABLED = WEBSITE_FEEDS_ENABLED;

/** The feed's path on the website, per kind, below the locale segment. */
const FEED_PATHS: Record<FeedKind, string> = {
  blog: "blog/rss.xml",
  changelog: "goat/changelog/rss.xml",
};

export const websiteFeedUrl = (kind: FeedKind, locale: FeedLocale): string =>
  `${WEBSITE_URL}/${locale}/${FEED_PATHS[kind]}`;

export const blogIndexUrl = (locale: FeedLocale): string => `${WEBSITE_URL}/${locale}/blog`;
export const changelogUrl = (locale: FeedLocale): string => `${WEBSITE_URL}/${locale}/goat/changelog`;

/** One formatter per UI language; construction is the expensive half of `Intl`. */
const DATE_FORMATTERS: Record<FeedLocale, Intl.DateTimeFormat> = {
  en: new Intl.DateTimeFormat("en-GB", { day: "numeric", month: "long", year: "numeric", timeZone: "UTC" }),
  de: new Intl.DateTimeFormat("de-DE", { day: "numeric", month: "long", year: "numeric", timeZone: "UTC" }),
};

/** A feed date for display, in UTC so a midnight `pubDate` never slips a day. */
export const formatFeedDate = (iso: string, locale: FeedLocale): string =>
  DATE_FORMATTERS[locale].format(new Date(iso));
