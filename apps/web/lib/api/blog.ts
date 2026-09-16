import { useMemo } from "react";

import { type FeedLocale, useFeed } from "@/lib/api/feeds";
import type { RssItem } from "@/lib/feeds/rss";

export interface BlogPost {
  id: string;
  title: string;
  /** ISO date. */
  date: string;
  summary: string;
  url: string;
  thumbnail: string | null;
  categories: string[];
}

/** The blog category slug that marks a post as being about GOAT. */
export const GOAT_CATEGORY = "goat";

export const blogPostFromRssItem = (item: RssItem): BlogPost | null => {
  if (!item.pubDate || !item.link || !item.title) return null;
  return {
    id: item.guid ?? item.link,
    title: item.title,
    date: item.pubDate,
    summary: item.description.replace(/\s+/g, " ").trim(),
    url: item.link,
    thumbnail: item.thumbnail,
    categories: item.categories.map((c) => c.toLowerCase()),
  };
};

/** Blog posts for one locale, newest first, optionally only those in `category`. */
export const useBlogPosts = (locale: FeedLocale, options: { category?: string; limit?: number } = {}) => {
  const { items, isLoading, isError } = useFeed("blog", locale);
  const { category, limit } = options;
  const posts = useMemo(() => {
    const all = items
      .map(blogPostFromRssItem)
      .filter((post): post is BlogPost => post !== null)
      .filter((post) => !category || post.categories.includes(category))
      .sort((a, b) => new Date(b.date).getTime() - new Date(a.date).getTime());
    return limit ? all.slice(0, limit) : all;
  }, [items, category, limit]);
  return { posts, isLoading, isError };
};
