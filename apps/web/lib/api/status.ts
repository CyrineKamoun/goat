import useSWR from "swr";

import { publicFetcher } from "@/lib/api/publicFetcher";
import { statusFeedSchema } from "@/lib/validations/home";

/** Service-status feed, published by the status site (plan4better/status). */
export const STATUS_FEED_URL = process.env.NEXT_PUBLIC_STATUS_FEED_URL ?? "";

/** Polls the status feed every minute so an incident banner appears without a reload. */
export const useStatusFeed = () => {
  const { data } = useSWR(
    STATUS_FEED_URL || null,
    async (url: string) => statusFeedSchema.parse(await publicFetcher(url)),
    { refreshInterval: 60_000, revalidateOnFocus: true }
  );
  return { status: data };
};
