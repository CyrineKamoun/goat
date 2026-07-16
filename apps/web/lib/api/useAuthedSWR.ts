"use client";

import useSWR, { type Key, type SWRConfiguration, type SWRResponse } from "swr";
import type { BareFetcher } from "swr";

import { useIsPublicProject } from "@/lib/providers/PublicProjectProvider";

/**
 * SWR wrapper for endpoints that require an authenticated user.
 *
 * The public project view has no user, so these requests would only ever 401 —
 * this nulls the SWR key in that context to skip them entirely. Use this instead
 * of plain `useSWR` for any hook that fetches user-scoped data, so the public-view
 * guard lives in one place and can't be forgotten at an individual call site.
 */
export function useAuthedSWR<Data = unknown, Err = unknown>(
  key: Key,
  fetcher: BareFetcher<Data> | null,
  config?: SWRConfiguration<Data, Err>
): SWRResponse<Data, Err> {
  const isPublicProject = useIsPublicProject();
  return useSWR<Data, Err>(isPublicProject ? null : key, fetcher, config);
}
