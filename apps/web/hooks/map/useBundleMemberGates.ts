import { useMemo } from "react";
import useSWR from "swr";

import { BUNDLES_API_BASE_URL, type BundleMember } from "@/lib/api/bundles";
import { apiRequestAuth } from "@/lib/api/fetcher";
import type { BundleMemberGate } from "@/lib/utils/bundleEditable";

/**
 * Role and editability of every bundle member layer present in a project.
 *
 * One request per bundle group, gathered into a single SWR entry: a hook cannot
 * be called once per group, and a project holds a handful of bundles at most.
 */
export function useBundleMemberGates(bundleIds: (string | null | undefined)[]) {
  const ids = useMemo(
    () => Array.from(new Set(bundleIds.filter((id): id is string => !!id))).sort(),
    [bundleIds]
  );

  const { data } = useSWR<Map<string, BundleMemberGate>>(
    ids.length ? ["bundle-member-gates", ...ids] : null,
    async () => {
      const gates = new Map<string, BundleMemberGate>();
      const listings = await Promise.all(
        ids.map(async (id) => {
          const response = await apiRequestAuth(`${BUNDLES_API_BASE_URL}/${id}/layers`);
          if (!response.ok) return [] as BundleMember[];
          return (await response.json()) as BundleMember[];
        })
      );
      for (const members of listings) {
        for (const member of members) {
          gates.set(member.layer_id, { role: member.role, editable: member.editable });
        }
      }
      return gates;
    }
  );

  return data;
}

export default useBundleMemberGates;
