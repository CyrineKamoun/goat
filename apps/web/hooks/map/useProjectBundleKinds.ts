import { useMemo } from "react";

import type { BundleRead } from "@/lib/api/bundles";
import { useBundles } from "@/lib/api/bundles";
import { useProjectLayerGroups } from "@/lib/api/projects";

/**
 * Whether this project holds a network each bundle selector could offer.
 *
 * Asked so a network section can be left out entirely where there is nothing to
 * choose between: a project with no street network of its own should not be
 * shown a question whose only answer is the default.
 *
 * Each flag is computed from the selector's *own* filters rather than from a
 * fixed pair, because the tools do not agree on what they need: a catchment
 * routes public-transport legs live and wants the bundle's routing graph, while
 * a heatmap reads them out of the precomputed stop-to-street linkage. A flag
 * built on the wrong artifact would part company with the dropdown beneath it —
 * and since the choice is required once the section shows, a section over an
 * empty dropdown is a tool that cannot be run at all.
 *
 * One request, filtered here: the readiness rule is `artifacts[].state`, which
 * every bundle already carries, so asking the server once per selector would be
 * the same answer twice.
 */
export type BundleSelectorSpec = {
  /** The field the flag is named after (`pt_network_bundle_id`). */
  name: string;
  bundleType?: string;
  artifactKind?: string;
};

/** `pt_network_bundle_id` → `_project_has_pt_network_bundle`, which is what the
 * schema's `visible_when` refers to. */
export const bundleFlagName = (fieldName: string): string => `_project_has_${fieldName.replace(/_id$/, "")}`;

const hasReadyArtifact = (bundle: BundleRead, kind: string | undefined): boolean =>
  !kind || (bundle.artifacts ?? []).some((a) => a.kind === kind && a.state === "ready");

export function useProjectBundleKinds(
  projectId: string | undefined,
  selectors: BundleSelectorSpec[]
): Record<string, boolean> {
  const { layerGroups } = useProjectLayerGroups(projectId);
  const { data: bundles } = useBundles({ enabled: !!projectId && selectors.length > 0 });

  return useMemo(() => {
    const inProject = new Set(
      (layerGroups ?? []).map((group) => group.bundle_id).filter((id): id is string => !!id)
    );
    const flags: Record<string, boolean> = {};
    for (const selector of selectors) {
      flags[bundleFlagName(selector.name)] = (bundles ?? []).some(
        (bundle) =>
          inProject.has(bundle.id) &&
          (!selector.bundleType || bundle.bundle_type === selector.bundleType) &&
          hasReadyArtifact(bundle, selector.artifactKind)
      );
    }
    return flags;
  }, [layerGroups, bundles, selectors]);
}

export default useProjectBundleKinds;
