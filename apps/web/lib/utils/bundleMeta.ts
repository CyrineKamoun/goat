/**
 * What a bundle's status and artifacts say about it.
 *
 * Read in two places that render nothing alike — the detail dialog's metadata
 * column and the map's bundle panel — so the rules live here rather than in
 * whichever of them was written first. Labels are looked up through the caller's
 * `t`/`i18n`: these are derivations, not components.
 */
import type { i18n as I18n, TFunction } from "i18next";

import { ICON_NAME } from "@p4b/ui/components/Icon";

import type { BundleRead } from "@/lib/api/bundles";

export type BundleStatusRow = {
  key: string;
  heading: string;
  value?: string;
  icon: ICON_NAME;
};

/** Translated where a label exists, capitalised otherwise, so a status added
 * later still reads as a label rather than raw data. */
const asLabel = (value: string, t: TFunction, i18n: I18n): string =>
  i18n.exists(`common:${value}`) ? t(value) : value.charAt(0).toUpperCase() + value.slice(1);

export const bundleStatusLabel = (bundle: BundleRead, t: TFunction, i18n: I18n): string | undefined =>
  bundle.status ? asLabel(bundle.status, t, i18n) : undefined;

export const artifactKindLabel = (kind: string, t: TFunction, i18n: I18n): string =>
  i18n.exists(`common:artifact_kind.${kind}`) ? t(`artifact_kind.${kind}`) : kind;

export const artifactStateLabel = (state: string, t: TFunction, i18n: I18n): string =>
  i18n.exists(`common:artifact_${state}`)
    ? t(`artifact_${state}`)
    : state.charAt(0).toUpperCase() + state.slice(1);

/**
 * One status, not two.
 *
 * `bundle.status` is only about the import, and an import that fails deletes
 * its bundle — so once it is past `processing` it reads "Ready" forever and
 * says nothing. What matters after that is whether the derived graph is usable,
 * which is what the artifact state answers. So: the import status while it is
 * still running, the artifact state afterwards, and the import status again for
 * a type that derives nothing.
 *
 * Still one row per artifact where a type has several: a GTFS bundle has both a
 * timetable and a stop-to-street linkage, and "something failed" is not useful
 * without saying which.
 */
export const bundleStatusRows = (bundle: BundleRead, t: TFunction, i18n: I18n): BundleStatusRow[] => {
  const artifacts = bundle.artifacts ?? [];
  if (bundle.status === "processing" || !artifacts.length) {
    return [
      {
        key: "status",
        heading: t("status"),
        value: bundleStatusLabel(bundle, t, i18n),
        icon: ICON_NAME.CIRCLEINFO,
      },
    ];
  }
  return artifacts.map((artifact) => ({
    key: artifact.kind,
    heading: artifacts.length > 1 ? artifactKindLabel(artifact.kind, t, i18n) : t("status"),
    value: artifactStateLabel(artifact.state, t, i18n),
    icon: ICON_NAME.CIRCLEINFO,
  }));
};

/**
 * Whether offering a rebuild would do anything.
 *
 * Offered whenever an artifact is unusable — but only where a rebuild is
 * possible at all: a GTFS bundle's timetable comes from the uploaded feed,
 * which is not kept, so the job would refuse and re-importing is the real
 * remedy. A bundle with no artifacts is either still importing or of a type
 * that derives none, and neither is something a rebuild fixes — an import that
 * fails deletes its bundle rather than leaving one to rescue.
 *
 * The second clause is the import that never got to write an artifact row at
 * all: a worker killed outright (OOM/SIGKILL) never reaches the delete that a
 * failed import does, leaving the bundle at `processing` for good. Still gated
 * on the type being rebuildable, or the button would only ever refuse.
 */
export const bundleNeedsRebuild = (bundle: BundleRead): boolean => {
  const artifacts = bundle.artifacts ?? [];
  return (
    !!bundle.artifacts_from_layers &&
    (artifacts.some((artifact) => artifact.state !== "ready") ||
      (artifacts.length === 0 && bundle.status !== "ready"))
  );
};

/** A build already running makes a second click a duplicate, not a retry. */
export const bundleIsBuilding = (bundle: BundleRead): boolean =>
  (bundle.artifacts ?? []).some((artifact) => artifact.state === "building");
