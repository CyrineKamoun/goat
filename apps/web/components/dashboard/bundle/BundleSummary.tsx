import { Button, Divider, Link, Stack, Typography, styled, useTheme } from "@mui/material";
import { format } from "date-fns";
import React, { useState } from "react";
import { useTranslation } from "react-i18next";
import { toast } from "react-toastify";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import { useDateFnsLocale } from "@/i18n/utils";

import { rebuildBundleArtifact } from "@/lib/api/bundleEdits";
import type { BundleDependency, BundleRead } from "@/lib/api/bundles";
import { METADATA_HEADER_ICONS } from "@/lib/constants/metadataIcons";
import { setRunningJobIds } from "@/lib/store/jobs/slice";

import { useGetMetadataValueTranslation } from "@/hooks/map/DatasetHooks";
import { useAppDispatch, useAppSelector } from "@/hooks/store/ContextHooks";

const ContainerWrapper = styled("div")({
  containerType: "inline-size",
  width: "100%",
});

const LayoutContainer = styled("div")({
  display: "flex",
  flexDirection: "row",
  gap: "16px",
  width: "100%",
  "@container (max-width: 600px)": {
    flexDirection: "column",
  },
});

const MetadataSection = styled("div")({
  flex: 4,
  order: 1,
  "@container (max-width: 600px)": {
    order: 2,
    flex: "1 1 100%",
  },
});

const MainContentSection = styled("div")({
  flex: 1,
  order: 2,
  "@container (max-width: 600px)": {
    order: 1,
    flex: "1 1 100%",
  },
});

/** Scalar provenance fields, as rendered on the summary. */
type MetadataField =
  | "description"
  | "geographical_code"
  | "data_reference_year"
  | "lineage"
  | "license"
  | "attribution"
  | "distributor_name"
  | "distributor_email"
  | "distribution_url";

interface BundleSummaryProps {
  bundle: BundleRead;
  dependencies?: BundleDependency[];
  /** Drop the long provenance list and keep only the aggregated fields,
   *  status and artifact state — what fits a bundle's metadata tab in the
   *  map, where vertical space is scarce. */
  hideMetadataSection?: boolean;
}

const BundleSummary: React.FC<BundleSummaryProps> = ({
  bundle,
  dependencies,
  hideMetadataSection = false,
}) => {
  const theme = useTheme();
  const [isRebuilding, setIsRebuilding] = useState(false);
  const dispatch = useAppDispatch();
  const runningJobIds = useAppSelector((state) => state.jobs.runningJobIds);
  const { t, i18n } = useTranslation("common");
  const getMetadataValueTranslation = useGetMetadataValueTranslation();
  const dateLocale = useDateFnsLocale();

  // The same aggregated fields a layer summarises, minus the two that classify a
  // layer rather than a dataset (data_category, language_code). Icons, headings
  // and value translation all go through the layer helpers so the two summaries
  // cannot drift apart. `type` resolves via metadata.type.<bundle_type>.
  const aggregatedFields = ["type", "geographical_code", "distributor_name", "license"] as const;

  // Statuses arrive as the backend's lowercase enum value ("ready"). Translated
  // where a label exists, capitalised otherwise so a status added later still
  // reads as a label rather than raw data.
  const statusLabel = bundle.status
    ? i18n.exists(`common:${bundle.status}`)
      ? t(bundle.status)
      : bundle.status.charAt(0).toUpperCase() + bundle.status.slice(1)
    : undefined;

  // One row per derived artifact. A GTFS bundle has both a timetable and a
  // stop-to-street linkage, and "something failed" is not useful without saying
  // which — so the API reports them separately and they are shown that way.
  const artifacts = bundle.artifacts ?? [];
  const artifactStateLabel = (state: string) =>
    i18n.exists(`common:artifact_${state}`)
      ? t(`artifact_${state}`)
      : state.charAt(0).toUpperCase() + state.slice(1);

  // Offered whenever an artifact is unusable, and for a bundle that holds no
  // artifact row at all unless its import completed: `failed` rows predating
  // the self-deleting import are deliberately left in the table by the
  // migration, and an import worker killed outright (OOM/SIGKILL) never
  // reaches that delete either, leaving the bundle at `processing` for good.
  // Neither ever grows an artifact on its own, so this button is the only way
  // back — and a bundle type that derives no artifact has nothing to rebuild,
  // but it reaches `ready` and so is not offered one.
  const needsRebuild =
    artifacts.some((artifact) => artifact.state !== "ready") ||
    (artifacts.length === 0 && bundle.status !== "ready");
  // A build already running makes a second click a duplicate, not a retry.
  const isBuilding = artifacts.some((artifact) => artifact.state === "building");

  // Bundle-specific, so they have no aggregated-field equivalent to reuse.
  const bundleAttributes: { key: string; heading: string; value?: string; icon: ICON_NAME }[] = [
    { key: "status", heading: t("status"), value: statusLabel, icon: ICON_NAME.CIRCLEINFO },
    ...artifacts.map((artifact) => ({
      key: artifact.kind,
      heading: i18n.exists(`common:artifact_kind.${artifact.kind}`)
        ? t(`artifact_kind.${artifact.kind}`)
        : artifact.kind,
      value: artifactStateLabel(artifact.state),
      icon: ICON_NAME.STREET_NETWORK,
    })),
    {
      key: "created_at",
      heading: t("created_at"),
      value: bundle.created_at ? format(new Date(bundle.created_at), "P", { locale: dateLocale }) : undefined,
      icon: ICON_NAME.CALENDAR,
    },
  ];

  // Same field vocabulary as a layer's summary, restricted to what describes a
  // whole acquisition. Rendered whether set or not, so an empty licence reads as
  // "not stated" rather than being invisible.
  const metadataFields: { field: MetadataField; heading: string; type: "text" | "email" | "url" }[] = [
    { field: "description", heading: t("metadata.headings.description"), type: "text" },
    { field: "geographical_code", heading: t("metadata.headings.geographical_code"), type: "text" },
    { field: "data_reference_year", heading: t("metadata.headings.data_reference_year"), type: "text" },
    { field: "lineage", heading: t("metadata.headings.lineage"), type: "text" },
    { field: "license", heading: t("metadata.headings.license"), type: "text" },
    { field: "attribution", heading: t("metadata.headings.attribution"), type: "text" },
    { field: "distributor_name", heading: t("metadata.headings.distributor_name"), type: "text" },
    { field: "distributor_email", heading: t("metadata.headings.distributor_email"), type: "email" },
    { field: "distribution_url", heading: t("metadata.headings.distribution_url"), type: "url" },
  ];

  return (
    <ContainerWrapper>
      <LayoutContainer>
        {!hideMetadataSection && (
          <MetadataSection>
            <Stack spacing={4} sx={{ width: "100%" }}>
              {metadataFields.map(({ field, heading, type }) => {
                // Description is the bundle's own column; the rest live in the
                // provenance document.
                const value = field === "description" ? bundle.description : bundle.dataset_metadata?.[field];
                return (
                  <Stack key={field} spacing={1}>
                    <Typography variant="caption">{heading}</Typography>
                    <Divider />
                    {!value && (
                      <Typography variant="body2" sx={{ fontStyle: "italic" }}>
                        {t(`metadata.no_metadata_available.${field}`)}
                      </Typography>
                    )}
                    {!!value && type === "email" && (
                      <Link href={`mailto:${value}`} target="_blank" rel="noopener noreferrer">
                        {String(value)}
                      </Link>
                    )}
                    {!!value && type === "url" && (
                      <Link href={String(value)} target="_blank" rel="noopener noreferrer">
                        {String(value)}
                      </Link>
                    )}
                    {!!value && type === "text" && <Typography variant="body2">{String(value)}</Typography>}
                  </Stack>
                );
              })}

              {!!dependencies?.length && (
                <Stack spacing={1}>
                  <Typography variant="caption">{t("bundle_dependencies")}</Typography>
                  <Divider />
                  {dependencies.map((dependency) => (
                    <Stack
                      key={`${dependency.dependency_kind}-${dependency.depends_on_bundle_id}`}
                      direction="row"
                      spacing={2}
                      alignItems="center">
                      <Icon
                        iconName={ICON_NAME.LINK}
                        style={{ fontSize: 14 }}
                        htmlColor={theme.palette.text.secondary}
                      />
                      <Link href={`/bundles/${dependency.depends_on_bundle_id}`} variant="body2">
                        {dependency.depends_on_name}
                      </Link>
                      <Typography variant="caption" color="text.secondary">
                        {i18n.exists(`common:${dependency.depends_on_type}`)
                          ? t(dependency.depends_on_type)
                          : dependency.depends_on_type}
                      </Typography>
                    </Stack>
                  ))}
                </Stack>
              )}
            </Stack>
          </MetadataSection>
        )}

        <MainContentSection>
          <Stack spacing={2}>
            {aggregatedFields.map((key) => (
              <div key={key} style={{ display: "flex", gap: "8px", alignItems: "center" }}>
                <Icon
                  iconName={METADATA_HEADER_ICONS[key]}
                  style={{ fontSize: 14, flexShrink: 0 }}
                  htmlColor={theme.palette.text.secondary}
                />
                <div style={{ minWidth: 0 }}>
                  <Typography variant="caption" noWrap>
                    {i18n.exists(`common:metadata.headings.${key}`)
                      ? t(`common:metadata.headings.${key}`)
                      : key}
                  </Typography>
                  <Typography variant="body2" fontWeight="bold" noWrap>
                    {getMetadataValueTranslation(
                      key,
                      key === "type" ? bundle.bundle_type : (bundle.dataset_metadata?.[key] ?? "")
                    )}
                  </Typography>
                </div>
              </div>
            ))}
            {needsRebuild && (
              <Button
                size="small"
                variant="outlined"
                disabled={isRebuilding || isBuilding}
                onClick={async () => {
                  setIsRebuilding(true);
                  try {
                    const job = await rebuildBundleArtifact(bundle.id);
                    // Tracked like any other job, so progress and completion
                    // show up in the jobs popper instead of going silent.
                    if (job?.jobID) dispatch(setRunningJobIds([...runningJobIds, job.jobID]));
                    toast.success(t("bundle_update_started"));
                  } catch {
                    toast.error(t("bundle_update_failed_to_start"));
                  } finally {
                    setIsRebuilding(false);
                  }
                }}
                sx={{ textTransform: "none" }}>
                {t("update_bundle")}
              </Button>
            )}
            {bundleAttributes.map(({ key, heading, value, icon }) => (
              <div key={key} style={{ display: "flex", gap: "8px", alignItems: "center" }}>
                <Icon
                  iconName={icon}
                  style={{ fontSize: 14, flexShrink: 0 }}
                  htmlColor={theme.palette.text.secondary}
                />
                <div style={{ minWidth: 0 }}>
                  <Typography variant="caption" noWrap>
                    {heading}
                  </Typography>
                  <Typography variant="body2" fontWeight="bold" noWrap>
                    {value ?? " — "}
                  </Typography>
                </div>
              </div>
            ))}
          </Stack>
        </MainContentSection>
      </LayoutContainer>
    </ContainerWrapper>
  );
};

export default BundleSummary;
