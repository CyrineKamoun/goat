"use client";

import { Box, Link, Stack, Typography, useTheme } from "@mui/material";
import { format } from "date-fns";
import { useMemo, useState } from "react";
import { useTranslation } from "react-i18next";

import { ICON_NAME } from "@p4b/ui/components/Icon";

import { useDateFnsLocale } from "@/i18n/utils";

import type { BundleDependency, BundleMember, BundleRead } from "@/lib/api/bundles";
import { useBundleMemberLayers } from "@/lib/api/bundles";
import { METADATA_HEADER_ICONS } from "@/lib/constants/metadataIcons";
import { bundleStatusRows } from "@/lib/utils/bundleMeta";

import { useGetMetadataValueTranslation } from "@/hooks/map/DatasetHooks";

import BundleDependencies from "@/components/dashboard/bundle/BundleDependencies";
import BundleLayers from "@/components/dashboard/bundle/BundleLayers";
import BundleMapPreview from "@/components/dashboard/bundle/BundleMapPreview";
import {
  BUNDLE_ACCENT,
  DetailHeader,
  type DetailTab,
  DetailTabs,
  LicenseNotices,
  type MetaField,
  MetaSidebar,
} from "@/components/dashboard/common/DetailChrome";
import MarkdownProse from "@/components/dashboard/common/MarkdownProse";

/** One owned bundle: its members on a map, its description, and the shared
 * metadata beside them, with the member list on a second tab. The body of the
 * bundle preview dialog, which is where a bundle is read — the same shape a
 * dataset is read in, so the two do not feel like different applications. Its
 * sections sit flat on the dialog's own paper. */

type TabId = "summary" | "layers" | "dependencies";

/** The frame the map is drawn in: a bordered, rounded surface of its own
 * height, which the map fills. Matches the dataset detail's. */
const MapFrame = ({ children }: { children: React.ReactNode }) => {
  const theme = useTheme();
  return (
    <Box
      sx={{
        position: "relative",
        flex: "none",
        height: { xs: 320, md: 460 },
        borderRadius: 2.5,
        overflow: "hidden",
        border: `1px solid ${theme.palette.divider}`,
      }}>
      {children}
    </Box>
  );
};

const SectionHeading = ({ children }: { children: React.ReactNode }) => (
  <Typography sx={{ fontSize: 15, fontWeight: 600, mb: 3.5 }}>{children}</Typography>
);

type BundleDetailProps = {
  bundle: BundleRead;
  members: BundleMember[] | undefined;
  areMembersLoading?: boolean;
  dependencies?: BundleDependency[];
  /** Buttons for the header's actions slot (Share, Move…). */
  actions?: React.ReactNode;
  /** Opens another bundle in place of this one — a dependency is read the same
   * way this bundle is, not in a second window stacked on it. */
  onOpenBundle?: (bundleId: string) => void;
};

const BundleDetail = ({
  bundle,
  members,
  areMembersLoading,
  dependencies,
  actions,
  onOpenBundle,
}: BundleDetailProps) => {
  const { t, i18n } = useTranslation("common");
  const theme = useTheme();
  const dateLocale = useDateFnsLocale();
  const getMetadataValueTranslation = useGetMetadataValueTranslation();
  const [tab, setTab] = useState<TabId>("summary");
  const { memberLayers } = useBundleMemberLayers(members);

  // Dependencies earn a tab only where there are some: most bundles depend on
  // nothing, and an always-present empty tab is a dead end to click.
  const tabs = useMemo(() => {
    const list: DetailTab<TabId>[] = [
      { id: "summary", label: t("summary") },
      { id: "layers", label: t("layers"), count: members?.length },
    ];
    if (dependencies?.length) {
      list.push({ id: "dependencies", label: t("bundle_dependencies"), count: dependencies.length });
    }
    return list;
  }, [t, members?.length, dependencies?.length]);

  const typeLabel = getMetadataValueTranslation("type", bundle.bundle_type);

  /** The same vocabulary a dataset's sidebar uses, restricted to what
   * describes a whole acquisition — plus the status rows, which are a bundle's
   * alone. */
  const fields: (MetaField | false | undefined)[] = [
    { icon: METADATA_HEADER_ICONS.type, label: t("metadata.headings.type"), value: typeLabel },
    ...bundleStatusRows(bundle, t, i18n).map((row) => ({
      icon: row.icon,
      label: row.heading,
      value: row.value,
    })),
    {
      icon: ICON_NAME.CALENDAR,
      label: t("created_at"),
      value: bundle.created_at ? format(new Date(bundle.created_at), "P", { locale: dateLocale }) : undefined,
    },
    {
      icon: METADATA_HEADER_ICONS.geographical_code,
      label: t("metadata.headings.geographical_code"),
      value: getMetadataValueTranslation(
        "geographical_code",
        bundle.dataset_metadata?.geographical_code ?? ""
      ),
    },
    {
      icon: ICON_NAME.CALENDAR,
      label: t("metadata.headings.data_reference_year"),
      value: bundle.dataset_metadata?.data_reference_year,
    },
    // Attribution rides with the licence, as it does on a dataset: it is a
    // condition of the licence, not a fact about the data.
    (!!bundle.dataset_metadata?.license || !!bundle.dataset_metadata?.attribution) && {
      icon: METADATA_HEADER_ICONS.license,
      label: t("metadata.headings.license"),
      value: (
        <Stack spacing={1}>
          {!!bundle.dataset_metadata?.license && (
            <span>{getMetadataValueTranslation("license", bundle.dataset_metadata.license)}</span>
          )}
          <LicenseNotices attribution={bundle.dataset_metadata?.attribution} />
        </Stack>
      ),
    },
    {
      icon: METADATA_HEADER_ICONS.distributor_name,
      label: t("metadata.headings.distributor_name"),
      value: bundle.dataset_metadata?.distributor_name,
    },
    {
      icon: ICON_NAME.EMAIL,
      label: t("metadata.headings.distributor_email"),
      value: bundle.dataset_metadata?.distributor_email ? (
        <Link
          href={`mailto:${bundle.dataset_metadata.distributor_email}`}
          target="_blank"
          rel="noopener noreferrer">
          {bundle.dataset_metadata.distributor_email}
        </Link>
      ) : undefined,
    },
    {
      icon: ICON_NAME.EXTERNAL_LINK,
      label: t("metadata.headings.distribution_url"),
      value: bundle.dataset_metadata?.distribution_url ? (
        <Link href={bundle.dataset_metadata.distribution_url} target="_blank" rel="noopener noreferrer">
          {bundle.dataset_metadata.distribution_url}
        </Link>
      ) : undefined,
    },
  ];

  return (
    <>
      <DetailHeader
        size="compact"
        title={bundle.name}
        badge={{ label: typeLabel || t("bundle"), color: BUNDLE_ACCENT }}
        actions={actions}
      />

      <DetailTabs<TabId> tabs={tabs} active={tab} onChange={setTab} />

      <Stack direction={{ xs: "column", md: "row" }} spacing={6} alignItems="flex-start">
        <Stack spacing={4} sx={{ flex: 1, minWidth: 0, alignSelf: "stretch" }}>
          {tab === "summary" && (
            <>
              <Box>
                <SectionHeading>{t("metadata.headings.description")}</SectionHeading>
                {bundle.description ? (
                  <MarkdownProse>{bundle.description}</MarkdownProse>
                ) : (
                  <Typography variant="body2" color="text.secondary">
                    {t("no_description")}
                  </Typography>
                )}
                {/* Under the description, as a dataset shows it: lineage is
                    prose about where the data came from, not a field. */}
                {!!bundle.dataset_metadata?.lineage && (
                  <Box sx={{ mt: 4.5, pt: 4, borderTop: `1px solid ${theme.palette.divider}` }}>
                    <Typography
                      sx={{
                        fontSize: 13,
                        fontWeight: 600,
                        color: theme.palette.text.secondary,
                        mb: 2.5,
                      }}>
                      {t("metadata.headings.lineage")}
                    </Typography>
                    <MarkdownProse>{bundle.dataset_metadata.lineage}</MarkdownProse>
                  </Box>
                )}
              </Box>

              <MapFrame>
                <BundleMapPreview layers={memberLayers ?? []} />
              </MapFrame>
            </>
          )}

          {tab === "layers" && <BundleLayers members={members} isLoading={areMembersLoading} />}

          {tab === "dependencies" && (
            <BundleDependencies dependencies={dependencies} onOpenBundle={onOpenBundle} />
          )}
        </Stack>

        {tab === "summary" && <MetaSidebar fields={fields} flat />}
      </Stack>
    </>
  );
};

export default BundleDetail;
