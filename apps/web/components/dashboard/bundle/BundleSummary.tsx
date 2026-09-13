import { Stack, Typography, useTheme } from "@mui/material";
import { format } from "date-fns";
import React from "react";
import { useTranslation } from "react-i18next";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import { useDateFnsLocale } from "@/i18n/utils";

import type { BundleRead } from "@/lib/api/bundles";
import { METADATA_HEADER_ICONS } from "@/lib/constants/metadataIcons";
import { bundleStatusRows } from "@/lib/utils/bundleMeta";

import { useGetMetadataValueTranslation } from "@/hooks/map/DatasetHooks";

import BundleUpdateButton from "@/components/dashboard/bundle/BundleUpdateButton";

interface BundleSummaryProps {
  bundle: BundleRead;
}

/**
 * A bundle at a glance: what it is, where it stands, and a way to rebuild it.
 *
 * The map's bundle panel, where vertical space is scarce — so the aggregated
 * fields only, not the provenance document. That document is read in the
 * bundle's own detail dialog (`BundleDetail`), which has room to lay it out.
 */
const BundleSummary: React.FC<BundleSummaryProps> = ({ bundle }) => {
  const theme = useTheme();
  const { t, i18n } = useTranslation("common");
  const getMetadataValueTranslation = useGetMetadataValueTranslation();
  const dateLocale = useDateFnsLocale();

  // The same aggregated fields a layer summarises, minus the two that classify a
  // layer rather than a dataset (data_category, language_code). Icons, headings
  // and value translation all go through the layer helpers so the two summaries
  // cannot drift apart. `type` resolves via metadata.type.<bundle_type>.
  const aggregatedFields = ["type", "geographical_code", "distributor_name", "license"] as const;

  // Bundle-specific, so they have no aggregated-field equivalent to reuse.
  const bundleAttributes: { key: string; heading: string; value?: string; icon: ICON_NAME }[] = [
    ...bundleStatusRows(bundle, t, i18n),
    {
      key: "created_at",
      heading: t("created_at"),
      value: bundle.created_at ? format(new Date(bundle.created_at), "P", { locale: dateLocale }) : undefined,
      icon: ICON_NAME.CALENDAR,
    },
  ];

  return (
    <Stack spacing={2} sx={{ width: "100%" }}>
      {aggregatedFields.map((key) => (
        <div key={key} style={{ display: "flex", gap: "8px", alignItems: "center" }}>
          <Icon
            iconName={METADATA_HEADER_ICONS[key]}
            style={{ fontSize: 14, flexShrink: 0 }}
            htmlColor={theme.palette.text.secondary}
          />
          <div style={{ minWidth: 0 }}>
            <Typography variant="caption" noWrap>
              {i18n.exists(`common:metadata.headings.${key}`) ? t(`common:metadata.headings.${key}`) : key}
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
      <BundleUpdateButton bundle={bundle} size="small" />
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
  );
};

export default BundleSummary;
