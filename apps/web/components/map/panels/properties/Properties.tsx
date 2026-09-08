import { Divider, Stack, Tooltip, Typography } from "@mui/material";
import { format, formatDistance } from "date-fns";
import { useTranslation } from "react-i18next";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import { useDateFnsLocale } from "@/i18n/utils";

import type { ProjectLayer } from "@/lib/validations/project";

import { isCatalogLayer } from "@/lib/utils/catalog-layer";
import { datasetUpdatedAt } from "@/lib/utils/datasetDates";

import DatasetSummary from "@/components/dashboard/dataset/DatasetSummary";

/** One labelled row, in the same three parts as every field above it. */
const Field = ({ label, children }: { label: string; children: React.ReactNode }) => (
  <Stack spacing={1}>
    <Typography variant="caption">{label}</Typography>
    <Divider />
    {children}
  </Stack>
);

const PropertiesPanel = ({ activeLayer }: { activeLayer: ProjectLayer }) => {
  const { t } = useTranslation("common");
  const dateLocale = useDateFnsLocale();
  const updatedAt = datasetUpdatedAt(activeLayer);
  return (
    <>
      <Stack spacing={4} sx={{ p: 2 }}>
        <DatasetSummary dataset={activeLayer} hideEmpty={true} hideMainSection={true} />

        {/* Where the layer came from. The tab already carries the publisher and
            the licence; what was missing is that this is someone else's dataset.
            Deliberately not a link to the catalog entry: the mirror is rebuilt
            wholesale on every sync, so a promoted layer outlives the entry it
            came from and the link would eventually 404. */}
        {isCatalogLayer(activeLayer) && (
          <Field label={t("source")}>
            <Stack direction="row" spacing={1} alignItems="center">
              <Icon iconName={ICON_NAME.GLOBE} style={{ fontSize: 13 }} htmlColor="inherit" />
              <Typography variant="body2">{t("catalog")}</Typography>
            </Stack>
          </Field>
        )}

        {updatedAt && (
          <Field label={t("last_updated")}>
            {/* The date itself, not "12 days ago": this is provenance, and a
                relative distance is only easy to read for something recent.
                The relative form stays, one hover away. */}
            <Tooltip
              title={formatDistance(new Date(updatedAt), new Date(), {
                addSuffix: true,
                locale: dateLocale,
              })}
              placement="bottom-start"
              arrow>
              <Typography variant="body2" sx={{ width: "fit-content" }}>
                {format(new Date(updatedAt), "PP", { locale: dateLocale })}
              </Typography>
            </Tooltip>
          </Field>
        )}
      </Stack>
    </>
  );
};

export default PropertiesPanel;
