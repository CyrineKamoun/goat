import { Box, CardMedia, Grid, Paper, Stack, Typography, useTheme } from "@mui/material";
import { useTranslation } from "react-i18next";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import type { Layer } from "@/lib/validations/layer";
import { datasetMetadataAggregated } from "@/lib/validations/layer";

import { useGetMetadataValueTranslation } from "@/hooks/map/DatasetHooks";

export const METADATA_HEADER_ICONS = {
  type: ICON_NAME.LAYERS,
  data_category: ICON_NAME.DATA_CATEGORY,
  distributor_name: ICON_NAME.ORGANIZATION,
  geographical_code: ICON_NAME.GLOBE,
  language_code: ICON_NAME.LANGUAGE,
  license: ICON_NAME.LICENSE,
};

const CatalogDatasetCard = ({
  dataset,
  onClick,
  selected,
}: {
  dataset: Layer;
  onClick?: (dataset: Layer) => void;
  selected?: boolean;
}) => {
  const theme = useTheme();
  const { t } = useTranslation(["common"]);
  const getMetadataValueTranslation = useGetMetadataValueTranslation();

  const csw = (dataset.other_properties as Record<string, unknown> | undefined)?.csw as
    | Record<string, unknown>
    | undefined;

  const getCatalogValue = (key: string): unknown => {
    if (key === "data_category") {
      return dataset.data_category || csw?.topic_category;
    }
    if (key === "distributor_name") {
      if (dataset.distributor_name) {
        return dataset.distributor_name;
      }
      const contacts = Array.isArray(csw?.contacts) ? csw.contacts : [];
      const firstContact = contacts.find(
        (contact) =>
          contact &&
          typeof contact === "object" &&
          typeof (contact as Record<string, unknown>).organization === "string"
      ) as Record<string, unknown> | undefined;
      return firstContact?.organization;
    }
    if (key === "geographical_code") {
      return dataset.geographical_code || csw?.geographical_code;
    }
    if (key === "language_code") {
      return dataset.language_code || csw?.language;
    }
    if (key === "license") {
      return dataset.license || csw?.license;
    }
    if (key === "type") {
      return dataset.type || "feature";
    }
    return (dataset as unknown as Record<string, unknown>)[key];
  };

  const metadataKeys = Object.keys(datasetMetadataAggregated.shape) as Array<
    keyof typeof datasetMetadataAggregated.shape
  >;

  return (
    <Paper
      onClick={() => onClick && onClick(dataset)}
      sx={{
        overflow: "hidden",
        "&:hover": {
          cursor: "pointer",
          boxShadow: 10,
          "& img": {
            transform: "scale(1.2)",
          },
        },
        ...(selected && {
          backgroundColor: "rgba(43, 179, 129, 0.2)",
        }),
      }}
      elevation={3}>
      <Grid container justifyContent="flex-start" spacing={2}>
        <Grid item xs={12} sm={6} md={4} lg={3} sx={{ pl: 0 }}>
          <Box
            sx={{
              overflow: "hidden",
              height: "100%",
            }}>
            <CardMedia
              component="img"
              sx={{
                mr: 6,
                height: "100%",
                transition: "transform 300ms cubic-bezier(0.4, 0, 0.2, 1) 0ms",
                transformOrigin: "center center",
                objectFit: "cover",
                backgroundSize: "cover",
              }}
              image={dataset.thumbnail_url}
            />
          </Box>
        </Grid>
        <Grid item xs={12} sm={6} md={8} lg={9}>
          <Stack direction="column" sx={{ p: 2 }} spacing={2}>
            <Stack spacing={2}>
              <Typography variant="h6" fontWeight="bold">
                {dataset.name}
              </Typography>
              <Box
                sx={{
                  height: "60px",
                  overflow: "hidden",
                  textOverflow: "ellipsis",
                  display: "-webkit-box",
                  WebkitBoxOrient: "vertical",
                  WebkitLineClamp: 3,
                }}>
                <Typography variant="body2" color="text.secondary">
                  {dataset.description || t("common:no_description")}
                </Typography>
              </Box>
            </Stack>
            <Grid container justifyContent="flex-start" sx={{ pl: 0 }}>
              {metadataKeys.map((key) => {
                const iconName = METADATA_HEADER_ICONS[key];
                return (
                  <Grid item xs={12} sm={6} md={4} lg={3} key={key} sx={{ pl: 0 }}>
                    <Stack
                      direction="row"
                      width="100%"
                      alignItems="center"
                      justifyContent="start"
                      sx={{ py: 1.25, pr: 2 }}
                      spacing={1.5}>
                      <Icon
                        iconName={iconName}
                        style={{ fontSize: 14 }}
                        htmlColor={theme.palette.text.secondary}
                      />
                      <Typography variant="body2" fontWeight={600}>
                        {getMetadataValueTranslation(key, getCatalogValue(key))}
                      </Typography>
                    </Stack>
                  </Grid>
                );
              })}
            </Grid>
          </Stack>
        </Grid>
      </Grid>
    </Paper>
  );
};

export default CatalogDatasetCard;
