import {
  Box,
  CardMedia,
  FormControl,
  Grid,
  InputLabel,
  MenuItem,
  Paper,
  Select,
  Stack,
  Typography,
  useTheme,
} from "@mui/material";
import { useState } from "react";
import { useTranslation } from "react-i18next";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import type { CatalogDatasetGrouped, CatalogLayerSummary, Layer } from "@/lib/validations/layer";
import { datasetMetadataAggregated } from "@/lib/validations/layer";
import { formatCatalogGroupName, formatCatalogLayerName } from "@/lib/utils/catalog-labels";
import { parseCatalogXmlMetadata } from "@/lib/utils/catalog-xml-metadata";

import { useGetMetadataValueTranslation } from "@/hooks/map/DatasetHooks";

export const METADATA_HEADER_ICONS = {
  type: ICON_NAME.LAYERS,
  data_category: ICON_NAME.DATA_CATEGORY,
  distributor_name: ICON_NAME.ORGANIZATION,
  geographical_code: ICON_NAME.GLOBE,
  language_code: ICON_NAME.LANGUAGE,
  license: ICON_NAME.LICENSE,
};

/** Normalise a raw catalog API response (grouped or plain layer) into the
 *  shape CatalogDatasetCard needs internally. */
function toGrouped(dataset: CatalogDatasetGrouped | Layer): CatalogDatasetGrouped {
  if ("layers" in dataset && Array.isArray(dataset.layers)) {
    return dataset as CatalogDatasetGrouped;
  }
  // Plain Layer — wrap it so the card can use a uniform interface.
  const layer = dataset as Layer;
  return {
    ...layer,
    layers: [
      {
        id: layer.id,
        name: layer.name,
        type: layer.type ?? "feature",
        feature_layer_geometry_type: layer.feature_layer_geometry_type,
      },
    ],
  } as CatalogDatasetGrouped;
}

const CatalogDatasetCard = ({
  dataset: rawDataset,
  onClick,
  selected,
  showFileSelect = false,
}: {
  dataset: CatalogDatasetGrouped | Layer;
  onClick?: (dataset: CatalogDatasetGrouped, selectedLayer: CatalogLayerSummary) => void;
  selected?: boolean;
  showFileSelect?: boolean;
}) => {
  const dataset = toGrouped(rawDataset);
  const theme = useTheme();
  const { t } = useTranslation(["common"]);
  const getMetadataValueTranslation = useGetMetadataValueTranslation();
  const xmlMeta = parseCatalogXmlMetadata(dataset.xml_metadata);
  const description = xmlMeta?.abstract || dataset.description || t("common:no_description");
  const bboxLabel = xmlMeta?.bbox
    ? `${xmlMeta.bbox.west.toFixed(3)}, ${xmlMeta.bbox.south.toFixed(3)} / ${xmlMeta.bbox.east.toFixed(3)}, ${xmlMeta.bbox.north.toFixed(3)}`
    : null;

  const multipleFiles = dataset.layers.length > 1;
  const [selectedLayerId, setSelectedLayerId] = useState<string>(dataset.layers[0]?.id ?? "");
  const selectedLayer = dataset.layers.find((l) => l.id === selectedLayerId) ?? dataset.layers[0];

  const handleCardClick = () => {
    if (onClick && selectedLayer) {
      onClick(dataset, selectedLayer);
    }
  };

  return (
    <Paper
      onClick={multipleFiles ? undefined : handleCardClick}
      sx={{
        overflow: "hidden",
        "&:hover": {
          cursor: multipleFiles ? "default" : "pointer",
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
              <Typography
                variant="h6"
                fontWeight="bold"
                onClick={multipleFiles ? handleCardClick : undefined}
                sx={multipleFiles ? { cursor: "pointer", "&:hover": { textDecoration: "underline" } } : undefined}>
                {formatCatalogGroupName(dataset.name)}
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
                  {description}
                </Typography>
              </Box>
            </Stack>
            {(bboxLabel || (xmlMeta?.keywords?.length ?? 0) > 0) && (
              <Stack spacing={1}>
                {bboxLabel && (
                  <Stack direction="row" spacing={1} alignItems="center">
                    <Icon iconName={ICON_NAME.GLOBE} style={{ fontSize: 14 }} htmlColor={theme.palette.text.secondary} />
                    <Typography variant="caption" color="text.secondary">
                      {bboxLabel}
                    </Typography>
                  </Stack>
                )}
                {(xmlMeta?.keywords?.length ?? 0) > 0 && (
                  <Typography variant="caption" color="text.secondary">
                    {xmlMeta?.keywords.slice(0, 4).join(", ")}
                  </Typography>
                )}
              </Stack>
            )}
            {multipleFiles && showFileSelect && (
              <FormControl size="small" onClick={(e) => e.stopPropagation()}>
                <InputLabel id={`file-select-label-${dataset.id}`}>{t("common:select_file")}</InputLabel>
                <Select
                  labelId={`file-select-label-${dataset.id}`}
                  value={selectedLayerId}
                  label={t("common:select_file")}
                  onChange={(e) => setSelectedLayerId(e.target.value)}
                  onClick={(e) => e.stopPropagation()}
                  onMouseDown={(e) => e.stopPropagation()}>
                  {dataset.layers.map((layer) => (
                    <MenuItem key={layer.id} value={layer.id} onClick={() => onClick?.(dataset, layer)}>
                      {formatCatalogLayerName(layer.name)}
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>
            )}
            <Grid container justifyContent="flex-start" sx={{ pl: 0 }}>
              {Object.keys(datasetMetadataAggregated.shape).map((key, index) => {
                return (
                  <Grid
                    item
                    {...(index < Object.keys(datasetMetadataAggregated.shape).length - 1 && {
                      xs: 12,
                      sm: 6,
                      md: 4,
                      lg: 3,
                    })}
                    key={key}
                    sx={{ pl: 0 }}>
                    <Stack
                      direction="row"
                      width="100%"
                      alignItems="center"
                      justifyContent="start"
                      sx={{ py: 2, pr: 2 }}
                      spacing={2}>
                      <Icon
                        iconName={METADATA_HEADER_ICONS[key]}
                        style={{ fontSize: 14 }}
                        htmlColor={theme.palette.text.secondary}
                      />
                      <Typography variant="body2" fontWeight="bold">
                        {getMetadataValueTranslation(key, dataset[key])}
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
