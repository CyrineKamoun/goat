"use client";

import {
  Box,
  Button,
  Container,
  Divider,
  Grid,
  List,
  ListItemButton,
  ListItemText,
  Paper,
  Skeleton,
  Stack,
  Typography,
  useTheme,
} from "@mui/material";
import { useRouter } from "next/navigation";
import { useEffect } from "react";
import { useTranslation } from "react-i18next";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import { useCatalogGroupedDataset } from "@/lib/api/layers";
import { formatCatalogGroupName, formatCatalogLayerName } from "@/lib/utils/catalog-labels";
import { parseCatalogXmlMetadata } from "@/lib/utils/catalog-xml-metadata";

import { useGetMetadataValueTranslation } from "@/hooks/map/DatasetHooks";

import { METADATA_HEADER_ICONS } from "@/components/dashboard/catalog/CatalogDatasetCard";

export default function DatasetGroupDetailPage({ params: { packageId } }) {
  const router = useRouter();
  const { t } = useTranslation("common");
  const theme = useTheme();
  const getMetadataValueTranslation = useGetMetadataValueTranslation();
  const { datasetGroup, isLoading } = useCatalogGroupedDataset(packageId);

  const xmlMeta = parseCatalogXmlMetadata(datasetGroup?.xml_metadata);
  const description = xmlMeta?.abstract || datasetGroup?.description || t("common:no_description");

  useEffect(() => {
    if (isLoading || !datasetGroup) {
      return;
    }

    if (datasetGroup.layers.length === 1) {
      router.replace(`/datasets/${datasetGroup.layers[0].id}`);
    }
  }, [datasetGroup, isLoading, router]);

  return (
    <Container sx={{ py: 10, px: 10 }} maxWidth="xl">
      <Box
        sx={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
          mb: 8,
        }}>
        <Button
          variant="text"
          startIcon={<Icon iconName={ICON_NAME.CHEVRON_LEFT} style={{ fontSize: 12 }} />}
          sx={{ borderRadius: 0 }}
          onClick={() => router.back()}>
          <Typography variant="body2" color="inherit">
            {t("back")}
          </Typography>
        </Button>
      </Box>

      {isLoading && <Skeleton variant="rectangular" width="100%" height={450} />}

      {!isLoading && datasetGroup && (
        <Paper elevation={3} sx={{ p: 4 }}>
          <Stack spacing={3}>
            <Typography variant="h6" fontWeight="bold">
              {formatCatalogGroupName(datasetGroup.name)}
            </Typography>

            <Typography variant="body2" color="text.secondary">
              {description}
            </Typography>

            <Grid container justifyContent="flex-start" sx={{ pl: 0 }}>
              {["type", "data_category", "distributor_name", "geographical_code", "language_code", "license"].map(
                (key, index) => {
                  return (
                    <Grid
                      item
                      {...(index < 5 && {
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
                        sx={{ py: 1.5, pr: 2 }}
                        spacing={1.5}>
                        <Icon
                          iconName={METADATA_HEADER_ICONS[key]}
                          style={{ fontSize: 14 }}
                          htmlColor={theme.palette.text.secondary}
                        />
                        <Typography variant="body2" fontWeight="bold">
                          {getMetadataValueTranslation(key, datasetGroup[key])}
                        </Typography>
                      </Stack>
                    </Grid>
                  );
                }
              )}
            </Grid>

            <Divider />

            <Stack spacing={1}>
              <Typography variant="subtitle1" fontWeight="bold">
                {t("select_file")}
              </Typography>
              <Typography variant="caption" color="text.secondary">
                {`${datasetGroup.layers.length} ${t("datasets")}`}
              </Typography>
              <List disablePadding>
                {datasetGroup.layers.map((layer) => (
                  <ListItemButton
                    key={layer.id}
                    onClick={() => router.push(`/datasets/${layer.id}`)}
                    sx={{ borderRadius: 1 }}>
                    <ListItemText
                      primary={formatCatalogLayerName(layer.name)}
                      secondary={layer.feature_layer_geometry_type || layer.type}
                    />
                  </ListItemButton>
                ))}
              </List>
            </Stack>
          </Stack>
        </Paper>
      )}
    </Container>
  );
}
