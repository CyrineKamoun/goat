"use client";

import {
  Box,
  Breadcrumbs,
  Chip,
  Divider,
  Link as MuiLink,
  Paper,
  Stack,
  Typography,
} from "@mui/material";
import NextLink from "next/link";
import { useRouter } from "next/navigation";
import { useMemo } from "react";
import { useTranslation } from "react-i18next";

import type { CatalogDatasetGrouped, CatalogLayerSummary } from "@/lib/validations/layer";

interface DatasetGroupOverviewProps {
  group: CatalogDatasetGrouped;
}

export default function DatasetGroupOverview({ group }: DatasetGroupOverviewProps) {
  const { t } = useTranslation("common");
  const router = useRouter();

  // Extract OGC Record properties from record_jsonb
  const record = group.record_jsonb as Record<string, unknown> | undefined;
  const props = record?.properties as Record<string, unknown> | undefined;

  const title = (props?.title as string) || group.name;
  const description = (props?.description as string) || "";
  const license = (props?.license as string) || group.license || "";
  const publisher =
    ((props?.publisher as Record<string, unknown>)?.name as string) ||
    group.distributor_name ||
    "";
  const language = (props?.language as string) || group.language_code || "";
  const category = group.data_category || "";

  // Temporal extent year
  const temporalInterval = (
    (props?.extent as Record<string, unknown>)?.temporal as Record<string, unknown>
  )?.interval as unknown[][] | undefined;
  const year = temporalInterval?.[0]?.[0] as number | undefined;

  // Unique types and geometry types across layers
  const layerSummary = useMemo(() => {
    const types = new Set<string>();
    const geomTypes = new Set<string>();
    for (const layer of group.layers) {
      if (layer.type) types.add(layer.type);
      if (layer.feature_layer_geometry_type) geomTypes.add(layer.feature_layer_geometry_type);
    }
    return {
      types: Array.from(types),
      geomTypes: Array.from(geomTypes),
    };
  }, [group.layers]);

  const handleOpenLayer = (layer: CatalogLayerSummary) => {
    router.push(`/datasets/${layer.id}?group=${group.package_id}`);
  };

  return (
    <Box>
      {/* Breadcrumb */}
      <Breadcrumbs sx={{ mb: 4 }}>
        <MuiLink component={NextLink} href="/catalog" underline="hover" color="inherit">
          {t("catalog")}
        </MuiLink>
        <Typography color="text.primary">{title}</Typography>
      </Breadcrumbs>

      <Paper elevation={3} sx={{ p: 4 }}>
        {/* Title */}
        <Typography variant="h5" fontWeight="bold" gutterBottom>
          {title}
        </Typography>

        {/* Metadata badges */}
        <Stack direction="row" spacing={1} flexWrap="wrap" useFlexGap sx={{ mb: 2 }}>
          {category && (
            <Chip label={category} size="small" color="primary" variant="outlined" />
          )}
          {layerSummary.types.map((t) => (
            <Chip key={t} label={t} size="small" color="info" variant="outlined" />
          ))}
          {layerSummary.geomTypes.map((g) => (
            <Chip key={g} label={g} size="small" color="secondary" variant="outlined" />
          ))}
          {license && <Chip label={license} size="small" variant="outlined" />}
          {publisher && <Chip label={publisher} size="small" variant="outlined" />}
          {language && <Chip label={language} size="small" variant="outlined" />}
          {year && <Chip label={String(year)} size="small" variant="outlined" />}
        </Stack>


        {/* Description */}
        {description && (
          <Typography variant="body2" color="text.secondary" sx={{ mb: 3, whiteSpace: "pre-line" }}>
            {description}
          </Typography>
        )}

        <Divider sx={{ my: 3 }} />

        {/* Layer list header with type summary */}
        <Typography variant="subtitle1" fontWeight="bold" sx={{ mb: 2 }}>
          {group.layers.length} {t("layers")}
        </Typography>

        <Stack spacing={0} divider={<Divider />}>
          {group.layers.map((layer) => (
            <Box
              key={layer.id}
              onClick={() => handleOpenLayer(layer)}
              sx={{
                display: "flex",
                alignItems: "center",
                justifyContent: "space-between",
                py: 1.5,
                px: 1,
                cursor: "pointer",
                "&:hover": { bgcolor: "action.hover" },
                borderRadius: 1,
              }}>
              <Stack direction="row" spacing={1} alignItems="center">
                <Typography variant="body2" fontWeight={500}>
                  {layer.name}
                </Typography>
                <Chip label={layer.type} size="small" color="info" variant="outlined" sx={{ fontSize: "0.7rem" }} />
                {layer.feature_layer_geometry_type && (
                  <Chip
                    label={layer.feature_layer_geometry_type}
                    size="small"
                    color="secondary"
                    variant="outlined"
                    sx={{ fontSize: "0.7rem" }}
                  />
                )}
              </Stack>
            </Box>
          ))}
        </Stack>
      </Paper>
    </Box>
  );
}
