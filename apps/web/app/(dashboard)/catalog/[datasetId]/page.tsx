"use client";

import { Box, Button, Container, Divider, Paper, Skeleton, Stack, Table, TableBody, TableCell, TableHead, TableRow, Tab, Tabs, Typography } from "@mui/material";
import { useRouter } from "next/navigation";
import { useMemo, useState } from "react";
import { useTranslation } from "react-i18next";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import {
  useCatalogDatasetDetail,
  useCatalogDatasetMapPreview,
  useCatalogDatasetSample,
} from "@/lib/api/layers";

import { CustomTabPanel, a11yProps } from "@/components/common/CustomTabPanel";
import { METADATA_HEADER_ICONS } from "@/components/dashboard/catalog/CatalogDatasetCard";
import DatasetMapPreview from "@/components/dashboard/dataset/DatasetMapPreview";

type NormalizedBbox = {
  west: number;
  south: number;
  east: number;
  north: number;
};

type CatalogDetailPageParams = {
  params: {
    datasetId: string;
  };
};

const fieldRows = (
  rows: Array<{ label: string; value?: string | null }>,
  emptyLabel: string
) => {
  return rows.map((row) => (
    <Stack key={row.label} spacing={1}>
      <Typography variant="caption">{row.label}</Typography>
      <Divider />
      <Typography variant="body2" color={row.value ? "text.primary" : "text.secondary"}>
        {row.value || emptyLabel}
      </Typography>
    </Stack>
  ));
};

const asText = (value: unknown): string | undefined => {
  if (value === null || value === undefined) {
    return undefined;
  }
  const text = String(value).trim();
  return text.length > 0 && text !== "-" ? text : undefined;
};

const resolveValue = (...candidates: unknown[]): string | undefined => {
  for (const candidate of candidates) {
    const normalized = asText(candidate);
    if (normalized) {
      return normalized;
    }
  }
  return undefined;
};

const normalizeBbox = (bbox: unknown): NormalizedBbox | null => {
  if (!bbox || typeof bbox !== "object") {
    return null;
  }
  const value = bbox as Record<string, unknown>;
  const west = value.west;
  const south = value.south;
  const east = value.east;
  const north = value.north;
  if (
    [west, south, east, north].every((coordinate) => typeof coordinate === "number")
  ) {
    return {
      west: west as number,
      south: south as number,
      east: east as number,
      north: north as number,
    };
  }
  return null;
};

const toOsmEmbedUrl = (bbox: NormalizedBbox): string => {
  const lonPad = Math.max((bbox.east - bbox.west) * 0.12, 0.08);
  const latPad = Math.max((bbox.north - bbox.south) * 0.12, 0.08);
  const minLon = Math.max(-180, bbox.west - lonPad);
  const minLat = Math.max(-90, bbox.south - latPad);
  const maxLon = Math.min(180, bbox.east + lonPad);
  const maxLat = Math.min(90, bbox.north + latPad);
  const query = new URLSearchParams({
    bbox: `${minLon},${minLat},${maxLon},${maxLat}`,
    layer: "mapnik",
  });
  return `https://www.openstreetmap.org/export/embed.html?${query.toString()}`;
};

const getOsmViewport = (bbox: NormalizedBbox) => {
  const lonPad = Math.max((bbox.east - bbox.west) * 0.12, 0.08);
  const latPad = Math.max((bbox.north - bbox.south) * 0.12, 0.08);
  const minLon = Math.max(-180, bbox.west - lonPad);
  const minLat = Math.max(-90, bbox.south - latPad);
  const maxLon = Math.min(180, bbox.east + lonPad);
  const maxLat = Math.min(90, bbox.north + latPad);

  const lonSpan = Math.max(maxLon - minLon, 0.0001);
  const latSpan = Math.max(maxLat - minLat, 0.0001);

  const left = ((bbox.west - minLon) / lonSpan) * 100;
  const width = ((bbox.east - bbox.west) / lonSpan) * 100;
  const top = ((maxLat - bbox.north) / latSpan) * 100;
  const height = ((bbox.north - bbox.south) / latSpan) * 100;

  return {
    url: toOsmEmbedUrl(bbox),
    overlay: {
      left: `${Math.max(0, Math.min(100, left))}%`,
      top: `${Math.max(0, Math.min(100, top))}%`,
      width: `${Math.max(1, Math.min(100, width))}%`,
      height: `${Math.max(1, Math.min(100, height))}%`,
    },
  };
};

const BboxPreview = ({ bbox, emptyLabel }: { bbox: NormalizedBbox | null; emptyLabel: string }) => {
  const viewport = bbox ? getOsmViewport(bbox) : null;

  return (
    <Box
      sx={{
        position: "relative",
        height: 220,
        border: (theme) => `1px solid ${theme.palette.divider}`,
        borderRadius: 1,
        overflow: "hidden",
        backgroundColor: "grey.100",
      }}>
        {bbox ? (
          <Box
            component="iframe"
            title="Bounding box map preview"
            src={viewport?.url}
            loading="lazy"
            sx={{
              width: "100%",
              height: "100%",
              border: 0,
            }}
          />
        ) : (
          <Stack alignItems="center" justifyContent="center" sx={{ height: "100%" }}>
            <Typography variant="body2" color="text.secondary" sx={{ fontSize: 13 }}>
              {emptyLabel}
            </Typography>
          </Stack>
        )}
        {bbox && viewport && (
          <Box
            sx={{
              pointerEvents: "none",
              position: "absolute",
              left: viewport.overlay.left,
              top: viewport.overlay.top,
              width: viewport.overlay.width,
              height: viewport.overlay.height,
              border: "2px solid #1d4ed8",
              backgroundColor: "rgba(37, 99, 235, 0.16)",
              boxShadow: "0 0 0 1px rgba(255,255,255,0.7) inset",
            }}
          />
        )}
    </Box>
  );
};

export default function CatalogDetailPage({ params: { datasetId } }: CatalogDetailPageParams) {
  const router = useRouter();
  const { t } = useTranslation("common");
  const [tabValue, setTabValue] = useState(0);
  const emptyValueDisplay = "-";

  const { detail, isLoading: isDetailLoading } = useCatalogDatasetDetail(datasetId);
  const { sample, isLoading: isSampleLoading } = useCatalogDatasetSample(datasetId, 25);
  const { mapPreview, isLoading: isMapPreviewLoading } = useCatalogDatasetMapPreview(datasetId);

  const dataset = detail?.dataset;
  const summary = detail?.metadata?.summary as Record<string, unknown> | undefined;
  const csw = detail?.metadata?.csw as Record<string, unknown> | undefined;
  const contacts = Array.isArray(csw?.contacts)
    ? (csw?.contacts as Array<Record<string, unknown>>)
    : [];
  const responsibleOrganization =
    (summary?.distributor_name as string | undefined) ||
    (contacts.find((entry) => typeof entry.organization === "string")?.organization as
      | string
      | undefined) ||
    (dataset?.distributor_name as string | undefined);
  const keywords = Array.isArray(csw?.keywords)
    ? csw.keywords.map((keyword) => String(keyword)).filter(Boolean)
    : [];
  const normalizedBbox = normalizeBbox(csw?.bbox);
  const filterPanelRows = [
    {
      key: "type",
      label: "Type",
      value: resolveValue(csw?.resource_type, dataset?.type, "dataset"),
    },
    {
      key: "data_category",
      label: "Data Category",
      value: resolveValue(csw?.topic_category, dataset?.data_category),
    },
    {
      key: "geographical_code",
      label: "Region",
      value: resolveValue(csw?.geographical_code, summary?.geographical_code, dataset?.geographical_code),
    },
    {
      key: "language_code",
      label: "Language",
      value: resolveValue(csw?.language, summary?.language_code, dataset?.language_code),
    },
    {
      key: "distributor_name",
      label: "Distributor Name",
      value: resolveValue(summary?.distributor_name, responsibleOrganization, dataset?.distributor_name),
    },
    {
      key: "license",
      label: "License",
      value: resolveValue(csw?.license, summary?.license, dataset?.license),
    },
  ] as const;

  const summaryRows = [
    {
      label: "Title",
      value: resolveValue(csw?.title, dataset?.name),
    },
    {
      label: "Abstract / Description",
      value: resolveValue(csw?.abstract, summary?.description, dataset?.description),
    },
    {
      label: "Resource type",
      value: resolveValue(csw?.resource_type, dataset?.type, "dataset"),
    },
    {
      label: "Last updated",
      value: resolveValue(csw?.updated_at, summary?.updated_at, dataset?.updated_at),
    },
    {
      label: "Keywords",
      value: keywords.length > 0 ? keywords.join(", ") : undefined,
    },
    {
      label: "CRS",
      value: resolveValue(csw?.crs),
    },
    {
      label: "Region code",
      value: resolveValue(csw?.geographical_code, summary?.geographical_code, dataset?.geographical_code),
    },
    {
      label: "Attribution",
      value: resolveValue(csw?.attribution, dataset?.attribution),
    },
    {
      label: "Distribution URL",
      value: resolveValue(csw?.distribution_url, dataset?.distribution_url),
    },
  ];
  const previewDataset = useMemo(() => {
    if (!dataset) {
      return undefined;
    }
    return {
      ...dataset,
      id: mapPreview?.collection_id || dataset.id,
    };
  }, [dataset, mapPreview?.collection_id]);

  const hasDataTab = useMemo(() => {
    return dataset?.type === "table" || dataset?.type === "feature";
  }, [dataset?.type]);

  const hasMapTab = useMemo(() => {
    return dataset?.type === "feature" || dataset?.type === "raster";
  }, [dataset?.type]);

  const tabs = useMemo(() => {
    const entries = [{ label: t("summary"), value: "summary" }];
    if (hasDataTab) {
      entries.push({ label: t("data"), value: "data" });
    }
    if (hasMapTab) {
      entries.push({ label: t("map"), value: "map" });
    }
    return entries;
  }, [hasDataTab, hasMapTab, t]);

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

      {isDetailLoading && <Skeleton variant="rectangular" width="100%" height={600} />}

      {!isDetailLoading && !dataset && (
        <Paper elevation={3} sx={{ p: 6 }}>
          <Typography variant="h6">Catalog dataset not found.</Typography>
        </Paper>
      )}

      {!isDetailLoading && dataset && (
        <Paper elevation={3} sx={{ p: 4 }}>
          <Typography variant="h6" fontWeight="bold">
            {dataset.name}
          </Typography>

          <Box sx={{ width: "100%", mt: 8 }}>
            <Box sx={{ borderBottom: 1, borderColor: "divider" }}>
              <Tabs value={tabValue} scrollButtons onChange={(_event, value) => setTabValue(value)}>
                {tabs.map((item) => (
                  <Tab key={item.value} label={item.label} {...a11yProps(item.value)} />
                ))}
              </Tabs>
            </Box>

            {tabs.map((item, index) => (
              <CustomTabPanel key={item.value} value={tabValue} index={index}>
                {item.value === "summary" && (
                  <Box
                    sx={{
                      display: "grid",
                      gap: 4,
                      gridTemplateColumns: {
                        xs: "1fr",
                        md: "minmax(0, 2fr) minmax(260px, 1fr)",
                      },
                    }}>
                    <Stack spacing={4}>{fieldRows(summaryRows, emptyValueDisplay)}</Stack>
                    <Stack spacing={3}>
                      <BboxPreview bbox={normalizedBbox} emptyLabel={emptyValueDisplay} />
                      <Paper variant="outlined" sx={{ p: 2 }}>
                        <Stack spacing={2}>
                          {filterPanelRows.map((row) => {
                            const iconName = METADATA_HEADER_ICONS[row.key] || ICON_NAME.INFO;
                            return (
                              <Stack key={row.key} direction="row" spacing={1.5} alignItems="flex-start">
                                <Icon
                                  iconName={iconName}
                                  style={{ fontSize: 14, marginTop: 2 }}
                                  htmlColor="rgba(0, 0, 0, 0.56)"
                                />
                                <Stack spacing={0.25}>
                                  <Typography variant="body2" color="text.secondary" sx={{ fontSize: 13, lineHeight: 1.2 }}>
                                    {row.label}
                                  </Typography>
                                  <Typography
                                    variant="body1"
                                    component="div"
                                    sx={{ fontSize: 16, fontWeight: 600, lineHeight: 1.25 }}>
                                    {row.value || emptyValueDisplay}
                                  </Typography>
                                </Stack>
                              </Stack>
                            );
                          })}
                        </Stack>
                      </Paper>
                    </Stack>
                  </Box>
                )}

                {item.value === "data" && (
                  <Stack spacing={3}>
                    {!isSampleLoading && sample && (
                      <Typography variant="caption" color="text.secondary">
                        Status: {sample.status} | Returned: {sample.returned} | Total rows: {sample.row_count}
                      </Typography>
                    )}
                    {isSampleLoading && <Skeleton variant="rectangular" height={280} />}
                    {!isSampleLoading && sample?.message && (
                      <Typography variant="body2" color="text.secondary">
                        {sample.message}
                      </Typography>
                    )}
                    {!isSampleLoading && sample && sample.rows.length > 0 && (
                      <Box sx={{ overflowX: "auto" }}>
                        <Table size="small" stickyHeader>
                          <TableHead>
                            <TableRow>
                              {sample.columns.map((column) => (
                                <TableCell key={column.name}>
                                  <Stack direction="column" spacing={0.5}>
                                    <Typography variant="body2" fontWeight="bold">
                                      {column.name}
                                    </Typography>
                                    {column.type && (
                                      <Typography variant="caption" color="text.secondary">
                                        {column.type}
                                      </Typography>
                                    )}
                                  </Stack>
                                </TableCell>
                              ))}
                            </TableRow>
                          </TableHead>
                          <TableBody>
                            {sample.rows.map((row, rowIndex) => (
                              <TableRow key={`${datasetId}-${rowIndex}`}>
                                {sample.columns.map((column) => (
                                  <TableCell key={`${rowIndex}-${column.name}`}>
                                    <Typography variant="body2">
                                      {row[column.name] === null || row[column.name] === undefined
                                        ? ""
                                        : typeof row[column.name] === "object"
                                          ? JSON.stringify(row[column.name])
                                          : String(row[column.name])}
                                    </Typography>
                                  </TableCell>
                                ))}
                              </TableRow>
                            ))}
                          </TableBody>
                        </Table>
                      </Box>
                    )}
                    {!isSampleLoading && sample && sample.rows.length === 0 && !sample.message && (
                      <Typography variant="body2" color="text.secondary">
                        No sample rows available for this dataset.
                      </Typography>
                    )}
                  </Stack>
                )}

                {item.value === "map" && (
                  <Stack spacing={3}>
                    {!isMapPreviewLoading && mapPreview && (
                      <Typography variant="caption" color="text.secondary">
                        Status: {mapPreview.status}
                        {mapPreview.collection_id ? ` | Collection: ${mapPreview.collection_id}` : ""}
                      </Typography>
                    )}
                    {isMapPreviewLoading && <Skeleton variant="rectangular" height={320} />}
                    {!isMapPreviewLoading && mapPreview?.status !== "ready" && (
                      <Typography variant="body2" color="text.secondary">
                        Map preview unavailable until processing is ready.
                      </Typography>
                    )}
                    {!isMapPreviewLoading && mapPreview?.status === "ready" && !mapPreview.collection_id && (
                      <Typography variant="body2" color="text.secondary">
                        Map preview is marked ready but no collection id was provided.
                      </Typography>
                    )}
                    {!isMapPreviewLoading && mapPreview?.status === "ready" && mapPreview.collection_id && previewDataset && (
                      <DatasetMapPreview dataset={previewDataset} />
                    )}
                  </Stack>
                )}
              </CustomTabPanel>
            ))}
          </Box>
        </Paper>
      )}
    </Container>
  );
}
