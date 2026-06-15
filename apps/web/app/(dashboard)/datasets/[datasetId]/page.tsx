"use client";

import {
  Box,
  Breadcrumbs,
  Button,
  Container,
  Link as MuiLink,
  Paper,
  Skeleton,
  Stack,
  Tab,
  Tabs,
  Typography,
} from "@mui/material";
import NextLink from "next/link";
import { useRouter, useSearchParams } from "next/navigation";
import { useEffect, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import { useCatalogGroup, useDataset } from "@/lib/api/layers";

import { CustomTabPanel, a11yProps } from "@/components/common/CustomTabPanel";
import DatasetGroupOverview from "@/components/dashboard/dataset/DatasetGroupOverview";
import DatasetMapPreview from "@/components/dashboard/dataset/DatasetMapPreview";
import DatasetSummary from "@/components/dashboard/dataset/DatasetSummary";
import DatasetTable from "@/components/dashboard/dataset/DatasetTable";
import LayerSwitcherDropdown from "@/components/dashboard/dataset/LayerSwitcherDropdown";

export default function DatasetDetailPage({ params: { datasetId } }) {
  const router = useRouter();
  const searchParams = useSearchParams();
  const { t } = useTranslation("common");
  const groupParam = searchParams?.get("group") ?? null;

  // Fetch as a group first. The OGC endpoint resolves both group_ids and layer_ids
  // to the same Feature. Treat as a group only when the resolved feature id
  // matches the requested datasetId (i.e. the user actually asked for the group).
  const { group: maybeGroup, isLoading: isGroupLoading } = useCatalogGroup(datasetId);
  const isGroup =
    !!maybeGroup && maybeGroup.layers.length > 1 && maybeGroup.package_id === datasetId;

  // Fetch group context for the breadcrumb / sibling switcher when ?group= is present.
  const { group: contextGroup } = useCatalogGroup(groupParam || undefined);

  // Single layer fetch — only when this id is not itself a group.
  const { dataset, isLoading: isDatasetLoading } = useDataset(isGroup ? "" : datasetId);

  const [tab, setTab] = useState(0);

  const tabItems = useMemo(() => {
    const items = [{ label: t("summary"), value: "summary" }];
    if (dataset?.type === "table" || dataset?.type === "feature") {
      items.push({ label: t("data"), value: "data" });
    }
    if (dataset?.type === "feature" || dataset?.type === "raster") {
      items.push({ label: t("map"), value: "map" });
    }
    return items;
  }, [dataset?.type, t]);

  // Reset tab when switching between layers/groups so we don't land on a stale index
  useEffect(() => {
    setTab(0);
  }, [datasetId]);

  return (
    <Container sx={{ py: 10, px: 10 }} maxWidth="xl">
      <Box sx={{ display: "flex", justifyContent: "space-between", alignItems: "center", mb: 4 }}>
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

      {isGroupLoading && <Skeleton variant="rectangular" width="100%" height={600} />}

      {!isGroupLoading && isGroup && maybeGroup && (
        <DatasetGroupOverview group={maybeGroup} />
      )}

      {!isGroupLoading && !isGroup && (
        <>
          {isDatasetLoading && <Skeleton variant="rectangular" width="100%" height={600} />}
          {!isDatasetLoading && dataset && (
            <Box>
              {contextGroup && (
                <Breadcrumbs sx={{ mb: 3 }}>
                  <MuiLink component={NextLink} href="/catalog" underline="hover" color="inherit">
                    {t("catalog")}
                  </MuiLink>
                  <MuiLink
                    component={NextLink}
                    href={`/datasets/${contextGroup.package_id}`}
                    underline="hover"
                    color="inherit">
                    {contextGroup.name}
                  </MuiLink>
                  <Typography color="text.primary">{dataset.name}</Typography>
                </Breadcrumbs>
              )}

              <Paper elevation={3} sx={{ p: 4 }}>
                <Stack
                  direction={{ xs: "column", sm: "row" }}
                  justifyContent="space-between"
                  alignItems={{ xs: "flex-start", sm: "center" }}
                  spacing={2}>
                  <Typography variant="h6" fontWeight="bold">
                    {dataset.name}
                  </Typography>
                  {contextGroup && contextGroup.layers.length > 1 && (
                    <LayerSwitcherDropdown
                      currentLayerId={datasetId}
                      layers={contextGroup.layers}
                      groupId={contextGroup.package_id}
                    />
                  )}
                </Stack>

                <Box sx={{ width: "100%", mt: 8 }}>
                  <Box sx={{ borderBottom: 1, borderColor: "divider" }}>
                    <Tabs value={tab} scrollButtons onChange={(_e, v) => setTab(v)}>
                      {tabItems.map((item) => (
                        <Tab key={item.value} label={item.label} {...a11yProps(item.value)} />
                      ))}
                    </Tabs>
                  </Box>
                  {tabItems.map((item) => (
                    <CustomTabPanel
                      key={item.value}
                      value={tab}
                      index={tabItems.findIndex((i) => i.value === item.value)}>
                      {item.value === "summary" && <DatasetSummary dataset={dataset} />}
                      {item.value === "data" && <DatasetTable dataset={dataset} />}
                      {item.value === "map" && <DatasetMapPreview dataset={dataset} />}
                    </CustomTabPanel>
                  ))}
                </Box>
              </Paper>
            </Box>
          )}
        </>
      )}
    </Container>
  );
}
