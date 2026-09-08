"use client";

import { Box, Stack, useTheme } from "@mui/material";
import { useTranslation } from "react-i18next";

import type { CatalogTab } from "@/hooks/catalog/useCatalogSearchState";

import CountPill from "@/components/dashboard/common/CountPill";

/** The catalog's top-level tabs: label, count pill, 3px underline on the active one. */

const CatalogTabs = ({
  active,
  onChange,
  datasetCount,
}: {
  active: CatalogTab;
  onChange: (tab: CatalogTab) => void;
  datasetCount?: number;
}) => {
  const { t, i18n } = useTranslation("common");
  const theme = useTheme();

  const tabs: { id: CatalogTab; label: string; count?: string }[] = [
    {
      id: "datasets",
      label: t("catalog_tab_datasets"),
      count: typeof datasetCount === "number" ? datasetCount.toLocaleString(i18n.language) : undefined,
    },
    { id: "templates", label: t("templates") },
  ];

  return (
    <Stack direction="row" sx={{ borderBottom: `1px solid ${theme.palette.divider}`, mb: 6, gap: 1 }}>
      {tabs.map((tab) => {
        const selected = tab.id === active;
        return (
          <Box
            component="button"
            type="button"
            key={tab.id}
            onClick={() => onChange(tab.id)}
            sx={{
              display: "flex",
              alignItems: "center",
              gap: 2.5,
              px: 1,
              py: 3,
              mr: 6,
              mb: "-1px",
              background: "transparent",
              border: "none",
              font: "inherit",
              fontSize: 16,
              fontWeight: 700,
              letterSpacing: "-0.1px",
              cursor: "pointer",
              color: selected ? theme.palette.primary.main : theme.palette.text.primary,
              borderBottom: `3px solid ${selected ? theme.palette.primary.main : "transparent"}`,
            }}>
            {tab.label}
            {tab.count !== undefined && <CountPill active={selected}>{tab.count}</CountPill>}
          </Box>
        );
      })}
    </Stack>
  );
};

export default CatalogTabs;
