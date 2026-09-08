"use client";

import { Box, Typography, useTheme } from "@mui/material";
import { useMemo } from "react";
import { useTranslation } from "react-i18next";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import type { CatalogColumn } from "@/lib/validations/catalog";
import type { DatasetCollectionItems } from "@/lib/validations/layer";

import type { FeatureTableField } from "@/components/common/FeatureTable";
import FeatureTableFrame from "@/components/dashboard/common/FeatureTableFrame";

/** The dataset's actual rows — the preview sample, one line per feature. */

/** Columns that are the feature's shape rather than its attributes. */
const STRUCTURAL = new Set(["geometry", "geom", "bbox"]);

const CatalogFeatureTable = ({
  features,
  columns,
  truncated,
}: {
  /** A geometry-less dataset's rows arrive as Features with a `null` geometry;
   * this table reads attributes either way. */
  features: GeoJSON.Feature<GeoJSON.Geometry | null>[];
  columns: CatalogColumn[];
  /** Whether the dataset holds more features than these — the preview's `goat:truncated`. */
  truncated?: boolean;
}) => {
  const { t } = useTranslation("common");
  const theme = useTheme();

  /** Declared columns first, then anything the data carries beyond them. */
  const fields = useMemo<FeatureTableField[]>(() => {
    const declared = columns.filter((column) => !!column.name && !STRUCTURAL.has(column.name));
    const seen = new Set(declared.map((column) => column.name));
    const extra: FeatureTableField[] = [];
    for (const feature of features) {
      for (const key of Object.keys(feature.properties ?? {})) {
        if (!seen.has(key)) {
          seen.add(key);
          // A value nobody declared is still a value.
          extra.push({ name: key, type: "string" });
        }
      }
    }
    return [...declared.map((column) => ({ name: column.name, type: column.type ?? "string" })), ...extra];
  }, [columns, features]);

  /** The sample as the table's own page shape. */
  const data = useMemo<DatasetCollectionItems>(
    () => ({
      type: "FeatureCollection",
      title: "",
      links: [],
      numberMatched: features.length,
      numberReturned: features.length,
      features: features.map((feature, index) => ({
        type: "Feature",
        // Positional: preview features carry no id, and the table only needs a
        // stable key per row.
        id: index,
        properties: (feature.properties ?? {}) as Record<string, unknown>,
      })),
    }),
    [features]
  );

  if (!fields.length || !features.length) return null;

  return (
    <FeatureTableFrame
      fields={fields}
      data={data}
      /* Why the table stops where it does, in the footer of the frame it applies to rather than as a sentence underneath it. */
      footer={
        truncated ? (
          <Box sx={{ px: 3, py: 2 }}>
            <Typography
              variant="caption"
              color="text.secondary"
              sx={{ display: "flex", alignItems: "center", gap: 1.5 }}>
              <Icon
                iconName={ICON_NAME.TABLE}
                style={{ fontSize: 11 }}
                htmlColor={theme.palette.text.secondary}
              />
              {t("catalog_preview_limited", { count: features.length })}
            </Typography>
          </Box>
        ) : undefined
      }
    />
  );
};

export default CatalogFeatureTable;
