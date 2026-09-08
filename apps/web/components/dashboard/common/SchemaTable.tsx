"use client";

import { Box, Button, Typography, useTheme } from "@mui/material";
import { useState } from "react";
import { useTranslation } from "react-i18next";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

/** A dataset's columns: name, storage type, and description where published. */

/** One column of a dataset, as either the catalog (STAC `table:columns`) or a
 * layer's field list describes it. */
export type SchemaColumn = {
  name: string;
  type?: string | null;
  description?: string | null;
};

/** Rows shown before the list is collapsed behind a toggle. */
const COLLAPSED_ROWS = 15;

const SchemaTable = ({ columns }: { columns: SchemaColumn[] }) => {
  const { t } = useTranslation("common");
  const theme = useTheme();
  const [expanded, setExpanded] = useState(false);

  const collapsible = columns.length > COLLAPSED_ROWS;
  const shown = collapsible && !expanded ? columns.slice(0, COLLAPSED_ROWS) : columns;

  const withDescription = columns.some((column) => !!column.description);
  // Without descriptions the two remaining columns still stay left, against a filler track — a type stranded at the far edge of the card reads as a different row.
  const template = withDescription ? "200px 110px minmax(320px, 1fr)" : "minmax(200px, 360px) 140px 1fr";
  const minWidth = withDescription ? 692 : undefined;

  const cell = {
    display: "grid",
    gridTemplateColumns: template,
    gap: 3.5,
    minWidth,
    px: 3.5,
  } as const;

  return (
    <Box
      sx={{
        border: `1px solid ${theme.palette.divider}`,
        borderRadius: 2,
        overflowX: "auto",
        overflowY: "hidden",
      }}>
      <Box
        sx={{
          ...cell,
          py: 2.5,
          backgroundColor: theme.palette.action.hover,
          borderBottom: `1px solid ${theme.palette.divider}`,
          // The sample table's header sets the pattern for both tables on this
          // tab: a column name, in the text's own case.
          fontSize: 13,
          fontWeight: 600,
          color: theme.palette.text.secondary,
        }}>
        <Box>{t("catalog_column_name")}</Box>
        <Box>{t("catalog_column_type")}</Box>
        {withDescription && <Box>{t("catalog_column_description")}</Box>}
      </Box>
      {shown.map((column, index) => (
        <Box
          key={column.name}
          sx={{
            ...cell,
            py: 2.75,
            alignItems: "baseline",
            // The toggle below carries its own top border, so the last row keeps
            // one only when it really is the end of the list.
            borderBottom:
              index < shown.length - 1 || collapsible ? `1px solid ${theme.palette.divider}` : "none",
          }}>
          <Typography sx={{ fontSize: 13, fontWeight: 600, fontFamily: "ui-monospace, monospace" }}>
            {column.name}
          </Typography>
          <Typography
            sx={{
              fontSize: 12,
              fontFamily: "ui-monospace, monospace",
              color: theme.palette.text.secondary,
            }}>
            {column.type || "—"}
          </Typography>
          {withDescription && (
            <Typography sx={{ fontSize: 12.5, color: theme.palette.text.secondary }}>
              {column.description}
            </Typography>
          )}
        </Box>
      ))}
      {collapsible && (
        <Button
          onClick={() => setExpanded((open) => !open)}
          // `text`, explicitly: the theme's default variant is `contained`, which would weigh more than the table this toggle sits under.
          variant="text"
          color="primary"
          // Full width and flush: the row it replaces, so the list ends on the
          // same rhythm it was read in.
          sx={{
            width: "100%",
            justifyContent: "flex-start",
            px: 3.5,
            py: 2,
            borderRadius: 0,
            fontSize: 12.5,
            fontWeight: 600,
            textTransform: "none",
            "&:hover": { backgroundColor: theme.palette.action.hover },
          }}
          endIcon={
            <Icon
              iconName={expanded ? ICON_NAME.CHEVRON_UP : ICON_NAME.CHEVRON_DOWN}
              style={{ fontSize: 11 }}
            />
          }>
          {expanded ? t("show_less") : t("catalog_show_all_columns", { count: columns.length })}
        </Button>
      )}
    </Box>
  );
};

export default SchemaTable;
