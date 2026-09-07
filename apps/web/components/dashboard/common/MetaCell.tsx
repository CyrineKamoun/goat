"use client";

import { Stack, Typography, useTheme } from "@mui/material";
import type { SxProps, Theme } from "@mui/material";

import type { ICON_NAME } from "@p4b/ui/components/Icon";
import { Icon } from "@p4b/ui/components/Icon";

interface MetaCellProps {
  icon: ICON_NAME;
  label: string;
  sx?: SxProps<Theme>;
}

/** One labelled icon + value pair: a cell of a card's meta grid, and the
 * space/date line of a feed card. The full label is also the cell's `title`,
 * so a truncated value can still be read. */
const MetaCell = ({ icon, label, sx }: MetaCellProps) => {
  const theme = useTheme();

  return (
    <Stack direction="row" spacing={1.5} alignItems="center" title={label} sx={{ minWidth: 0, ...sx }}>
      <Icon iconName={icon} style={{ fontSize: 12 }} htmlColor={theme.palette.text.secondary} />
      <Typography variant="caption" color="text.secondary" noWrap sx={{ minWidth: 0 }}>
        {label}
      </Typography>
    </Stack>
  );
};

export default MetaCell;
