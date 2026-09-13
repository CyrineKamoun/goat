import { Box, Stack, Typography, useTheme } from "@mui/material";
import React from "react";

import type { MarkKind } from "@/lib/catalog/kind";

import MarkBlock from "@/components/dashboard/common/MarkBlock";

/**
 * One thing inside a bundle, written the way the content feed writes it: the
 * kind's tinted `MarkBlock`, the name, and a quieter line beneath it.
 *
 * Shared by the two tabs that list a bundle's contents — its member layers and
 * the bundles it depends on — so a layer looks like a layer and a bundle looks
 * like a bundle, both the way they look on the datasets page.
 *
 * Not `ContentRow` itself, which wants a whole `ContentItem`, its space, its
 * selection state and a kebab menu. A member carries a name, a type and a role;
 * a dependency rather less. Standing the rest up would put invented creators
 * and dates on screen, so this takes the shared mark block and the feed's own
 * type scale and leaves the columns that need feed data out.
 */
const BundleItemRow = ({
  kind,
  geometryType,
  title,
  subtitle,
  onClick,
}: {
  kind: MarkKind;
  geometryType?: string | null;
  title: string;
  /** The feed's second line: what the thing is. */
  subtitle?: string;
  onClick?: () => void;
}) => {
  const theme = useTheme();

  return (
    <Stack
      direction="row"
      spacing={3}
      alignItems="center"
      onClick={onClick}
      sx={{
        px: 3,
        py: 2,
        borderRadius: 2,
        border: `1px solid ${theme.palette.divider}`,
        cursor: onClick ? "pointer" : "default",
        "&:hover": onClick ? { backgroundColor: theme.palette.action.hover } : undefined,
      }}>
      <MarkBlock kind={kind} geometryType={geometryType} />
      <Box sx={{ minWidth: 0 }}>
        <Typography component="div" noWrap sx={{ fontSize: 13.5, fontWeight: 700 }} title={title}>
          {title}
        </Typography>
        {subtitle && (
          <Typography component="div" noWrap sx={{ fontSize: 11.5, color: theme.palette.text.secondary }}>
            {subtitle}
          </Typography>
        )}
      </Box>
    </Stack>
  );
};

export default BundleItemRow;
