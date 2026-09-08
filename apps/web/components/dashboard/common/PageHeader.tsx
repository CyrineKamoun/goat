"use client";

import { Box, Typography, useTheme } from "@mui/material";
import type { ReactNode } from "react";

/** The title block every dashboard page opens with: the page's name, an
 * optional line saying what it is for, and an optional control on the right.
 * One scale, so Content, Catalog and Settings read as the same product. The
 * gap below it belongs to the caller. */
const PageHeader = ({
  title,
  subtitle,
  action,
  mobile = false,
}: {
  title: string;
  subtitle?: ReactNode;
  /** Right-aligned control on the title row. */
  action?: ReactNode;
  mobile?: boolean;
}) => {
  const theme = useTheme();

  return (
    <Box>
      <Box sx={{ display: "flex", alignItems: "center", gap: 2 }}>
        <Typography
          component="h1"
          sx={{
            m: 0,
            flex: 1,
            minWidth: 0,
            fontSize: mobile ? 21 : 24,
            fontWeight: 600,
            letterSpacing: "-0.3px",
            lineHeight: 1.2,
          }}>
          {title}
        </Typography>
        {action}
      </Box>
      {subtitle && (
        <Typography
          component="p"
          sx={{
            m: 0,
            mt: "4px",
            fontSize: mobile ? 12.5 : 14,
            lineHeight: 1.5,
            color: theme.palette.text.secondary,
            maxWidth: 640,
          }}>
          {subtitle}
        </Typography>
      )}
    </Box>
  );
};

export default PageHeader;
