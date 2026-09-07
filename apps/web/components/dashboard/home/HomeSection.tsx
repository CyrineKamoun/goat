"use client";

import { Box, Typography } from "@mui/material";
import type { SxProps, Theme } from "@mui/material";
import type { ReactNode } from "react";

import CountPill from "@/components/dashboard/common/CountPill";

interface HomeSectionProps {
  title: string;
  /** Rendered as a `CountPill` beside the title when given. */
  count?: number;
  /** A "See all" link or similar, right-aligned beside the title. */
  action?: ReactNode;
  children: ReactNode;
  sx?: SxProps<Theme>;
}

/** The one section header every Home band uses (title, optional count,
 * optional trailing action), so the checklist, "jump back in", recent
 * datasets, teams and blog bands cannot drift on spacing or type scale. */
const HomeSection = ({ title, count, action, children, sx }: HomeSectionProps) => (
  <Box component="section" sx={sx}>
    <Box
      sx={{
        display: "flex",
        alignItems: "center",
        justifyContent: "space-between",
        gap: "10px",
        mb: "14px",
      }}>
      <Box sx={{ display: "flex", alignItems: "center", gap: "8px", minWidth: 0 }}>
        <Typography component="h2" sx={{ fontSize: 16, fontWeight: 700 }}>
          {title}
        </Typography>
        {count !== undefined && <CountPill>{count}</CountPill>}
      </Box>
      {action}
    </Box>
    {children}
  </Box>
);

export default HomeSection;
