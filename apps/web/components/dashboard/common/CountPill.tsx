"use client";

import { Box, useTheme } from "@mui/material";
import type { ReactNode } from "react";

interface CountPillProps {
  children: ReactNode;
  /** The thing the count belongs to is the selected tab / open section — the
   * pill takes the primary accent instead of the neutral tint. */
  active?: boolean;
}

/** The small count chip that follows a label: a tab's dataset count, a feed
 * section's item count, a tool pill's active-filter count. */
const CountPill = ({ children, active }: CountPillProps) => {
  const theme = useTheme();

  return (
    <Box
      component="span"
      sx={{
        display: "inline-flex",
        alignItems: "center",
        flexShrink: 0,
        fontSize: 11,
        fontWeight: 700,
        letterSpacing: 0.2,
        lineHeight: 1.5,
        padding: "1px 8px",
        borderRadius: "999px",
        backgroundColor: active ? theme.palette.action.selected : theme.palette.action.hover,
        color: active ? theme.palette.primary.main : theme.palette.text.secondary,
      }}>
      {children}
    </Box>
  );
};

export default CountPill;
