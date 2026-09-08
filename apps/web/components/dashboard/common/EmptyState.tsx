"use client";

import { Box, Paper, Typography, alpha, useTheme } from "@mui/material";
import type { ReactNode } from "react";

import type { ICON_NAME } from "@p4b/ui/components/Icon";
import { Icon } from "@p4b/ui/components/Icon";

interface EmptyStateProps {
  icon: ICON_NAME;
  title: string;
  /** A sentence under the title saying what to do next. A node, not a string,
   * so a caller can emphasise part of it. */
  hint?: ReactNode;
  /** One button under the hint, where there is something to act on. */
  action?: ReactNode;
  /** Half-height variant for a dialog column: a 40px badge and less padding,
   * so the card does not push the rest of the panel off screen. */
  compact?: boolean;
}

/** "There is nothing here": a dashed card with a tinted round badge, a title
 * and a hint. Used wherever a page's own body comes back empty — a space or
 * folder with nothing in it, a search that matched nothing, a favourites view
 * with nothing starred. */
const EmptyState = ({ icon, title, hint, action, compact }: EmptyStateProps) => {
  const theme = useTheme();

  return (
    <Paper
      elevation={0}
      sx={{
        border: `1px dashed ${alpha(theme.palette.text.primary, 0.24)}`,
        borderRadius: "14px",
        padding: compact ? "28px 20px" : "70px 24px",
        textAlign: "center",
      }}>
      <Box
        sx={{
          width: compact ? 40 : 60,
          height: compact ? 40 : 60,
          borderRadius: "50%",
          backgroundColor: alpha(theme.palette.primary.main, 0.12),
          display: "inline-flex",
          alignItems: "center",
          justifyContent: "center",
          mb: compact ? "10px" : "14px",
        }}>
        <Icon iconName={icon} style={{ fontSize: compact ? 18 : 26, color: theme.palette.primary.main }} />
      </Box>
      <Typography component="div" sx={{ fontSize: compact ? 15 : 17, fontWeight: 700, mb: "4px" }}>
        {title}
      </Typography>
      {hint && (
        <Typography
          component="div"
          sx={{ fontSize: compact ? 13 : 13.5, color: theme.palette.text.secondary }}>
          {hint}
        </Typography>
      )}
      {action && <Box sx={{ mt: "18px" }}>{action}</Box>}
    </Paper>
  );
};

export default EmptyState;
