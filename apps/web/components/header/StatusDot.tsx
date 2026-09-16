"use client";

import { Box, Typography, alpha, useTheme } from "@mui/material";
import { useTranslation } from "react-i18next";

import { useStatusFeed } from "@/lib/api/status";
import type { StatusLevel } from "@/lib/validations/home";

type NonOperationalLevel = Exclude<StatusLevel, "operational">;

const LEVEL_LABEL_KEY: Record<NonOperationalLevel, string> = {
  notice: "status_notice",
  maintenance: "status_maintenance",
  disrupted: "status_disrupted",
  outage: "status_outage",
};

const LEVEL_PALETTE: Record<NonOperationalLevel, "info" | "warning" | "error"> = {
  notice: "info",
  maintenance: "warning",
  disrupted: "warning",
  outage: "error",
};

/**
 * H8's header indicator for `useStatusFeed()`: nothing while `overall` is
 * operational, otherwise a coloured pill with the level label linking to the
 * status page. Renders nothing while no feed URL is configured either
 * (`useStatusFeed` then returns `status: undefined`).
 */
const StatusDot = () => {
  const { t } = useTranslation("common");
  const theme = useTheme();
  const { status } = useStatusFeed();

  if (!status) return null;

  // Nothing to say while everything is operational: a permanent grey dot is
  // noise, and the status page is one click away in Help for the curious.
  if (status.overall === "operational") return null;

  const level = status.overall;
  const color = theme.palette[LEVEL_PALETTE[level]].main;

  return (
    <Box
      component="a"
      href={status.url}
      target="_blank"
      rel="noreferrer"
      sx={{
        display: "flex",
        alignItems: "center",
        gap: "6px",
        px: "8px",
        py: "3px",
        borderRadius: "999px",
        textDecoration: "none",
        backgroundColor: alpha(color, 0.12),
      }}>
      <Box sx={{ width: 8, height: 8, borderRadius: "50%", backgroundColor: color, flexShrink: 0 }} />
      <Typography
        sx={{ fontSize: 11.5, fontWeight: 800, letterSpacing: 0.4, textTransform: "uppercase", color }}>
        {t(LEVEL_LABEL_KEY[level])}
      </Typography>
    </Box>
  );
};

export default StatusDot;
