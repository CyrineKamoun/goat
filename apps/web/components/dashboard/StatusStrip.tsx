"use client";

import { Box, IconButton, Link, Stack, Tooltip, Typography, alpha, useTheme } from "@mui/material";
import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import { useStatusFeed } from "@/lib/api/status";
import type { StatusFeed } from "@/lib/validations/home";
import { localized } from "@/lib/validations/home";

const DISMISSED_KEY = "goat.status.dismissed";

type Incident = StatusFeed["incidents"][number];
type Tone = "warning" | "error";

/**
 * The status site's incident `severity` is "outage" | "disrupted" | "notice"
 * (see plan4better/status `SEVERITIES`). "outage" is the error tier;
 * "disrupted" is the warning tier the strip labels `status_disrupted`.
 * "notice" incidents don't affect the strip.
 */
const toneForSeverity = (severity: string): Tone | null => {
  const s = severity.toLowerCase();
  if (s.includes("outage")) return "error";
  if (s.includes("disrupt")) return "warning";
  return null;
};

const readDismissedId = (): string | null => {
  if (typeof window === "undefined") return null;
  try {
    return window.localStorage.getItem(DISMISSED_KEY);
  } catch {
    return null;
  }
};

const writeDismissedId = (id: string): void => {
  if (typeof window === "undefined") return;
  try {
    window.localStorage.setItem(DISMISSED_KEY, id);
  } catch {
    // Storage refused the write: the incident reopens next visit, which is
    // no worse than not dismissing it at all.
  }
};

/** The most severe active incident that is disrupted- or outage-tier, or
 * `null` when there is none — the strip renders nothing in that case. */
const worstIncident = (incidents: Incident[]): { incident: Incident; tone: Tone } | null => {
  const candidates = incidents
    .map((incident) => ({ incident, tone: toneForSeverity(incident.severity) }))
    .filter((c): c is { incident: Incident; tone: Tone } => c.tone !== null);
  return candidates.find((c) => c.tone === "error") ?? candidates.find((c) => c.tone === "warning") ?? null;
};

/**
 * H8's app-wide banner: shown under the header on every dashboard page (not
 * the map) while an incident is disrupted- or outage-tier, until
 * dismissed for that incident's id — a new incident id reopens it. Renders
 * nothing when no feed URL is configured, when everything is operational,
 * or when the worst incident's id is the one stored as dismissed.
 */
const StatusStrip = () => {
  const { t, i18n } = useTranslation("common");
  const theme = useTheme();
  const { status } = useStatusFeed();
  const [dismissedId, setDismissedId] = useState<string | null>(null);

  useEffect(() => {
    setDismissedId(readDismissedId());
  }, []);

  if (!status) return null;

  const worst = worstIncident(status.incidents);
  if (!worst || worst.incident.id === dismissedId) return null;

  const locale = i18n.language === "de" ? "de" : "en";
  const color = theme.palette[worst.tone].main;
  const levelKey = worst.tone === "error" ? "status_outage" : "status_disrupted";

  const dismiss = () => {
    writeDismissedId(worst.incident.id);
    setDismissedId(worst.incident.id);
  };

  return (
    <Stack
      direction="row"
      alignItems="center"
      spacing={1.5}
      sx={{ height: "34px", px: 2, flexShrink: 0, backgroundColor: alpha(color, 0.12) }}>
      <Box sx={{ width: 8, height: 8, borderRadius: "50%", backgroundColor: color, flexShrink: 0 }} />
      <Typography
        sx={{
          fontSize: 11.5,
          fontWeight: 800,
          letterSpacing: 0.4,
          textTransform: "uppercase",
          color,
          flexShrink: 0,
        }}>
        {t(levelKey)}
      </Typography>
      <Typography variant="body2" noWrap sx={{ flexGrow: 1 }}>
        {localized(worst.incident.title, locale)}
      </Typography>
      <Link
        href={status.url}
        target="_blank"
        rel="noreferrer"
        variant="body2"
        sx={{ flexShrink: 0, fontWeight: 600 }}>
        {t("details")}
      </Link>
      <Tooltip title={t("hide_until_next_update")}>
        <IconButton size="small" aria-label={t("hide_until_next_update")} onClick={dismiss}>
          <Icon iconName={ICON_NAME.CLOSE} fontSize="inherit" />
        </IconButton>
      </Tooltip>
    </Stack>
  );
};

export default StatusStrip;
