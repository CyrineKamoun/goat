"use client";

import { Box, Button, Stack, Typography, useTheme } from "@mui/material";
import { useTranslation } from "react-i18next";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import { usePreferences } from "@/lib/api/preferences";
import { RELEASES_FEED_URL, releasesIndexUrl, unreadCount, useReleases } from "@/lib/api/releases";
import type { ReleaseEntry, ReleaseTag } from "@/lib/validations/home";

import SurfaceCard from "@/components/dashboard/common/SurfaceCard";
import HomeSection from "@/components/dashboard/home/HomeSection";

const TAG_LABEL_KEY: Record<ReleaseTag, string> = {
  new: "release_new",
  improved: "release_improved",
  fixed: "release_fixed",
};

const TAG_COLOR: Record<ReleaseTag, "primary" | "info" | "success"> = {
  new: "primary",
  improved: "info",
  fixed: "success",
};

/**
 * H7's passive Home surface: the three latest release notes. Self-contained
 * so the controller can mount it unconditionally — it returns null when no
 * feed URL is configured or the feed has no entries yet.
 */
const WhatsNewCard = () => {
  const { t, i18n } = useTranslation("common");
  const locale = i18n.language === "de" ? "de" : "en";

  const { entries } = useReleases(locale);
  const { preferences } = usePreferences();

  if (!RELEASES_FEED_URL || entries.length === 0) return null;

  const latest = entries.slice(0, 3);
  const unread = unreadCount(entries, preferences?.releases_seen_at ?? null);
  const allNotesLabel = unread > 0 ? `${t("all_notes")} · ${t("n_new", { n: unread })}` : t("all_notes");

  return (
    <HomeSection
      title={t("whats_new")}
      action={
        <Button
          variant="text"
          size="small"
          component="a"
          href={releasesIndexUrl(RELEASES_FEED_URL)}
          target="_blank"
          rel="noopener noreferrer"
          endIcon={<Icon iconName={ICON_NAME.CHEVRON_RIGHT} style={{ fontSize: 12 }} />}
          sx={{ borderRadius: 0 }}>
          {allNotesLabel}
        </Button>
      }>
      <SurfaceCard hoverable={false} sx={{ p: "6px" }}>
        {latest.map((entry) => (
          <ReleaseRow key={entry.id} entry={entry} />
        ))}
      </SurfaceCard>
    </HomeSection>
  );
};

const ReleaseRow = ({ entry }: { entry: ReleaseEntry }) => {
  const { t } = useTranslation("common");
  const theme = useTheme();

  return (
    <Box
      component="a"
      href={entry.url}
      target="_blank"
      rel="noopener noreferrer"
      sx={{
        display: "block",
        p: "10px 12px",
        borderRadius: "8px",
        textDecoration: "none",
        color: "inherit",
        "&:hover": { backgroundColor: theme.palette.action.hover },
      }}>
      <Stack direction="row" alignItems="center" spacing={1} sx={{ mb: 0.5 }}>
        <Typography
          component="span"
          sx={{ fontSize: 11, fontWeight: 700, color: theme.palette[TAG_COLOR[entry.tag]].main }}>
          {t(TAG_LABEL_KEY[entry.tag])}
        </Typography>
        <Typography component="span" variant="caption" color="text.secondary">
          {entry.date}
        </Typography>
      </Stack>
      <Typography sx={{ fontSize: 13.5, fontWeight: 600 }} gutterBottom>
        {entry.title}
      </Typography>
      <Typography
        variant="caption"
        color="text.secondary"
        sx={{ display: "-webkit-box", WebkitLineClamp: 2, WebkitBoxOrient: "vertical", overflow: "hidden" }}>
        {entry.summary}
      </Typography>
    </Box>
  );
};

export default WhatsNewCard;
