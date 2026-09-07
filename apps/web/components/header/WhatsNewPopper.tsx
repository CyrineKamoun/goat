"use client";

import { Badge, Box, Button, IconButton, Stack, Tooltip, Typography, useTheme } from "@mui/material";
import { useState } from "react";
import { useTranslation } from "react-i18next";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import { patchPreferences, usePreferences } from "@/lib/api/preferences";
import { RELEASES_FEED_URL, releasesIndexUrl, unreadCount, useReleases } from "@/lib/api/releases";
import type { ReleaseEntry, ReleaseTag } from "@/lib/validations/home";

import { ArrowPopper } from "@/components/ArrowPoper";
import HeaderPopoverPaper, { HEADER_POPOVER_PLACEMENT } from "@/components/header/HeaderPopoverPaper";

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

/** H7's header surface for release notes: replaces the old hardcoded card
 * with the real feed. Renders nothing when no feed URL is configured. */
export default function WhatsNewPopper() {
  const { t, i18n } = useTranslation("common");
  const theme = useTheme();
  const [open, setOpen] = useState(false);
  const locale = i18n.language === "de" ? "de" : "en";

  const { entries } = useReleases(locale);
  const { preferences, mutate } = usePreferences();

  if (!RELEASES_FEED_URL) return null;

  const unread = unreadCount(entries, preferences?.releases_seen_at ?? null);

  const handleToggle = () => {
    const next = !open;
    setOpen(next);
    if (next) {
      void patchPreferences({ releases_seen_at: new Date().toISOString() }).then(() => mutate());
    }
  };

  return (
    <ArrowPopper
      open={open}
      onClose={() => setOpen(false)}
      placement={HEADER_POPOVER_PLACEMENT}
      arrow={false}
      content={
        <HeaderPopoverPaper sx={{ maxHeight: 420 }}>
          <Box sx={{ p: 2 }}>
            <Stack direction="row" alignItems="center" spacing={1} sx={{ px: 1, pb: 1.5 }}>
              <Icon
                iconName={ICON_NAME.ROCKET}
                style={{ fontSize: 16 }}
                htmlColor={theme.palette.primary.main}
              />
              <Typography variant="body1" fontWeight="bold">
                {t("whats_new")}
              </Typography>
            </Stack>
            <Stack spacing={0.5} sx={{ maxHeight: 300, overflowY: "auto" }}>
              {entries.map((entry) => (
                <ReleaseListItem key={entry.id} entry={entry} />
              ))}
            </Stack>
            <Button
              fullWidth
              variant="text"
              size="small"
              component="a"
              href={releasesIndexUrl(RELEASES_FEED_URL)}
              target="_blank"
              rel="noopener noreferrer"
              sx={{ mt: 1.5 }}
              endIcon={<Icon iconName={ICON_NAME.EXTERNAL_LINK} style={{ fontSize: 12 }} />}>
              {t("view_all_updates")}
            </Button>
          </Box>
        </HeaderPopoverPaper>
      }>
      <Tooltip title={t("whats_new")}>
        <IconButton size="small" onClick={handleToggle}>
          <Badge color="error" badgeContent={unread} max={9} invisible={unread === 0}>
            <Icon iconName={ICON_NAME.ROCKET} fontSize="inherit" />
          </Badge>
        </IconButton>
      </Tooltip>
    </ArrowPopper>
  );
}

const ReleaseListItem = ({ entry }: { entry: ReleaseEntry }) => {
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
        p: "8px 10px",
        borderRadius: "8px",
        textDecoration: "none",
        color: "inherit",
        "&:hover": { backgroundColor: theme.palette.action.hover },
      }}>
      <Stack direction="row" alignItems="center" spacing={1} sx={{ mb: 0.5 }}>
        <Typography
          component="span"
          sx={{
            fontSize: 11,
            fontWeight: 700,
            color: `${theme.palette[TAG_COLOR[entry.tag]].main}`,
          }}>
          {t(TAG_LABEL_KEY[entry.tag])}
        </Typography>
        <Typography component="span" variant="caption" color="text.secondary">
          {entry.date}
        </Typography>
      </Stack>
      <Typography variant="body2" fontWeight="bold" gutterBottom>
        {entry.title}
      </Typography>
      <Typography
        variant="caption"
        color="text.secondary"
        sx={{
          display: "-webkit-box",
          WebkitLineClamp: 2,
          WebkitBoxOrient: "vertical",
          overflow: "hidden",
        }}>
        {entry.summary}
      </Typography>
    </Box>
  );
};
