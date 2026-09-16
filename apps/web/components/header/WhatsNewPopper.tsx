"use client";

import { Badge, Box, Button, IconButton, Stack, Tooltip, Typography, useTheme } from "@mui/material";
import { useState } from "react";
import { useTranslation } from "react-i18next";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import { FEEDS_ENABLED, changelogUrl } from "@/lib/api/feeds";
import { patchPreferences, usePreferences } from "@/lib/api/preferences";
import { unreadCount, useReleases } from "@/lib/api/releases";

import { ArrowPopper } from "@/components/ArrowPoper";
import ReleaseItem from "@/components/dashboard/home/ReleaseItem";
import HeaderPopoverPaper, { HEADER_POPOVER_PLACEMENT } from "@/components/header/HeaderPopoverPaper";

/** H7's header surface for release notes: the changelog feed as a scrollable
 * list behind the rocket, badged with the unread count. Renders nothing when
 * no website URL is configured or the feed has no entries. */
export default function WhatsNewPopper() {
  const { t, i18n } = useTranslation("common");
  const theme = useTheme();
  const [open, setOpen] = useState(false);
  const locale = i18n.language === "de" ? "de" : "en";

  const { entries } = useReleases(locale);
  const { preferences, mutate } = usePreferences();

  // No feed, or nothing in it yet (or the website unreachable): no rocket rather than an empty panel.
  if (!FEEDS_ENABLED || entries.length === 0) return null;

  // No badge until the preferences are in: the feed usually arrives first,
  // and a missing watermark would flash the full count for a moment.
  const unread = preferences ? unreadCount(entries, preferences.releases_seen_at) : 0;

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
        <HeaderPopoverPaper sx={{ maxHeight: 480, width: 360 }}>
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
            <Stack spacing={0.25} sx={{ maxHeight: 340, overflowY: "auto" }}>
              {entries.map((entry) => (
                <ReleaseItem key={entry.id} entry={entry} />
              ))}
            </Stack>
            <Button
              fullWidth
              variant="text"
              size="small"
              component="a"
              href={changelogUrl(locale)}
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
