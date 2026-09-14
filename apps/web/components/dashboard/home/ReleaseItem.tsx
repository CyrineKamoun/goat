"use client";

import { Box, Stack, Typography, useTheme } from "@mui/material";
import { useTranslation } from "react-i18next";

import { type FeedLocale, formatFeedDate } from "@/lib/api/feeds";
import type { ReleaseEntry, ReleaseTag } from "@/lib/validations/home";

export const TAG_LABEL_KEY: Record<ReleaseTag, string> = {
  new: "release_new",
  improved: "release_improved",
  fixed: "release_fixed",
};

export const TAG_COLOR: Record<ReleaseTag, "primary" | "info" | "success"> = {
  new: "primary",
  improved: "info",
  fixed: "success",
};

const clamp = (lines: number) => ({
  display: "-webkit-box",
  WebkitLineClamp: lines,
  WebkitBoxOrient: "vertical" as const,
  overflow: "hidden",
  wordBreak: "break-word" as const,
});

/** "New · 14 September 2026" — the tag in its colour, the date muted. */
export const ReleaseMeta = ({ entry }: { entry: ReleaseEntry }) => {
  const { t, i18n } = useTranslation("common");
  const theme = useTheme();
  const locale: FeedLocale = i18n.language === "de" ? "de" : "en";
  return (
    <Stack direction="row" alignItems="center" spacing={1}>
      <Typography
        component="span"
        sx={{ fontSize: 11, fontWeight: 700, color: theme.palette[TAG_COLOR[entry.tag]].main }}>
        {t(TAG_LABEL_KEY[entry.tag])}
      </Typography>
      <Typography component="span" variant="caption" color="text.secondary">
        {formatFeedDate(entry.date, locale)}
      </Typography>
    </Stack>
  );
};

/**
 * One changelog entry as a link that opens the website's changelog at that
 * feature: tag and date, title, two lines of summary. No thumbnail — at list
 * size a UI screenshot is unreadable; the screenshots belong to the spotlight
 * and the changelog page itself.
 */
const ReleaseItem = ({ entry }: { entry: ReleaseEntry }) => {
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
        transition: theme.transitions.create(["background-color"], { duration: 120 }),
        "&:hover": { backgroundColor: theme.palette.action.hover },
        "&:hover .release-title": { color: theme.palette.primary.main },
      }}>
      <Stack spacing={0.5} sx={{ minWidth: 0 }}>
        <ReleaseMeta entry={entry} />
        <Typography
          className="release-title"
          sx={{
            fontSize: 13.5,
            fontWeight: 600,
            lineHeight: 1.35,
            transition: theme.transitions.create(["color"], { duration: 120 }),
            ...clamp(2),
          }}>
          {entry.title}
        </Typography>
        <Typography variant="caption" color="text.secondary" sx={clamp(2)}>
          {entry.summary}
        </Typography>
      </Stack>
    </Box>
  );
};

export default ReleaseItem;
