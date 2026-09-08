"use client";

import { Box, Link, Typography, alpha, useTheme } from "@mui/material";
import { useTranslation } from "react-i18next";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import { DOCS_URL } from "@/lib/constants";

/** Not on the docs site — the video lives on the product's own channel. */
const HELP_VIDEO_URL = "https://www.youtube.com/@plan4better";

interface HelpStripProps {
  /** Below `md`, the three tiles stack one to a row instead of three across. */
  mobile: boolean;
}

/**
 * H12: three onboarding tiles under the quick actions — the docs home page
 * (framed as "getting started" and as "documentation") and a short video.
 * Shown in New and Getting started only; Established drops it, since the
 * header's own docs icon is the entry point from then on.
 */
const HelpStrip = ({ mobile }: HelpStripProps) => {
  const { t, i18n } = useTranslation("common");
  const theme = useTheme();
  const lng = i18n.language === "de" ? "/de" : "";
  const docsUrl = `${DOCS_URL}${lng}`;

  const tiles: { key: string; icon: ICON_NAME; href: string; meta: string }[] = [
    { key: "help_getting_started", icon: ICON_NAME.ROCKET, href: docsUrl, meta: t("min_read", { n: 6 }) },
    { key: "help_docs", icon: ICON_NAME.BOOK, href: docsUrl, meta: t("help_docs_sub") },
    { key: "help_video", icon: ICON_NAME.PLAY, href: HELP_VIDEO_URL, meta: t("video") },
  ];

  return (
    <Box
      sx={{
        mt: "26px",
        pt: "20px",
        borderTop: `1px solid ${theme.palette.divider}`,
        display: "grid",
        gridTemplateColumns: mobile ? "1fr" : "repeat(3, 1fr)",
        gap: "10px",
      }}>
      {tiles.map((tile) => (
        <Link
          key={tile.key}
          href={tile.href}
          target="_blank"
          rel="noopener noreferrer"
          underline="none"
          sx={{
            display: "flex",
            alignItems: "center",
            gap: "10px",
            padding: "9px 11px",
            borderRadius: "10px",
            color: "inherit",
            "&:hover": { backgroundColor: theme.palette.action.hover },
          }}>
          <Box
            sx={{
              flexShrink: 0,
              width: 32,
              height: 32,
              borderRadius: "9px",
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              backgroundColor: alpha(theme.palette.primary.main, 0.12),
            }}>
            <Icon iconName={tile.icon} style={{ fontSize: 15, color: theme.palette.primary.main }} />
          </Box>
          <Box sx={{ minWidth: 0, flex: 1 }}>
            <Typography component="div" noWrap sx={{ fontSize: 13.5, fontWeight: 700 }}>
              {t(tile.key)}
            </Typography>
            <Typography
              component="div"
              noWrap
              sx={{ fontSize: 11.5, fontWeight: 600, color: theme.palette.text.secondary }}>
              {tile.meta}
            </Typography>
          </Box>
          <Icon
            iconName={ICON_NAME.EXTERNAL_LINK}
            style={{ fontSize: 11, color: theme.palette.text.secondary, flexShrink: 0 }}
          />
        </Link>
      ))}
    </Box>
  );
};

export default HelpStrip;
