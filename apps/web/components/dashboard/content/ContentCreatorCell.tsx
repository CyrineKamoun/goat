"use client";

import { Box, Tooltip, Typography, useTheme } from "@mui/material";
import type { SxProps, Theme } from "@mui/material";
import type { ReactNode } from "react";
import { useTranslation } from "react-i18next";

import { useUserProfile } from "@/lib/api/users";
import type { ContentCreator } from "@/lib/validations/content";

import UserAvatar from "@/components/dashboard/common/UserAvatar";

interface ContentCreatorCellProps {
  creator: ContentCreator | null | undefined;
  /** Render the name beside the avatar (a list column). Off, the name lives
   * in the tooltip only (a card's meta row). */
  showName?: boolean;
  /** Stands in for a creating account that is not a person — a seeded
   * template, whose source is GOAT itself. Given, a creatorless item renders
   * the same row with this name and avatar; left unset, it renders nothing,
   * which is what every other caller relies on. */
  fallbackName?: string;
  fallbackAvatar?: ReactNode;
  sx?: SxProps<Theme>;
}

/** Who created an item: their avatar, and — inline or in the tooltip — their
 * name, "You" for the caller. Renders nothing when the creating account is
 * gone and the caller offers no stand-in, so a card's meta row simply starts
 * at the time. */
const ContentCreatorCell = ({
  creator,
  showName,
  fallbackName,
  fallbackAvatar,
  sx,
}: ContentCreatorCellProps) => {
  const { t } = useTranslation("common");
  const theme = useTheme();
  const { userProfile } = useUserProfile();

  if (!creator && !fallbackName) return null;

  const name = creator ? creator.name : (fallbackName ?? "");
  const isMe = !!creator && !!userProfile?.id && userProfile.id === creator.id;
  const label = isMe ? t("you") : name;
  const tooltip = isMe ? t("created_by_you") : t("created_by_name", { name });

  return (
    <Tooltip title={tooltip} placement="top" disableInteractive>
      <Box
        sx={{ display: "flex", alignItems: "center", gap: "7px", minWidth: 0, overflow: "hidden", ...sx }}
        aria-label={tooltip}>
        {creator ? <UserAvatar id={creator.id} name={creator.name} src={creator.avatar} /> : fallbackAvatar}
        {showName && (
          <Typography
            component="div"
            noWrap
            sx={{ fontSize: 12.5, color: theme.palette.text.secondary, minWidth: 0 }}>
            {label}
          </Typography>
        )}
      </Box>
    </Tooltip>
  );
};

export default ContentCreatorCell;
