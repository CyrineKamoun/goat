"use client";

import { Box, ButtonBase, alpha, useTheme } from "@mui/material";
import { useTranslation } from "react-i18next";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import ToolPill from "@/components/dashboard/common/ToolPill";

interface ContentActionBarProps {
  count: number;
  onClear: () => void;
  canShare: boolean;
  canMove: boolean;
  canDelete: boolean;
  onShare: () => void;
  onMove: () => void;
  onDelete: () => void;
  detailsOpen: boolean;
  onToggleDetails: () => void;
}

/** Replaces the toolbar's normal right side once one or more items are
 * selected: a count with a way to clear it, then the bulk actions gated by
 * whether every selected item allows them. Download is always disabled —
 * bulk download isn't wired up yet, this is a placeholder per the design. */
const ContentActionBar = ({
  count,
  onClear,
  canShare,
  canMove,
  canDelete,
  onShare,
  onMove,
  onDelete,
  detailsOpen,
  onToggleDetails,
}: ContentActionBarProps) => {
  const { t } = useTranslation("common");
  const theme = useTheme();

  return (
    <Box sx={{ display: "flex", alignItems: "center", gap: "8px", flexWrap: "wrap" }}>
      <ButtonBase
        onClick={onClear}
        aria-label={t("clear")}
        sx={{
          display: "inline-flex",
          alignItems: "center",
          gap: "7px",
          height: 38,
          px: "14px",
          borderRadius: "999px",
          border: `1.5px solid ${theme.palette.primary.main}`,
          backgroundColor: alpha(theme.palette.primary.main, 0.12),
          color: theme.palette.primary.main,
          fontSize: 13,
          fontWeight: 800,
          flexShrink: 0,
        }}>
        <Icon iconName={ICON_NAME.CLOSE} style={{ fontSize: 13, color: theme.palette.primary.main }} />
        {t("n_selected", { count })}
      </ButtonBase>

      <ToolPill icon={ICON_NAME.SHARE} label={t("share")} disabled={!canShare} onClick={onShare} />
      <ToolPill icon={ICON_NAME.FOLDER} label={t("move_to")} disabled={!canMove} onClick={onMove} />
      <ToolPill icon={ICON_NAME.DOWNLOAD} label={t("download")} disabled />
      <ToolPill
        icon={ICON_NAME.TRASH}
        label={t("delete")}
        danger
        disabled={!canDelete}
        onClick={onDelete}
      />
      <ToolPill
        icon={ICON_NAME.INFO}
        label={t("details")}
        active={detailsOpen}
        onClick={onToggleDetails}
      />
    </Box>
  );
};

export default ContentActionBar;
