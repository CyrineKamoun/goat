"use client";

import { Box, ButtonBase, Collapse, Typography, useTheme } from "@mui/material";
import type { ReactNode } from "react";
import { useTranslation } from "react-i18next";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import CountPill from "@/components/dashboard/common/CountPill";

interface ContentSectionProps {
  labelKey: string;
  count: number;
  open: boolean;
  onToggle: () => void;
  /** Rendered instead of `t(labelKey)` — for a section whose title carries a
   * value, such as "Shared with {space}". */
  label?: string;
  children: ReactNode;
}

/** One collapsible group of the feed (Folders, Shortcuts, Projects,
 * Datasets, …): a leading chevron, the label, and a count chip. Collapsing
 * hides the group's items without dropping it from the page. */
const ContentSection = ({ labelKey, count, open, onToggle, label, children }: ContentSectionProps) => {
  const { t } = useTranslation("common");
  const theme = useTheme();

  return (
    <Box>
      <ButtonBase
        onClick={onToggle}
        sx={{
          display: "flex",
          alignItems: "center",
          gap: "8px",
          mb: "12px",
          borderRadius: "6px",
          padding: "2px 6px 2px 0",
          userSelect: "none",
        }}>
        <Icon
          iconName={open ? ICON_NAME.CHEVRON_DOWN : ICON_NAME.CHEVRON_RIGHT}
          style={{ fontSize: 13, color: theme.palette.text.secondary }}
        />
        <Typography
          component="span"
          sx={{ fontSize: 16, fontWeight: 700, color: theme.palette.text.primary }}>
          {label ?? t(labelKey)}
        </Typography>
        <CountPill>{count}</CountPill>
      </ButtonBase>
      <Collapse in={open}>{children}</Collapse>
    </Box>
  );
};

export default ContentSection;
