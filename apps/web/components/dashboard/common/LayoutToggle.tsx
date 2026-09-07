"use client";

import { Box, ButtonBase, alpha, useTheme } from "@mui/material";
import { useTranslation } from "react-i18next";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

export type ContentLayoutValue = "grid" | "list";

interface LayoutToggleProps {
  value: ContentLayoutValue;
  onChange: (value: ContentLayoutValue) => void;
  /** Phone layout: each segment drops its visible label and keeps only its
   * glyph — the label still reaches assistive tech through `aria-label`. */
  compact?: boolean;
}

/** The Grid/List segmented control both pages carry: one pill holding two
 * 32px segments, the active one filled with the primary tint. */
const LayoutToggle = ({ value, onChange, compact }: LayoutToggleProps) => {
  const { t } = useTranslation("common");
  const theme = useTheme();

  const segment = (id: ContentLayoutValue, icon: ICON_NAME, label: string, ariaLabel: string) => {
    const on = value === id;
    return (
      <ButtonBase
        onClick={() => onChange(id)}
        aria-label={ariaLabel}
        sx={{
          display: "inline-flex",
          alignItems: "center",
          justifyContent: "center",
          gap: "6px",
          height: 32,
          px: compact ? "12px" : "13px",
          borderRadius: "999px",
          backgroundColor: on ? alpha(theme.palette.primary.main, 0.12) : "transparent",
          color: on ? theme.palette.primary.main : theme.palette.text.secondary,
          fontSize: 13.5,
          fontWeight: 600,
        }}>
        <Icon
          iconName={icon}
          style={{
            fontSize: 16,
            color: on ? theme.palette.primary.main : theme.palette.text.secondary,
          }}
        />
        {!compact && label}
      </ButtonBase>
    );
  };

  return (
    <Box
      sx={{
        display: "flex",
        gap: "2px",
        padding: "3px",
        flexShrink: 0,
        borderRadius: "999px",
        border: `1px solid ${theme.palette.divider}`,
        backgroundColor: theme.palette.background.paper,
      }}>
      {segment("grid", ICON_NAME.GRIP_HORIZONTAL, t("grid"), t("grid_view"))}
      {segment("list", ICON_NAME.LIST, t("list"), t("list_view"))}
    </Box>
  );
};

export default LayoutToggle;
