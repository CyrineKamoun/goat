"use client";

import { ButtonBase, alpha, useTheme } from "@mui/material";
import { forwardRef } from "react";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import CountPill from "@/components/dashboard/common/CountPill";

interface ToolPillProps {
  icon: ICON_NAME;
  label: string;
  /** Rendered next to the label; hidden when the label is. Only for a pill
   * that opens something (a popover, a menu) — a pill that just toggles says
   * so through `active` instead. */
  chevron?: boolean;
  /** Filled state (primary soft) — the pill's popover/menu is open, its
   * filter narrows what is on screen, or what it toggles is on. */
  active?: boolean;
  /** A count shown after the label (active filter types, a menu's selection). */
  badge?: number;
  /** Destructive action — label and icon take the error palette. */
  danger?: boolean;
  disabled?: boolean;
  /** Drops the visible label, leaving an icon-only pill; the `label` still
   * reaches assistive tech through `aria-label`. */
  iconOnly?: boolean;
  onClick?: () => void;
}

/** One 38px pill of a page's tool row — Filter, Sort, Details, and every
 * button of the Content selection action bar — so both pages' rows read as one
 * set. Ref-forwarding, so a popover or a menu can anchor to it. */
const ToolPill = forwardRef<HTMLButtonElement, ToolPillProps>(function ToolPill(
  { icon, label, chevron, active, badge, danger, disabled, iconOnly, onClick },
  ref
) {
  const theme = useTheme();
  const accent = danger
    ? theme.palette.error.main
    : active
      ? theme.palette.primary.main
      : theme.palette.text.primary;
  const iconColor = danger
    ? theme.palette.error.main
    : active
      ? theme.palette.primary.main
      : theme.palette.text.secondary;

  return (
    <ButtonBase
      ref={ref}
      disabled={disabled}
      onClick={onClick}
      aria-label={label}
      sx={{
        display: "inline-flex",
        alignItems: "center",
        justifyContent: "center",
        gap: "7px",
        height: 38,
        px: iconOnly ? 0 : "14px",
        width: iconOnly ? 38 : "auto",
        borderRadius: "999px",
        whiteSpace: "nowrap",
        border: `1px solid ${active ? theme.palette.primary.main : theme.palette.divider}`,
        backgroundColor: active
          ? alpha(theme.palette.primary.main, 0.12)
          : theme.palette.background.paper,
        color: accent,
        fontSize: 13,
        fontWeight: 700,
        flexShrink: 0,
        opacity: disabled ? 0.45 : 1,
        transition: theme.transitions.create(["background-color", "border-color"], {
          duration: 120,
        }),
        "&:hover": {
          backgroundColor: active
            ? alpha(theme.palette.primary.main, 0.16)
            : theme.palette.action.hover,
        },
      }}>
      <Icon iconName={icon} style={{ fontSize: 14, color: iconColor }} />
      {!iconOnly && label}
      {!iconOnly && badge !== undefined && <CountPill active>{badge}</CountPill>}
      {!iconOnly && chevron && (
        <Icon iconName={ICON_NAME.CHEVRON_DOWN} style={{ fontSize: 12, color: iconColor }} />
      )}
    </ButtonBase>
  );
});

export default ToolPill;
