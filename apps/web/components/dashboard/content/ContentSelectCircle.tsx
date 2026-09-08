"use client";

import { Checkbox, useTheme } from "@mui/material";
import { useTranslation } from "react-i18next";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

interface ContentSelectCircleProps {
  checked: boolean;
  onToggle: () => void;
  /** Below `md` the circle grows to a 28px tap target and stays visible at
   * rest — a touch screen has no hover to reveal it with. */
  mobile?: boolean;
  /** Marks the element the card's hover rule targets. */
  className?: string;
}

/** The select circle on every card and row: shown while its own card is
 * hovered (a rule the card owns, keyed on `className`), while it is checked,
 * and at rest on touch screens. Selecting one item deliberately does not
 * reveal the circles of the others. */
const ContentSelectCircle = ({ checked, onToggle, mobile, className }: ContentSelectCircleProps) => {
  const { t } = useTranslation("common");
  const theme = useTheme();
  const size = mobile ? 28 : 22;

  return (
    <Checkbox
      className={className}
      size="small"
      checked={checked}
      aria-label={t("select")}
      onClick={(event) => {
        event.stopPropagation();
        onToggle();
      }}
      icon={
        <Icon iconName={ICON_NAME.CIRCLE_EMPTY} style={{ fontSize: size, color: theme.palette.text.secondary }} />
      }
      checkedIcon={
        <Icon iconName={ICON_NAME.CIRCLECHECK} style={{ fontSize: size, color: theme.palette.primary.main }} />
      }
      sx={{
        p: 0,
        flexShrink: 0,
        borderRadius: "50%",
        // Sits over a card thumbnail as well as over plain paper, so it
        // carries its own backdrop to stay readable either way.
        backgroundColor: theme.palette.background.paper,
        opacity: checked || mobile ? 1 : 0,
        transition: "opacity 120ms",
        "&:hover": { backgroundColor: theme.palette.background.paper },
      }}
    />
  );
};

export default ContentSelectCircle;
