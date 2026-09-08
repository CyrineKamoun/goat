"use client";

import { Box, Typography, alpha, useTheme } from "@mui/material";

import type { ICON_NAME} from "@p4b/ui/components/Icon";
import { Icon } from "@p4b/ui/components/Icon";

/**
 * The small pill the row's controls are made of.
 *
 * Its own component rather than MUI's `Chip`: a chip's icon sits hard against the border
 * and its `filled` state paints the whole pill in the accent, which is far too loud for a
 * control that is merely *set*. Here the accent shows as a tinted ground and a coloured
 * border, and the icon keeps its own breathing room.
 */
const UploadPill = ({
  icon,
  label,
  tone = "default",
  onClick,
  title,
  trailingIcon,
}: {
  icon: ICON_NAME;
  label: string;
  /** `active` = this has been set; `attention` = there is something here to look at. */
  tone?: "default" | "active" | "attention" | "static";
  onClick?: () => void;
  title?: string;
  trailingIcon?: ICON_NAME;
}) => {
  const theme = useTheme();

  const palette = {
    default: {
      border: theme.palette.divider,
      background: "transparent",
      color: theme.palette.text.secondary,
      icon: theme.palette.text.secondary,
    },
    active: {
      border: alpha(theme.palette.primary.main, 0.5),
      background: alpha(theme.palette.primary.main, 0.08),
      color: theme.palette.text.primary,
      icon: theme.palette.primary.main,
    },
    attention: {
      border: alpha(theme.palette.warning.main, 0.6),
      background: alpha(theme.palette.warning.main, 0.1),
      color: theme.palette.text.primary,
      icon: theme.palette.warning.main,
    },
    static: {
      border: theme.palette.divider,
      background: theme.palette.action.hover,
      color: theme.palette.text.secondary,
      icon: theme.palette.text.disabled,
    },
  }[tone];

  return (
    <Box
      component={onClick ? "button" : "div"}
      type={onClick ? "button" : undefined}
      onClick={onClick}
      title={title}
      sx={{
        display: "inline-flex",
        alignItems: "center",
        gap: 1.5,
        maxWidth: "100%",
        px: 2.25,
        py: 1,
        borderRadius: 999,
        border: `1px solid ${palette.border}`,
        backgroundColor: palette.background,
        color: palette.color,
        font: "inherit",
        cursor: onClick ? "pointer" : "default",
        transition: theme.transitions.create(["background-color", "border-color"], {
          duration: theme.transitions.duration.shortest,
        }),
        "&:hover": onClick
          ? { borderColor: theme.palette.text.disabled, backgroundColor: theme.palette.action.hover }
          : undefined,
      }}>
      <Icon iconName={icon} style={{ fontSize: 11 }} htmlColor={palette.icon} />
      <Typography
        variant="caption"
        fontWeight={600}
        noWrap
        sx={{ color: "inherit", lineHeight: 1.4 }}>
        {label}
      </Typography>
      {trailingIcon && (
        <Icon iconName={trailingIcon} style={{ fontSize: 10 }} htmlColor={palette.icon} />
      )}
    </Box>
  );
};

export default UploadPill;
