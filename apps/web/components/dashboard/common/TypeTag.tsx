"use client";

import { Box, alpha, useTheme } from "@mui/material";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

interface TypeTagProps {
  label: string;
  /** The item narrows who in its space can see it — a padlock leads the tag. */
  locked?: boolean;
}

/** The kind, overlaid on a card's top-left corner. It sits on imagery — a map
 * thumbnail or a drawn stand-in — so it is a scrim over whatever is behind it
 * rather than a themed surface: a dark wash of `common.black` carrying
 * `common.white` text, which holds in both themes. */
const TypeTag = ({ label, locked }: TypeTagProps) => {
  const theme = useTheme();

  return (
    <Box
      sx={{
        display: "inline-flex",
        alignItems: "center",
        gap: 1,
        height: 20,
        px: 2.5,
        borderRadius: "6px",
        backgroundColor: alpha(theme.palette.common.black, 0.55),
        backdropFilter: "blur(2px)",
        fontSize: 12,
        fontWeight: 600,
        letterSpacing: 0.2,
        color: theme.palette.common.white,
        whiteSpace: "nowrap",
      }}>
      {locked && (
        <Icon
          iconName={ICON_NAME.LOCK}
          style={{ fontSize: 10 }}
          htmlColor={theme.palette.common.white}
        />
      )}
      {label}
    </Box>
  );
};

export default TypeTag;
