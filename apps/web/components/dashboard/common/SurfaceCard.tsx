"use client";

import { Paper, alpha, useTheme } from "@mui/material";
import type { PaperProps } from "@mui/material";
import { forwardRef } from "react";

import { surfaceShadows } from "@/components/dashboard/common/surfaceShadows";

export type SurfaceCardProps = Omit<PaperProps, "elevation" | "variant"> & {
  /** Picked, or ticked in a picker: the border and a soft ring take the primary
   * accent. */
  selected?: boolean;
  /** Lifts and deepens on hover. On by default — every card either of the two
   * pages renders is something to click. */
  hoverable?: boolean;
  /** A pointer rather than the thing itself — a shortcut tile. */
  dashed?: boolean;
};

/**
 * The one card chrome both pages use: a 12px radius, a 1.5px border, the
 * shared soft rest shadow that deepens and lifts on hover, and a soft primary ring
 * once the card is selected. Every result tile, feed card, folder tile, list
 * row and shortcut tile is one of these, so the two pages cannot drift on
 * radius, border weight or elevation.
 */
const SurfaceCard = forwardRef<HTMLDivElement, SurfaceCardProps>(function SurfaceCard(
  { selected, hoverable = true, dashed, sx, children, ...rest },
  ref
) {
  const theme = useTheme();
  const shadow = surfaceShadows(theme);
  const ring = `0 0 0 3px ${alpha(theme.palette.primary.main, 0.12)}`;
  const selectedShadow = `${ring}, ${shadow.hover}`;

  return (
    <Paper
      ref={ref}
      elevation={0}
      sx={[
        {
          position: "relative",
          borderRadius: "12px",
          border: `1.5px ${dashed ? "dashed" : "solid"} ${
            selected ? theme.palette.primary.main : theme.palette.divider
          }`,
          boxShadow: selected ? selectedShadow : shadow.rest,
          transition: theme.transitions.create(["transform", "box-shadow", "border-color"], {
            duration: 140,
          }),
          ...(hoverable && {
            "&:hover": {
              // Not the primary accent: on the Content page a hover that looks
              // like a selection is a lie, since selection is what drives the
              // action bar.
              borderColor: selected ? theme.palette.primary.main : alpha(theme.palette.text.primary, 0.24),
              boxShadow: selected ? selectedShadow : shadow.hover,
              transform: "translateY(-2px)",
            },
          }),
        },
        ...(Array.isArray(sx) ? sx : [sx]),
      ]}
      {...rest}>
      {children}
    </Paper>
  );
});

export default SurfaceCard;
