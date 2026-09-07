"use client";

import { Box, alpha, useTheme } from "@mui/material";
import type { ReactNode } from "react";

import { Icon } from "@p4b/ui/components/Icon";

import type { MarkKind } from "@/lib/catalog/kind";
import { markFor, toneColorOf, toneFor } from "@/lib/catalog/kind";

interface MarkBlockProps {
  kind: MarkKind;
  /** The data's shape, where the item states one. */
  geometryType?: string | null;
  /** The square's side; its glyph and corner scale with it. */
  size?: number;
  /** Overlaid on the block's bottom-right corner — the "external" bubble a
   * shortcut carries. */
  badge?: ReactNode;
}

/**
 * The small tinted square that stands in for a picture wherever a card or row
 * is too narrow to carry one: a list row's leading block, a folder tile, a
 * shortcut tile, a dialog header. It takes its kind's tone, so a row says what
 * it holds before its label is read, and it draws the same glyph the full
 * thumbnail would.
 */
const MarkBlock = ({ kind, geometryType, size = 32, badge }: MarkBlockProps) => {
  const theme = useTheme();
  const color = toneColorOf(theme, toneFor(kind));

  return (
    <Box
      sx={{
        position: "relative",
        width: size,
        height: size,
        flexShrink: 0,
        borderRadius: `${Math.round(size / 4)}px`,
        backgroundColor: alpha(color, 0.12),
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
      }}>
      <Icon
        iconName={markFor({ kind, geometryType })}
        style={{ fontSize: Math.round(size * 0.5) }}
        htmlColor={color}
      />
      {badge}
    </Box>
  );
};

export default MarkBlock;
