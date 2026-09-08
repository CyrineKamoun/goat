"use client";

import { Box, useTheme } from "@mui/material";
import { type Theme, emphasize } from "@mui/material/styles";
import { useEffect, useRef, useState } from "react";

import type { DatasetCollectionItems } from "@/lib/validations/layer";

import FeatureTable, { type FeatureTableField } from "@/components/common/FeatureTable";

/** The frame a read-only feature table sits in: a bordered box, a scrolling
 * area whose header band is painted behind the sticky header, and an optional
 * footer strip under it (a truncation note, a pager). */

/** Tall enough to read a dozen records in, short enough to leave what follows
 * it reachable. */
const DEFAULT_MAX_HEIGHT = 420;

/** The header's surface: a touch lighter than the card. */
const HEADER_BG = (theme: Theme) => emphasize(theme.palette.background.paper, 0.03);

const FeatureTableFrame = ({
  fields,
  data,
  isLoading,
  maxHeight = DEFAULT_MAX_HEIGHT,
  footer,
}: {
  fields: FeatureTableField[];
  data: DatasetCollectionItems | undefined;
  isLoading?: boolean;
  /** How tall the scrolling area may grow before it scrolls, in px. */
  maxHeight?: number;
  footer?: React.ReactNode;
}) => {
  const theme = useTheme();

  /** How tall the header is, so the strip beside it can be painted to match — see the band on the scrolling box. */
  const scrollBox = useRef<HTMLDivElement | null>(null);
  const [headHeight, setHeadHeight] = useState(0);
  useEffect(() => {
    const head = scrollBox.current?.querySelector("thead");
    if (!head) return;
    const measure = () => setHeadHeight(head.getBoundingClientRect().height);
    measure();
    const observer = new ResizeObserver(measure);
    observer.observe(head);
    return () => observer.disconnect();
  }, [fields, data]);

  return (
    <Box
      sx={{
        border: `1px solid ${theme.palette.divider}`,
        borderRadius: 2,
        overflow: "hidden",
      }}>
      <Box
        ref={scrollBox}
        sx={{
          overflow: "auto",
          maxHeight,
          /** The platform's own scrollbar, deliberately unstyled. */
          backgroundImage: `linear-gradient(${HEADER_BG(theme)} 0 ${headHeight}px, transparent ${headHeight}px)`,
        }}>
        {/* `bordered` for the column dividers: a sample can run to 40 columns and scrolls sideways. */}
        <FeatureTable
          fields={fields}
          data={data}
          isLoading={isLoading}
          variant="bordered"
          headerColor={HEADER_BG(theme)}
        />
      </Box>
      {footer && (
        <Box
          sx={{
            borderTop: `1px solid ${theme.palette.divider}`,
            backgroundColor: theme.palette.action.hover,
          }}>
          {footer}
        </Box>
      )}
    </Box>
  );
};

export default FeatureTableFrame;
