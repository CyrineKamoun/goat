import { Box } from "@mui/material";
import type { PropsWithChildren } from "react";

type Anchor = "top_left" | "top_right" | "bottom_left" | "bottom_right";

// Margin between the popup and the nearest edge of its host area.
const EDGE_GAP = 12;

/**
 * Positions a fixed popup at one of the 4 corners of its parent
 * container. The parent is a positioned Box created by the layout
 * (see MapFixedPopupSlot host) and sized to the visible map content
 * area (between the toolbar, data panel, and side panels). Because
 * we position relative to that box, the popup automatically respects
 * the layout's chrome without any global state — same way the
 * Geocoder / ToolboxCtrl / MeasureButton controls are positioned.
 */
export function PopupFixedHost({
  anchor,
  children,
}: PropsWithChildren<{ anchor: Anchor }>) {
  const position: React.CSSProperties = {};
  switch (anchor) {
    case "top_left":
      position.top = EDGE_GAP;
      position.left = EDGE_GAP;
      break;
    case "top_right":
      position.top = EDGE_GAP;
      position.right = EDGE_GAP;
      break;
    case "bottom_left":
      position.bottom = EDGE_GAP;
      position.left = EDGE_GAP;
      break;
    case "bottom_right":
      position.bottom = EDGE_GAP;
      position.right = EDGE_GAP;
      break;
  }

  return (
    <Box
      sx={{
        position: "absolute",
        pointerEvents: "auto",
        ...position,
      }}>
      {children}
    </Box>
  );
}
