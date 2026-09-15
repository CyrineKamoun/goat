"use client";

import { Box } from "@mui/material";
import { useEffect, useState } from "react";

import { shortcutLabel } from "@/lib/utils/platform";

interface ShortcutHintProps {
  /** The key pressed together with Ctrl or Command, e.g. `K`. */
  letter: string;
}

/**
 * The key badge shown inside a field that has a Ctrl/Cmd shortcut.
 *
 * The label is decided from the browser, so it reads `⌘K` on a Mac and
 * `Ctrl+K` on Windows and Linux. That can only be known on the client, so
 * nothing is rendered until after mount — the server has no platform to
 * render, and guessing one would mismatch on hydration.
 */
const ShortcutHint: React.FC<ShortcutHintProps> = ({ letter }) => {
  const [label, setLabel] = useState<string | null>(null);

  useEffect(() => {
    setLabel(shortcutLabel(letter, window.navigator));
  }, [letter]);

  if (!label) return null;

  return (
    <Box
      component="kbd"
      sx={{
        fontFamily: "inherit",
        fontSize: 11.5,
        color: "text.secondary",
        border: (theme) => `1px solid ${theme.palette.divider}`,
        borderRadius: "5px",
        px: "6px",
        py: "1px",
        whiteSpace: "nowrap",
      }}>
      {label}
    </Box>
  );
};

export default ShortcutHint;
