import { useMemo } from "react";

import { createTheme, useTheme } from "@mui/material/styles";

/**
 * Returns a MUI theme with the primary color overridden if `primaryColor` is set.
 * Falls back to the base theme when no custom color is provided.
 */
export function useBrandedTheme(primaryColor: string | undefined) {
  const baseTheme = useTheme();
  return useMemo(() => {
    if (!primaryColor) return baseTheme;
    return createTheme(baseTheme, {
      palette: {
        primary: baseTheme.palette.augmentColor({
          color: { main: primaryColor },
        }),
      },
    });
  }, [baseTheme, primaryColor]);
}
