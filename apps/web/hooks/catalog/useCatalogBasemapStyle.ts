import { useTheme } from "@mui/material";

import { BASEMAPS } from "@/lib/constants/basemaps";

/** The basemap style matching the current theme. */
export const useCatalogBasemapStyle = (): string | undefined => {
  const theme = useTheme();
  const value = theme.palette.mode === "dark" ? "dark" : "light";
  return BASEMAPS.find((basemap) => basemap.value === value)?.url;
};
