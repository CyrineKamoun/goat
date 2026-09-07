import type { Theme } from "@mui/material";

/**
 * The one elevation recipe every dashboard surface (Home bands, Content and
 * Catalog cards and rows, side panels, search fields) rests on: a single soft
 * 10px blur at low opacity, so a surface reads as sitting on the page rather
 * than as a hard edge under it. Hover keeps the same recipe, only deeper and
 * a touch further away, so lifting a card continues its resting shadow instead
 * of switching to a different one.
 */
export const surfaceShadows = (theme: Theme): { rest: string; hover: string } => {
  const ink = theme.palette.mode === "light" ? "58, 53, 65" : "19, 17, 32";
  return {
    rest: theme.shadows[6],
    hover: `0px 6px 20px 0px rgba(${ink}, 0.16)`,
  };
};
