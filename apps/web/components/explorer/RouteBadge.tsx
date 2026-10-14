import { Box } from "@mui/material";
import type { TFunction } from "i18next";

import { modeOf, routeColor, textOn } from "@/lib/explorer/modes";

import type { RouteRow } from "@/hooks/explorer/useExplorerData";

/** A route's number on its colour, as riders see it on the vehicle. */
export function RouteBadge({ route, small }: { route: RouteRow; small?: boolean }) {
  const background = routeColor(route.color, modeOf(route.route_type));
  const color =
    route.text_color && /^[0-9A-Fa-f]{6}$/.test(route.text_color)
      ? `#${route.text_color}`
      : textOn(background);
  return (
    <Box
      component="span"
      sx={{
        display: "inline-flex",
        alignItems: "center",
        justifyContent: "center",
        minWidth: small ? 28 : 36,
        height: small ? 18 : 22,
        px: 0.75,
        borderRadius: 0.75,
        fontSize: small ? 11 : 12,
        fontWeight: 700,
        lineHeight: 1,
        whiteSpace: "nowrap",
        backgroundColor: background,
        color,
        flexShrink: 0,
      }}>
      {route.short_name || route.long_name?.slice(0, 4) || route.route_id}
    </Box>
  );
}

/** The name to show beside a route's badge once it is selected: its long
 * name, or — when it has none, or one that only repeats the number on the
 * badge — its mode. */
export function routeName(route: RouteRow, t: TFunction): string {
  return route.long_name && route.long_name !== route.short_name
    ? route.long_name
    : t(`explorer_mode_${modeOf(route.route_type)}`);
}

/** Whether `routeName` fell back to the route's mode. */
export function namedByMode(route: RouteRow): boolean {
  return !route.long_name || route.long_name === route.short_name;
}
