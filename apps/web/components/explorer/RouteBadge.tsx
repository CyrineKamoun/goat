import { Box } from "@mui/material";

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
