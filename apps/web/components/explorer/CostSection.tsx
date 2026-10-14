/**
 * The Explorer's operating cost, at the foot of its tab: one fixed place for
 * money, whatever is selected — the network with nothing selected, the route
 * once one is. Its title and the button to the cost rates never move; only
 * the figures below them follow the selection.
 */
import { Box, IconButton, Skeleton, Stack, Tooltip, Typography } from "@mui/material";
import { useTranslation } from "react-i18next";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import { compact, euros } from "@/lib/explorer/format";
import { modeOf, routeColor } from "@/lib/explorer/modes";

import { type RouteRow, useNetworkCost, useRouteSummary, useRoutes } from "@/hooks/explorer/useExplorerData";
import { useAppSelector } from "@/hooks/store/ContextHooks";

import { useExplorer } from "@/components/explorer/ExplorerContext";
import { useTimetablesPending } from "@/components/explorer/ExplorerLoading";
import { RouteBadge, routeName } from "@/components/explorer/RouteBadge";

/** Fixed to what its figures need, so the list or summary above never
 * changes height with them. */
const COST_SECTION_HEIGHT = 160;

/** What the figures below are for: a fixed line whose content follows the
 * selection, so the figures never change without saying what they now cover. */
function Scope({ route }: { route: RouteRow | undefined }) {
  const { t } = useTranslation("common");
  const name = route ? routeName(route, t) : t("explorer_network");
  return (
    // A label, not a control: no box around it, just a bar in the route's
    // colour (grey for the network) marking what the figures cover.
    <Stack
      direction="row"
      alignItems="center"
      spacing={1}
      sx={{
        height: 24,
        mt: 0.5,
        pl: 1.25,
        borderLeft: 3,
        borderColor: route ? routeColor(route.color, modeOf(route.route_type)) : "text.disabled",
      }}>
      {route ? (
        <RouteBadge route={route} small />
      ) : (
        <Box sx={{ display: "flex", color: "text.secondary" }}>
          <Icon iconName={ICON_NAME.WORKFLOW} style={{ fontSize: 13 }} htmlColor="inherit" />
        </Box>
      )}
      <Typography variant="body2" fontWeight={600} noWrap title={name}>
        {name}
      </Typography>
    </Stack>
  );
}

function Figure({ value, label }: { value: string | undefined; label: string }) {
  return (
    <Box sx={{ minWidth: 0 }}>
      {value ? (
        <Typography variant="body2" fontWeight="bold" noWrap>
          {value}
        </Typography>
      ) : (
        <Skeleton variant="rounded" width={44} height={14} sx={{ my: "3px" }} />
      )}
      <Typography variant="caption" color="text.secondary" display="block" noWrap>
        {label}
      </Typography>
    </Box>
  );
}

export function CostSection() {
  const { t, i18n } = useTranslation("common");
  const lang = i18n.language;
  const { db, complete, window } = useExplorer();
  const selectedRouteId = useAppSelector((s) => s.explorer.selectedRouteId);
  const date = useAppSelector((s) => s.explorer.date);
  const routes = useRoutes(db);
  const route = routes.data?.find((r) => r.route_id === selectedRouteId);
  // Both answers are remembered per database: the route summary above asks
  // for the same ones, so nothing is computed twice.
  const network = useNetworkCost(complete && !route ? db : undefined, window);
  const summary = useRouteSummary(complete && route ? db : undefined, route, date, window);
  const annual = route ? summary.data?.annual : network.data;
  const pending = useTimetablesPending();

  return (
    // Its own tinted ground: the cost is a standing part of the Explorer,
    // not a footnote to the list above.
    <Stack
      sx={{
        height: COST_SECTION_HEIGHT,
        flexShrink: 0,
        overflow: "hidden",
        borderTop: 1,
        borderColor: "divider",
        bgcolor: "action.hover",
        px: 2.5,
        pt: 1,
        pb: 2,
      }}>
      {/* Laid out like the tab's section headers: icon, name, action. */}
      <Stack direction="row" alignItems="center" spacing={1.25} sx={{ minHeight: 36 }}>
        <Icon iconName={ICON_NAME.COINS} style={{ fontSize: 14 }} htmlColor="inherit" />
        <Typography variant="body2" fontWeight="bold" sx={{ flex: 1 }} noWrap>
          {t("explorer_operating_cost")}
        </Typography>
        {/* The rates dialog comes next; the button already sits where it
            will open it. */}
        <Tooltip title={`${t("explorer_cost_rates")} · ${t("explorer_coming_later")}`}>
          <span>
            <IconButton size="small" disabled aria-label={t("explorer_cost_rates")} sx={{ mr: -0.75 }}>
              <Icon iconName={ICON_NAME.SLIDERS} style={{ fontSize: 13 }} />
            </IconButton>
          </span>
        </Tooltip>
      </Stack>
      <Scope route={route} />

      <Box sx={{ mt: 1.75 }}>
        <Stack direction="row" alignItems={annual ? "baseline" : "center"} spacing={1} sx={{ minHeight: 31 }}>
          {annual ? (
            <Typography fontWeight="bold" noWrap sx={{ fontSize: 26, lineHeight: 1.2 }}>
              ≈ {euros(annual.cost, lang)}
            </Typography>
          ) : (
            <Skeleton variant="rounded" width={84} height={26} sx={{ my: "2.5px" }} />
          )}
          {annual ? (
            <Typography variant="body2" color="text.secondary" noWrap>
              {t("explorer_a_year")} · {t("explorer_no_edits")}
            </Typography>
          ) : (
            <Typography variant="caption" color="text.secondary" noWrap>
              {pending ?? t("explorer_calculating")}
            </Typography>
          )}
        </Stack>
        <Stack direction="row" spacing={3} sx={{ mt: 1 }}>
          <Figure value={annual && compact(annual.vehicle_km, lang)} label={t("explorer_vehicle_km_year")} />
          <Figure
            value={annual && compact(annual.vehicle_hours, lang)}
            label={t("explorer_vehicle_hours_year")}
          />
        </Stack>
      </Box>
    </Stack>
  );
}
