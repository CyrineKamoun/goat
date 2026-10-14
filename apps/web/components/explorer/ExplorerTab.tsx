/**
 * The Explorer tab: the bundle's routes, searchable, and the network's
 * operating cost. Selecting a route turns the tab into its summary.
 */
import {
  Box,
  Divider,
  IconButton,
  InputAdornment,
  List,
  ListItemButton,
  Stack,
  TextField,
  Typography,
} from "@mui/material";
import { type ReactNode, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import { count, euros } from "@/lib/explorer/format";
import { modeOf } from "@/lib/explorer/modes";
import { selectExplorerRoute } from "@/lib/store/explorer/slice";

import { type RouteRow, useNetworkCost, useRoutes, useStopCount } from "@/hooks/explorer/useExplorerData";
import { useAppDispatch, useAppSelector } from "@/hooks/store/ContextHooks";

import { useExplorer } from "@/components/explorer/ExplorerContext";
import { RouteBadge } from "@/components/explorer/RouteBadge";
import { RouteSummary } from "@/components/explorer/RouteSummary";
import { modeIcon } from "@/components/explorer/modeIcon";

/** Side padding of the tab's rows (board 3). */
const PX = 2.5;

/** A list longer than this is cut short: narrowing the search finds the rest. */
const SHOWN = 200;

function matches(route: RouteRow, query: string): boolean {
  if (!query) return true;
  const q = query.toLowerCase();
  return [route.short_name, route.long_name, route.agency, route.route_id].some((v) =>
    v?.toLowerCase().includes(q)
  );
}

/** A section's header: its icon, its name, how many it holds, and — for a
 * section that lists them — a fold toggle. */
function SectionHeader({
  icon,
  label,
  amount,
  folded,
  onFold,
}: {
  icon: ICON_NAME;
  label: string;
  amount: ReactNode;
  folded?: boolean;
  onFold?: () => void;
}) {
  const { t } = useTranslation("common");
  return (
    <Stack direction="row" alignItems="center" spacing={1.25} sx={{ px: PX, minHeight: 40 }}>
      <Icon iconName={icon} style={{ fontSize: 14 }} htmlColor="inherit" />
      <Typography variant="body2" fontWeight="bold" sx={{ flex: 1 }}>
        {label}
      </Typography>
      <Box
        component="span"
        sx={{
          px: 1,
          py: 0.25,
          borderRadius: 1,
          bgcolor: "action.hover",
          color: "text.secondary",
          fontSize: 12,
          fontWeight: 600,
        }}>
        {amount}
      </Box>
      {onFold && (
        <IconButton
          size="small"
          aria-label={folded ? t("explorer_unfold") : t("explorer_fold")}
          aria-expanded={!folded}
          onClick={onFold}
          sx={{ mr: -0.75 }}>
          <Icon iconName={folded ? ICON_NAME.CHEVRON_DOWN : ICON_NAME.CHEVRON_UP} style={{ fontSize: 11 }} />
        </IconButton>
      )}
    </Stack>
  );
}

export function ExplorerTab() {
  const { t, i18n } = useTranslation("common");
  const dispatch = useAppDispatch();
  const { db, complete, window } = useExplorer();
  const bundleName = useAppSelector((s) => s.explorer.bundleName);
  const selectedRouteId = useAppSelector((s) => s.explorer.selectedRouteId);
  const routes = useRoutes(db);
  const stops = useStopCount(db);
  const network = useNetworkCost(complete ? db : undefined, window);
  const [query, setQuery] = useState("");
  const [routesFolded, setRoutesFolded] = useState(false);

  const filtered = useMemo(
    () => (routes.data ?? []).filter((r) => matches(r, query.trim())),
    [routes.data, query]
  );
  const selected = routes.data?.find((r) => r.route_id === selectedRouteId);

  if (selected) return <RouteSummary route={selected} />;

  return (
    <Stack sx={{ height: "100%", minHeight: 0 }}>
      {/* No title of its own: the tab bar above already says Explorer. */}
      <Box sx={{ px: PX, pt: 2, pb: 1.5 }}>
        <Stack direction="row" spacing={1} alignItems="center" sx={{ color: "text.secondary" }}>
          <Icon iconName={ICON_NAME.CUBE} style={{ fontSize: 13 }} htmlColor="inherit" />
          <Typography variant="body2" fontWeight="bold" color="text.primary" noWrap>
            {bundleName}
          </Typography>
        </Stack>
        <TextField
          size="small"
          fullWidth
          sx={{ mt: 1.5 }}
          placeholder={t("explorer_search")}
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          InputProps={{
            startAdornment: (
              <InputAdornment position="start">
                <Icon iconName={ICON_NAME.SEARCH} style={{ fontSize: 14 }} />
              </InputAdornment>
            ),
          }}
        />
      </Box>
      <SectionHeader
        icon={ICON_NAME.ROUTE}
        label={t("explorer_routes")}
        amount={count(filtered.length, i18n.language)}
        folded={routesFolded}
        onFold={() => setRoutesFolded((f) => !f)}
      />
      <List
        dense
        disablePadding
        sx={{
          flex: routesFolded ? 0 : 1,
          minHeight: 0,
          overflowY: "auto",
          display: routesFolded ? "none" : "block",
        }}>
        {filtered.slice(0, SHOWN).map((route) => (
          <ListItemButton
            key={route.route_idx}
            onClick={() => dispatch(selectExplorerRoute(route.route_id))}
            sx={{ px: PX, py: 0.75, gap: 1.5, minHeight: 36 }}>
            <RouteBadge route={route} />
            <Typography variant="body2" noWrap title={route.long_name ?? undefined} sx={{ flex: 1 }}>
              {route.long_name || route.short_name || route.route_id}
            </Typography>
            <Box sx={{ color: "text.disabled", display: "flex" }}>
              <Icon
                iconName={modeIcon(modeOf(route.route_type))}
                style={{ fontSize: 13 }}
                htmlColor="inherit"
              />
            </Box>
          </ListItemButton>
        ))}
        {filtered.length > SHOWN && (
          <Typography variant="caption" color="text.secondary" sx={{ display: "block", px: PX, py: 1 }}>
            {t("explorer_more_routes", { count: filtered.length - SHOWN })}
          </Typography>
        )}
        {routes.data && filtered.length === 0 && (
          <Typography variant="caption" color="text.secondary" sx={{ display: "block", px: PX, py: 1 }}>
            {t("explorer_no_routes")}
          </Typography>
        )}
      </List>
      {routesFolded && <Box sx={{ flex: 1 }} />}
      <SectionHeader
        icon={ICON_NAME.LOCATION_MARKER}
        label={t("explorer_stops_stations")}
        amount={stops.data !== undefined ? count(stops.data, i18n.language) : "…"}
      />
      <Divider />
      <Box sx={{ px: PX, py: 1.5 }}>
        <Typography variant="caption" color="text.secondary" display="block">
          {t("explorer_network_cost")}
        </Typography>
        <Typography variant="body2">
          <b>{network.data ? `≈ ${euros(network.data.cost, i18n.language)} ${t("explorer_a_year")}` : "…"}</b>
          {network.data && (
            <Box component="span" sx={{ color: "text.secondary" }}>
              {" · "}
              {t("explorer_no_changes")}
            </Box>
          )}
        </Typography>
      </Box>
    </Stack>
  );
}
