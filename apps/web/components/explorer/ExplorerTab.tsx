/**
 * The Explorer tab: the bundle's routes, searchable, and the network's
 * operating cost. Selecting a route turns the tab into its summary.
 */
import {
  Box,
  Divider,
  InputAdornment,
  List,
  ListItemButton,
  Stack,
  TextField,
  Typography,
} from "@mui/material";
import { useMemo, useState } from "react";
import { useTranslation } from "react-i18next";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import { count, euros } from "@/lib/explorer/format";
import { selectExplorerRoute } from "@/lib/store/explorer/slice";

import { type RouteRow, useNetworkCost, useRoutes, useStopCount } from "@/hooks/explorer/useExplorerData";
import { useAppDispatch, useAppSelector } from "@/hooks/store/ContextHooks";

import { useExplorer } from "@/components/explorer/ExplorerContext";
import { RouteBadge } from "@/components/explorer/RouteBadge";
import { RouteSummary } from "@/components/explorer/RouteSummary";

/** A list longer than this is cut short: narrowing the search finds the rest. */
const SHOWN = 200;

function matches(route: RouteRow, query: string): boolean {
  if (!query) return true;
  const q = query.toLowerCase();
  return [route.short_name, route.long_name, route.agency, route.route_id].some((v) =>
    v?.toLowerCase().includes(q)
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

  const filtered = useMemo(
    () => (routes.data ?? []).filter((r) => matches(r, query.trim())),
    [routes.data, query]
  );
  const selected = routes.data?.find((r) => r.route_id === selectedRouteId);

  if (selected) return <RouteSummary route={selected} />;

  return (
    <Stack sx={{ height: "100%", minHeight: 0 }}>
      <Box sx={{ px: 2, pt: 2, pb: 1 }}>
        <Typography variant="body1" fontWeight="bold">
          {t("explorer")}
        </Typography>
        <Stack direction="row" spacing={1} alignItems="center" sx={{ mt: 1.5, color: "text.secondary" }}>
          <Icon iconName={ICON_NAME.CUBE} style={{ fontSize: 13 }} />
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
      <Stack direction="row" justifyContent="space-between" alignItems="center" sx={{ px: 2, py: 1 }}>
        <Typography variant="body2" fontWeight="bold">
          {t("explorer_routes")}
        </Typography>
        <Typography variant="caption" color="text.secondary">
          {count(filtered.length, i18n.language)}
        </Typography>
      </Stack>
      <List dense disablePadding sx={{ flex: 1, minHeight: 0, overflowY: "auto" }}>
        {filtered.slice(0, SHOWN).map((route) => (
          <ListItemButton
            key={route.route_idx}
            onClick={() => dispatch(selectExplorerRoute(route.route_id))}
            sx={{ px: 2, gap: 1.5 }}>
            <RouteBadge route={route} />
            <Typography variant="body2" noWrap title={route.long_name ?? undefined}>
              {route.long_name || route.short_name || route.route_id}
            </Typography>
          </ListItemButton>
        ))}
        {filtered.length > SHOWN && (
          <Typography variant="caption" color="text.secondary" sx={{ display: "block", px: 2, py: 1 }}>
            {t("explorer_more_routes", { count: filtered.length - SHOWN })}
          </Typography>
        )}
        {routes.data && filtered.length === 0 && (
          <Typography variant="caption" color="text.secondary" sx={{ display: "block", px: 2, py: 1 }}>
            {t("explorer_no_routes")}
          </Typography>
        )}
      </List>
      <Stack direction="row" justifyContent="space-between" sx={{ px: 2, py: 1 }}>
        <Typography variant="body2" fontWeight="bold">
          {t("explorer_stops_stations")}
        </Typography>
        <Typography variant="caption" color="text.secondary">
          {stops.data !== undefined ? count(stops.data, i18n.language) : "…"}
        </Typography>
      </Stack>
      <Divider />
      <Box sx={{ px: 2, py: 1.5 }}>
        <Typography variant="caption" color="text.secondary" display="block">
          {t("explorer_network_cost")}
        </Typography>
        <Typography variant="body2" fontWeight="bold">
          {network.data ? `≈ ${euros(network.data.cost, i18n.language)} ${t("explorer_a_year")}` : "…"}
        </Typography>
      </Box>
    </Stack>
  );
}
