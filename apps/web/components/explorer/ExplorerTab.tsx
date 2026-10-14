/**
 * The Explorer tab (boards 3, 4): the bundle's routes and its stations
 * and platforms, searched together and filtered by type, route type and agency.
 * Selecting a route turns the tab into its summary; picking a station or
 * platform shows it on the map. The operating cost sits below, in CostSection.
 */
import { Box, InputAdornment, List, ListItemButton, Stack, TextField, Typography } from "@mui/material";
import { type ReactNode, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import { filterCount, listPlaces, matchingRouteIdxs, routeMatches, shows } from "@/lib/explorer/filters";
import { compact, count } from "@/lib/explorer/format";
import { modeOf } from "@/lib/explorer/modes";
import { selectExplorerRoute } from "@/lib/store/explorer/slice";

import { type RouteRow, useAgencies, useRoutes, useStops } from "@/hooks/explorer/useExplorerData";
import { useAppDispatch, useAppSelector } from "@/hooks/store/ContextHooks";

import { useExplorer } from "@/components/explorer/ExplorerContext";
import { ExplorerFilterButton, ExplorerFilterChips } from "@/components/explorer/ExplorerFilter";
import { RouteBadge } from "@/components/explorer/RouteBadge";
import { RouteSummary } from "@/components/explorer/RouteSummary";
import { PX, SectionHeader } from "@/components/explorer/SectionHeader";
import { StationsList } from "@/components/explorer/StationsList";
import { StopMarker } from "@/components/explorer/StopMarker";
import { modeIcon } from "@/components/explorer/modeIcon";

/** A list longer than this is cut short: narrowing the search finds the rest. */
const SHOWN = 200;

/** Rows a list keeps when the two share the panel, and a row's height. */
const MIN_ROWS = 6;
const ROW_HEIGHT = 36;

function matches(route: RouteRow, query: string): boolean {
  if (!query) return true;
  const q = query.toLowerCase();
  return [route.short_name, route.long_name, route.agency, route.route_id].some((v) =>
    v?.toLowerCase().includes(q)
  );
}

/** "8 of 89" once something narrows the list, else the plain count. */
function useAmount(shown: number, total: number | undefined, narrowed: boolean): string {
  const { t, i18n } = useTranslation("common");
  if (total === undefined) return "…";
  if (!narrowed) return count(total, i18n.language);
  // A five-digit total is shortened, so the pill leaves the title room.
  const m = total >= 10000 ? compact(total, i18n.language) : count(total, i18n.language);
  return t("explorer_n_of_m", { n: count(shown, i18n.language), m });
}

/** A section's rows, scrolling within the room the section gets. */
function SectionList({ hidden, rows, children }: { hidden: boolean; rows: number; children: ReactNode }) {
  return (
    <List
      dense
      disablePadding
      // As tall as its rows, shrinking to share the room when both are long;
      // a short list keeps all its rows, a long one at least a few.
      sx={{
        flex: "0 1 auto",
        minHeight: Math.min(Math.max(rows, 1), MIN_ROWS) * ROW_HEIGHT,
        overflowY: "auto",
        display: hidden ? "none" : "block",
      }}>
      {children}
    </List>
  );
}

function Note({ children }: { children: ReactNode }) {
  return (
    <Typography variant="caption" color="text.secondary" sx={{ display: "block", px: PX, py: 1 }}>
      {children}
    </Typography>
  );
}

export function ExplorerTab() {
  const { t } = useTranslation("common");
  const dispatch = useAppDispatch();
  const { db } = useExplorer();
  const bundleName = useAppSelector((s) => s.explorer.bundleName);
  const selectedRouteId = useAppSelector((s) => s.explorer.selectedRouteId);
  const filters = useAppSelector((s) => s.explorer.filters);
  const routes = useRoutes(db);
  const stops = useStops(db);
  const agencies = useAgencies(db);
  const [query, setQuery] = useState("");
  const [routesFolded, setRoutesFolded] = useState(false);
  // Board 15: the stops are there to be found, the routes to be browsed.
  const [stopsFolded, setStopsFolded] = useState(true);

  const q = query.trim();
  const narrowed = !!q || filterCount(filters) > 0;
  const filteredRoutes = useMemo(
    () => (routes.data ?? []).filter((r) => routeMatches(r, filters) && matches(r, q)),
    [routes.data, filters, q]
  );
  const filteredStops = useMemo(
    () => listPlaces(stops.data ?? [], q, matchingRouteIdxs(routes.data ?? [], filters)),
    [stops.data, routes.data, filters, q]
  );
  const routeByIdx = useMemo(() => new Map((routes.data ?? []).map((r) => [r.route_idx, r])), [routes.data]);
  const routesAmount = useAmount(filteredRoutes.length, routes.data?.length, narrowed);
  const stopsAmount = useAmount(filteredStops.length, stops.data?.length, narrowed);

  const selected = routes.data?.find((r) => r.route_id === selectedRouteId);
  if (selected) return <RouteSummary route={selected} />;

  const showRoutes = shows(filters, "routes");
  const showStops = shows(filters, "stops");
  // A search that finds stops opens their section: the match is the point.
  const stopsOpen = !stopsFolded || (!!q && filteredStops.length > 0);

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
          // body2, as the rows below: the field's default size outweighs them.
          sx={{ mt: 1.5, "& .MuiInputBase-input": { fontSize: 14 } }}
          placeholder={t("explorer_search")}
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          InputProps={{
            startAdornment: (
              <InputAdornment position="start">
                <Icon iconName={ICON_NAME.SEARCH} style={{ fontSize: 14 }} />
              </InputAdornment>
            ),
            endAdornment: (
              <InputAdornment position="end">
                <ExplorerFilterButton
                  routes={routes.data ?? []}
                  agencies={agencies.data ?? []}
                  stopCount={stops.data?.length}
                />
              </InputAdornment>
            ),
          }}
        />
        <ExplorerFilterChips agencies={agencies.data ?? []} />
      </Box>

      {showRoutes && (
        <>
          <SectionHeader
            icon={ICON_NAME.ROUTE}
            label={t("explorer_routes")}
            amount={routesAmount}
            folded={routesFolded}
            onFold={() => setRoutesFolded((f) => !f)}
          />
          <SectionList hidden={routesFolded} rows={Math.min(filteredRoutes.length, SHOWN)}>
            {filteredRoutes.slice(0, SHOWN).map((route) => (
              <ListItemButton
                key={route.route_idx}
                onClick={() => dispatch(selectExplorerRoute(route.route_id))}
                sx={{ px: PX, py: 0.75, gap: 1.5, minHeight: ROW_HEIGHT }}>
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
            {filteredRoutes.length > SHOWN && (
              <Note>{t("explorer_more_routes", { count: filteredRoutes.length - SHOWN })}</Note>
            )}
            {routes.data && filteredRoutes.length === 0 && <Note>{t("explorer_no_routes")}</Note>}
          </SectionList>
        </>
      )}

      {showStops && (
        <>
          <SectionHeader
            icon={<StopMarker size={12} />}
            label={t("explorer_stops_stations")}
            amount={stopsAmount}
            folded={!stopsOpen}
            onFold={() => setStopsFolded(stopsOpen)}
          />
          <SectionList hidden={!stopsOpen} rows={Math.min(filteredStops.length, SHOWN)}>
            <StationsList listed={filteredStops.slice(0, SHOWN)} routeByIdx={routeByIdx} />
            {filteredStops.length > SHOWN && (
              <Note>{t("explorer_more_stops", { count: filteredStops.length - SHOWN })}</Note>
            )}
            {stops.data && filteredStops.length === 0 && <Note>{t("explorer_no_stops")}</Note>}
          </SectionList>
        </>
      )}
      {/* Sections sit at the top; what room is left stays below them. */}
      <Box sx={{ flex: 1 }} />
    </Stack>
  );
}
