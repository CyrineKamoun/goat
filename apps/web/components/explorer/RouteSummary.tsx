/**
 * A route's summary in the left panel (board 6): its name and, on the
 * Explorer's date, its Overview and its Stops (board 10). Service and
 * Calendar come with later steps and are shown, disabled, where they will be.
 */
import { Box, ButtonBase, Skeleton, Stack, Tab, Tabs, Tooltip, Typography } from "@mui/material";
import { useEffect } from "react";
import { useTranslation } from "react-i18next";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import { DEFAULT_PERIODS } from "@/lib/explorer/derive/operations";
import { clock, count, shortDate } from "@/lib/explorer/format";
import { modeOf, routeColor } from "@/lib/explorer/modes";
import {
  type ExplorerRouteTab,
  selectExplorerRoute,
  setExplorerDrawing,
  setExplorerRouteTab,
} from "@/lib/store/explorer/slice";

import { type RouteRow, type RouteSummaryData, useRouteSummary } from "@/hooks/explorer/useExplorerData";
import { useAppDispatch, useAppSelector } from "@/hooks/store/ContextHooks";

import { useExplorer } from "@/components/explorer/ExplorerContext";
import { useTimetablesPending } from "@/components/explorer/ExplorerLoading";
import { RouteBadge, namedByMode, routeName } from "@/components/explorer/RouteBadge";
import { RouteStops } from "@/components/explorer/RouteStops";
import { DateChip, SectionHeader } from "@/components/explorer/SectionHeader";
import { VehiclesCurve } from "@/components/explorer/VehiclesCurve";
import { modeIcon } from "@/components/explorer/modeIcon";

/** A figure on the panel's tinted ground, as in the cost section. */
function Tile({ value, label }: { value: string; label: string }) {
  return (
    <Box sx={{ flex: 1, minWidth: 0, bgcolor: "action.hover", borderRadius: 1.5, px: 1.25, py: 1 }}>
      <Typography variant="body1" fontWeight="bold" noWrap>
        {value}
      </Typography>
      <Typography variant="caption" color="text.secondary" display="block" sx={{ lineHeight: 1.3 }}>
        {label}
      </Typography>
    </Box>
  );
}

/** Width of a direction's column. */
const DIRECTION_COL = 76;

function ServiceTable({ data }: { data: RouteSummaryData }) {
  const { t } = useTranslation("common");
  const cols = data.directions;
  return (
    // Tinted like the cost section; rows split by hairlines, the head by none.
    <Box sx={{ bgcolor: "action.hover", borderRadius: 1.5, overflow: "hidden", px: 1.5, py: 0.5 }}>
      {/* The directions, each in its own column; the unit is in the values. */}
      <Stack direction="row" columnGap={1.5} alignItems="flex-end" sx={{ py: 1 }}>
        <Box sx={{ flex: 1 }} />
        {cols.map((c) => (
          // "To" goes without saying in a column of headways: more of the
          // destination fits; the whole of it is in the tooltip.
          <Typography
            key={c.direction.key}
            variant="caption"
            color="text.secondary"
            fontWeight={600}
            noWrap
            title={c.direction.label}
            sx={{ width: DIRECTION_COL, flexShrink: 0, textAlign: "right" }}>
            {c.direction.label.replace(/^To /, "")}
          </Typography>
        ))}
      </Stack>
      {DEFAULT_PERIODS.map((p) => (
        <Stack
          key={p.key}
          direction="row"
          columnGap={1.5}
          alignItems="center"
          sx={{ py: 0.75, borderTop: 1, borderColor: "divider" }}>
          <Box sx={{ flex: 1, minWidth: 0 }}>
            <Typography variant="body2" fontWeight={600} noWrap sx={{ lineHeight: 1.3 }}>
              {t(`explorer_period_${p.key}`)}
            </Typography>
            <Typography
              variant="caption"
              color="text.secondary"
              display="block"
              noWrap
              sx={{ lineHeight: 1.3 }}>
              {p.start_s ? clock(p.start_s) : ""}–{p.end_s < 30 * 3600 ? clock(p.end_s) : ""}
            </Typography>
          </Box>
          {cols.map((c) => {
            const h = c.headways.get(p.key);
            const none = !h || h.departures === 0;
            const single = !none && h.minutes === null;
            return (
              <Typography
                key={c.direction.key}
                variant="body2"
                fontWeight={none || single ? 400 : 600}
                color={none || single ? "text.secondary" : "text.primary"}
                sx={{
                  width: DIRECTION_COL,
                  flexShrink: 0,
                  textAlign: "right",
                  fontVariantNumeric: "tabular-nums",
                }}>
                {none ? "–" : single ? t("explorer_one_trip") : `${h.minutes}′`}
              </Typography>
            );
          })}
        </Stack>
      ))}
    </Box>
  );
}

function Overview({ route, data }: { route: RouteRow; data: RouteSummaryData }) {
  const { t, i18n } = useTranslation("common");
  const date = useAppSelector((s) => s.explorer.date) as string;
  const lang = i18n.language;
  const ops = data.operations;
  if (!ops.runs) {
    return (
      <Typography variant="body2" color="text.secondary" sx={{ mt: 2 }}>
        {t("explorer_no_service", { date: shortDate(date, lang) })}
      </Typography>
    );
  }
  return (
    <>
      <Box sx={{ mt: 1 }}>
        <SectionHeader
          px={0}
          icon={ICON_NAME.CLOCK}
          label={t("explorer_headways")}
          aside={<DateChip date={date} />}
        />
      </Box>
      <ServiceTable data={data} />
      <Typography variant="caption" color="text.secondary" display="block" sx={{ mt: 1 }}>
        {t("explorer_first_last_trips", {
          first: clock(ops.first_dep_s),
          last: clock(ops.last_dep_s),
          count: ops.runs,
        })}
      </Typography>

      <Box sx={{ mt: 2 }}>
        <SectionHeader
          px={0}
          icon={ICON_NAME.BUS}
          label={t("explorer_vehicles_in_service_title")}
          aside={<DateChip date={date} />}
        />
      </Box>
      <VehiclesCurve values={data.curve} color={routeColor(route.color, modeOf(route.route_type))} />
      <Box sx={{ mt: 2 }}>
        <SectionHeader
          px={0}
          icon={ICON_NAME.CHART}
          label={t("explorer_operations")}
          aside={<DateChip date={date} />}
        />
      </Box>
      <Stack direction="row" spacing={1}>
        <Tile value={count(ops.vehicle_hours, lang)} label={t("explorer_vehicle_hours")} />
        <Tile value={count(ops.vehicle_km, lang)} label={t("explorer_vehicle_km")} />
        <Tooltip title={ops.peak_from_blocks ? "" : t("explorer_peak_estimated")}>
          <Box sx={{ flex: 1, display: "flex" }}>
            <Tile value={count(ops.peak_vehicles, lang)} label={t("explorer_peak_vehicles")} />
          </Box>
        </Tooltip>
      </Stack>
      <Box sx={{ mb: 2 }} />
    </>
  );
}

/** The Overview's shape while its figures are worked out, so it fills in
 * rather than jumps in; says so while the timetables are still loading. */
function OverviewSkeleton() {
  const { t } = useTranslation("common");
  const pending = useTimetablesPending();
  return (
    <Box aria-busy sx={{ pt: 2, pb: 2 }}>
      <Typography variant="caption" color="text.secondary" display="block" sx={{ mb: 1 }}>
        {pending ?? t("explorer_calculating")}
      </Typography>
      <Skeleton variant="rounded" height={190} />
      <Skeleton variant="text" width="60%" sx={{ mt: 1 }} />
      <Skeleton variant="rounded" height={80} sx={{ mt: 2 }} />
      <Stack direction="row" spacing={1} sx={{ mt: 2 }}>
        {[0, 1, 2].map((i) => (
          <Skeleton key={i} variant="rounded" height={58} sx={{ flex: 1 }} />
        ))}
      </Stack>
    </Box>
  );
}

/** The meta line's line height: the icon beside it is centred on one. */
const META_LINE = "20px";

/** The route's tabs as GOAT's panels draw theirs (the toolbox's): sentence
 * case, 13 px, on a divider. They fill the bar, each as wide as its label plus
 * padding either side and a share of what is left, so no label meets an edge. */
const TAB_SX = {
  minHeight: 36,
  minWidth: 0,
  flex: "1 1 auto",
  px: 3,
  textTransform: "none",
  fontSize: "0.8125rem",
} as const;

export function RouteSummary({ route }: { route: RouteRow }) {
  const { t } = useTranslation("common");
  const dispatch = useAppDispatch();
  const { db, complete, window, shapeLinesLayerId } = useExplorer();
  const date = useAppSelector((s) => s.explorer.date);
  const tab = useAppSelector((s) => s.explorer.routeTab);
  const summary = useRouteSummary(complete ? db : undefined, route, date, window);
  const mode = modeOf(route.route_type);

  useEffect(() => {
    if (!summary.data) return;
    dispatch(
      setExplorerDrawing({
        routeId: route.route_id,
        color: routeColor(route.color, mode),
        shapeLinesLayerId,
        ...summary.data.drawing,
      })
    );
  }, [summary.data, route.route_id, route.color, mode, shapeLinesLayerId, dispatch]);

  const meta = [
    // Named by its mode already, the heading needs no second mention of it.
    namedByMode(route) ? null : t(`explorer_mode_${mode}`),
    route.agency,
    summary.data ? t("explorer_directions", { count: summary.data.directions.length }) : null,
    summary.data ? t("explorer_stops", { count: summary.data.n_stops }) : null,
  ]
    .filter(Boolean)
    .join(" · ");

  return (
    <Stack sx={{ height: "100%", minHeight: 0 }}>
      <Box sx={{ px: 2.5, pt: 1.5, pb: 0.5 }}>
        <ButtonBase
          onClick={() => dispatch(selectExplorerRoute(null))}
          sx={{ gap: 0.75, color: "text.secondary", borderRadius: 1, px: 0.5 }}>
          <Icon iconName={ICON_NAME.CHEVRON_LEFT} style={{ fontSize: 11 }} />
          <Typography variant="caption">{t("explorer_routes")}</Typography>
        </ButtonBase>
        <Stack direction="row" spacing={1.25} alignItems="center" sx={{ mt: 1 }}>
          <RouteBadge route={route} />
          <Typography variant="h6" fontWeight="bold" noWrap sx={{ fontSize: 17 }}>
            {routeName(route, t)}
          </Typography>
        </Stack>
        <Stack
          direction="row"
          spacing={0.75}
          alignItems="flex-start"
          sx={{ mt: 0.75, color: "text.secondary" }}>
          {/* One text line tall, so the icon centres on the first line however
              the text wraps. */}
          <Box sx={{ display: "flex", alignItems: "center", height: META_LINE, flexShrink: 0 }}>
            <Icon iconName={modeIcon(mode)} style={{ fontSize: 12 }} htmlColor="inherit" />
          </Box>
          <Typography variant="caption" sx={{ lineHeight: META_LINE }}>
            {meta}
          </Typography>
        </Stack>
      </Box>
      <Box sx={{ borderBottom: 1, borderColor: "divider", flexShrink: 0 }}>
        <Tabs
          value={tab}
          onChange={(_e, value: ExplorerRouteTab) => dispatch(setExplorerRouteTab(value))}
          sx={{ minHeight: 36 }}>
          <Tab value="overview" label={t("overview")} sx={TAB_SX} />
          <Tab value="stops" label={t("explorer_tab_stops")} sx={TAB_SX} />
          {/* Shown where they will be, disabled until their phase. Direct
            children of Tabs: it hands its own props to each. */}
          {[t("explorer_tab_service"), t("calendar")].map((label) => (
            <Tab
              key={label}
              label={label}
              disabled
              title={t("explorer_coming_later")}
              sx={{ ...TAB_SX, "&.Mui-disabled": { pointerEvents: "auto" } }}
            />
          ))}
        </Tabs>
      </Box>
      <Box sx={{ flex: 1, minHeight: 0, overflowY: "auto", px: 2.5 }}>
        {summary.error && (
          <Typography variant="body2" color="error" sx={{ mt: 2 }}>
            {summary.error}
          </Typography>
        )}
        {!summary.data && !summary.error && <OverviewSkeleton />}
        {summary.data && tab === "overview" && <Overview route={route} data={summary.data} />}
        {summary.data && tab === "stops" && (
          <RouteStops route={route} directions={summary.data.directions.map((d) => d.direction)} />
        )}
      </Box>
    </Stack>
  );
}
