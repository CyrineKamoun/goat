/**
 * A route's summary in the left panel (board 6): its name and figures for the
 * Explorer's date. Overview only for now; Stops, Service and Calendar come
 * with later phases and are shown, disabled, where they will be.
 */
import { Box, ButtonBase, Skeleton, Stack, Tab, Tabs, Tooltip, Typography } from "@mui/material";
import { useEffect } from "react";
import { useTranslation } from "react-i18next";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import { DEFAULT_PERIODS } from "@/lib/explorer/derive/operations";
import { clock, count, shortDate } from "@/lib/explorer/format";
import { modeOf, routeColor } from "@/lib/explorer/modes";
import { selectExplorerRoute, setExplorerDrawing } from "@/lib/store/explorer/slice";

import { type RouteRow, type RouteSummaryData, useRouteSummary } from "@/hooks/explorer/useExplorerData";
import { useAppDispatch, useAppSelector } from "@/hooks/store/ContextHooks";

import { useExplorer } from "@/components/explorer/ExplorerContext";
import { useTimetablesPending } from "@/components/explorer/ExplorerLoading";
import { RouteBadge, namedByMode, routeName } from "@/components/explorer/RouteBadge";
import { SectionHeader } from "@/components/explorer/SectionHeader";
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

function ServiceTable({ data }: { data: RouteSummaryData }) {
  const { t } = useTranslation("common");
  const cols = data.directions;
  return (
    // Tinted like the cost section; rows split by hairlines, the head by none.
    <Box sx={{ bgcolor: "action.hover", borderRadius: 1.5, overflow: "hidden", px: 1.25, py: 0.5 }}>
      <Stack direction="row" columnGap={1} sx={{ py: 0.75 }}>
        <Typography variant="caption" color="text.secondary" sx={{ flex: 1 }}>
          {t("explorer_every_min")}
        </Typography>
        {cols.map((c) => (
          <Typography
            key={c.direction.key}
            variant="caption"
            color="text.secondary"
            noWrap
            title={c.direction.label}
            sx={{ width: 64, flexShrink: 0, textAlign: "right" }}>
            {c.direction.label}
          </Typography>
        ))}
      </Stack>
      {DEFAULT_PERIODS.map((p) => (
        <Stack
          key={p.key}
          direction="row"
          columnGap={1}
          sx={{ py: 0.6, borderTop: 1, borderColor: "divider" }}>
          <Typography variant="caption" sx={{ flex: 1 }}>
            <b>{t(`explorer_period_${p.key}`)}</b>{" "}
            <Box component="span" sx={{ color: "text.secondary" }}>
              {p.start_s ? clock(p.start_s) : ""}–{p.end_s < 30 * 3600 ? clock(p.end_s) : ""}
            </Box>
          </Typography>
          {cols.map((c) => {
            const h = c.headways.get(p.key);
            const text =
              !h || h.departures === 0 ? "–" : h.minutes === null ? t("explorer_one_trip") : `${h.minutes}′`;
            return (
              <Typography
                key={c.direction.key}
                variant="caption"
                fontWeight="bold"
                sx={{ width: 64, flexShrink: 0, textAlign: "right" }}>
                {text}
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
        {t("explorer_no_service")}
      </Typography>
    );
  }
  const peakIndex = ops.peak_at_s !== null ? Math.floor(ops.peak_at_s / 300) : null;
  return (
    <>
      <Box sx={{ mt: 1 }}>
        <SectionHeader
          px={0}
          icon={ICON_NAME.CLOCK}
          label={t("explorer_service")}
          note={shortDate(date, lang)}
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
          icon={ICON_NAME.CHART}
          label={t("explorer_operations")}
          note={t("explorer_per_day")}
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
      <Box sx={{ mt: 2 }}>
        <SectionHeader
          px={0}
          icon={ICON_NAME.BUS}
          label={t("explorer_vehicles_in_service_title")}
          note={t("explorer_peak_at", { count: ops.peak_vehicles, time: clock(ops.peak_at_s) })}
        />
      </Box>
      <VehiclesCurve
        values={data.curve}
        color={routeColor(route.color, modeOf(route.route_type))}
        markIndex={peakIndex}
      />
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
      <Stack direction="row" spacing={1} sx={{ mt: 2 }}>
        {[0, 1, 2].map((i) => (
          <Skeleton key={i} variant="rounded" height={58} sx={{ flex: 1 }} />
        ))}
      </Stack>
      <Skeleton variant="rounded" height={80} sx={{ mt: 2 }} />
    </Box>
  );
}

export function RouteSummary({ route }: { route: RouteRow }) {
  const { t } = useTranslation("common");
  const dispatch = useAppDispatch();
  const { db, complete, window, shapeLinesLayerId } = useExplorer();
  const date = useAppSelector((s) => s.explorer.date);
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
          <Box sx={{ display: "flex", pt: "2px" }}>
            <Icon iconName={modeIcon(mode)} style={{ fontSize: 12 }} htmlColor="inherit" />
          </Box>
          <Typography variant="caption">{meta}</Typography>
        </Stack>
      </Box>
      <Tabs value={0} variant="fullWidth" sx={{ minHeight: 40 }}>
        <Tab label={t("overview")} sx={{ minHeight: 40, minWidth: 0, px: 0.5, textTransform: "none" }} />
        {/* Shown where they will be, disabled until their phase. Direct
            children of Tabs: it hands its own props to each. */}
        {[t("explorer_tab_stops"), t("explorer_tab_service"), t("calendar")].map((label) => (
          <Tab
            key={label}
            label={label}
            disabled
            title={t("explorer_coming_later")}
            sx={{
              minHeight: 40,
              minWidth: 0,
              px: 0.5,
              textTransform: "none",
              "&.Mui-disabled": { pointerEvents: "auto" },
            }}
          />
        ))}
      </Tabs>
      <Box sx={{ flex: 1, minHeight: 0, overflowY: "auto", px: 2.5, borderTop: 1, borderColor: "divider" }}>
        {summary.error && (
          <Typography variant="body2" color="error" sx={{ mt: 2 }}>
            {summary.error}
          </Typography>
        )}
        {!summary.data && !summary.error && <OverviewSkeleton />}
        {summary.data && <Overview route={route} data={summary.data} />}
      </Box>
    </Stack>
  );
}
