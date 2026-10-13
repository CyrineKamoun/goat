/**
 * A route's summary in the left panel (board 6): its name and figures for the
 * Explorer's date. Overview only for now; Stops, Service and Calendar come
 * with later phases and are shown, disabled, where they will be.
 */
import { Box, ButtonBase, Stack, Tab, Tabs, Tooltip, Typography } from "@mui/material";
import { useEffect } from "react";
import { useTranslation } from "react-i18next";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import { DEFAULT_PERIODS } from "@/lib/explorer/derive/operations";
import { clock, count, euros, rate as formatRate, shortDate } from "@/lib/explorer/format";
import { modeOf, routeColor } from "@/lib/explorer/modes";
import { selectExplorerRoute, setExplorerDrawing } from "@/lib/store/explorer/slice";

import { type RouteRow, type RouteSummaryData, useRouteSummary } from "@/hooks/explorer/useExplorerData";
import { useAppDispatch, useAppSelector } from "@/hooks/store/ContextHooks";

import { useExplorer } from "@/components/explorer/ExplorerContext";
import { RouteBadge } from "@/components/explorer/RouteBadge";
import { VehiclesCurve } from "@/components/explorer/VehiclesCurve";

function SectionTitle({ title, aside }: { title: string; aside?: string }) {
  return (
    <Stack direction="row" justifyContent="space-between" alignItems="baseline" sx={{ mt: 2, mb: 1 }}>
      <Typography variant="caption" fontWeight="bold" sx={{ textTransform: "uppercase", letterSpacing: 0.5 }}>
        {title}
      </Typography>
      {aside && (
        <Typography variant="caption" color="text.secondary">
          {aside}
        </Typography>
      )}
    </Stack>
  );
}

function Tile({ value, label }: { value: string; label: string }) {
  return (
    <Box sx={{ flex: 1, border: 1, borderColor: "divider", borderRadius: 1, px: 1.25, py: 1 }}>
      <Typography variant="body1" fontWeight="bold">
        {value}
      </Typography>
      <Typography variant="caption" color="text.secondary">
        {label}
      </Typography>
    </Box>
  );
}

function ServiceTable({ data }: { data: RouteSummaryData }) {
  const { t } = useTranslation("common");
  const cols = data.directions;
  return (
    <Box sx={{ border: 1, borderColor: "divider", borderRadius: 1, overflow: "hidden" }}>
      <Stack direction="row" sx={{ px: 1.25, py: 0.75, bgcolor: "action.hover" }}>
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
            sx={{ width: 76, textAlign: "right" }}>
            {c.direction.label}
          </Typography>
        ))}
      </Stack>
      {DEFAULT_PERIODS.map((p) => (
        <Stack key={p.key} direction="row" sx={{ px: 1.25, py: 0.6, borderTop: 1, borderColor: "divider" }}>
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
                sx={{ width: 76, textAlign: "right" }}>
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
      <SectionTitle
        title={t("explorer_service")}
        aside={t("explorer_timetable_of", { date: shortDate(date, lang) })}
      />
      <ServiceTable data={data} />
      <Typography variant="caption" color="text.secondary" display="block" sx={{ mt: 1 }}>
        {t("explorer_first_last_trips", {
          first: clock(ops.first_dep_s),
          last: clock(ops.last_dep_s),
          count: ops.runs,
        })}
      </Typography>

      <SectionTitle title={t("explorer_operations")} aside={t("explorer_per_day")} />
      <Stack direction="row" spacing={1}>
        <Tile value={count(ops.vehicle_hours, lang)} label={t("explorer_vehicle_hours")} />
        <Tile value={count(ops.vehicle_km, lang)} label={t("explorer_vehicle_km")} />
        <Tooltip title={ops.peak_from_blocks ? "" : t("explorer_peak_estimated")}>
          <Box sx={{ flex: 1, display: "flex" }}>
            <Tile value={count(ops.peak_vehicles, lang)} label={t("explorer_peak_vehicles")} />
          </Box>
        </Tooltip>
      </Stack>
      <Stack direction="row" justifyContent="space-between" sx={{ mt: 1.5 }}>
        <Typography variant="caption" color="text.secondary">
          {t("explorer_vehicles_in_service", { date: shortDate(date, lang) })}
        </Typography>
        <Typography variant="caption" color="text.secondary">
          {t("explorer_peak_at", { count: ops.peak_vehicles, time: clock(ops.peak_at_s) })}
        </Typography>
      </Stack>
      <VehiclesCurve
        values={data.curve}
        color={routeColor(route.color, modeOf(route.route_type))}
        markIndex={peakIndex}
      />

      <SectionTitle title={t("explorer_operating_cost")} />
      <Typography variant="h6" fontWeight="bold">
        ≈ {euros(data.annual.cost, lang)}{" "}
        <Typography component="span" variant="body2" color="text.secondary">
          {t("explorer_a_year")}
        </Typography>
      </Typography>
      <Typography variant="body2">
        {t("explorer_cost_km_line", {
          km: count(data.annual.vehicle_km, lang),
          rate: formatRate(data.rate.per_km, lang),
        })}
      </Typography>
      <Typography variant="body2" color="text.secondary">
        {t("explorer_cost_hours_line", { hours: count(data.annual.vehicle_hours, lang) })}
      </Typography>
      <Typography variant="caption" color="text.secondary" display="block" sx={{ mt: 1, mb: 2 }}>
        {t("explorer_cost_note")}
      </Typography>
    </>
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
    t(`explorer_mode_${mode}`),
    route.agency,
    summary.data ? t("explorer_directions", { count: summary.data.directions.length }) : null,
    summary.data ? t("explorer_stops", { count: summary.data.n_stops }) : null,
  ]
    .filter(Boolean)
    .join(" · ");

  return (
    <Stack sx={{ height: "100%", minHeight: 0 }}>
      <Box sx={{ px: 2, pt: 1.5 }}>
        <ButtonBase
          onClick={() => dispatch(selectExplorerRoute(null))}
          sx={{ gap: 0.75, color: "text.secondary", borderRadius: 1, px: 0.5 }}>
          <Icon iconName={ICON_NAME.CHEVRON_LEFT} style={{ fontSize: 11 }} />
          <Typography variant="caption">{t("explorer_routes")}</Typography>
        </ButtonBase>
        <Stack direction="row" spacing={1.25} alignItems="center" sx={{ mt: 1 }}>
          <RouteBadge route={route} />
          <Typography variant="body1" fontWeight="bold" noWrap>
            {route.long_name || route.short_name}
          </Typography>
        </Stack>
        <Typography variant="caption" color="text.secondary" display="block" sx={{ mt: 0.5 }}>
          {meta}
        </Typography>
      </Box>
      <Tabs value={0} variant="scrollable" sx={{ px: 1, minHeight: 40 }}>
        <Tab label={t("overview")} sx={{ minHeight: 40, textTransform: "none" }} />
        {/* Shown where they will be, disabled until their phase. Direct
            children of Tabs: it hands its own props to each. */}
        {[t("explorer_tab_stops"), t("explorer_tab_service"), t("calendar")].map((label) => (
          <Tab
            key={label}
            label={label}
            disabled
            title={t("explorer_coming_later")}
            sx={{ minHeight: 40, textTransform: "none", "&.Mui-disabled": { pointerEvents: "auto" } }}
          />
        ))}
      </Tabs>
      <Box sx={{ flex: 1, minHeight: 0, overflowY: "auto", px: 2, borderTop: 1, borderColor: "divider" }}>
        {summary.error && (
          <Typography variant="body2" color="error" sx={{ mt: 2 }}>
            {summary.error}
          </Typography>
        )}
        {!summary.data && !summary.error && (
          <Typography variant="body2" color="text.secondary" sx={{ mt: 2 }}>
            {t("loading")}
          </Typography>
        )}
        {summary.data && <Overview route={route} data={summary.data} />}
      </Box>
    </Stack>
  );
}
