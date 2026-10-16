/**
 * One run of a trip in the right panel (board 7): its route and where it
 * goes, the trips before and after it in its direction, and its stops on a
 * line with their times and the other routes there. While playback is within
 * the run, "Vehicle now" sits between the stops the vehicle is between, and
 * the map rings the vehicle. A stop opens its timetable, which goes back to
 * the trip.
 */
import { Box, ButtonBase, IconButton, Skeleton, Stack, Tooltip, Typography } from "@mui/material";
import { useMemo } from "react";
import { useTranslation } from "react-i18next";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import type { TripStop } from "@/lib/explorer/derive/trip";
import { clock, shortDate } from "@/lib/explorer/format";
import { modeOf, routeColor } from "@/lib/explorer/modes";
import { openExplorerPanel } from "@/lib/store/explorer/slice";

import { type RouteRow, useRoutes, useTripDetail } from "@/hooks/explorer/useExplorerData";
import { useAppDispatch, useAppSelector } from "@/hooks/store/ContextHooks";

import { useExplorer } from "@/components/explorer/ExplorerContext";
import { BADGES, RAIL, Rail, StopDot } from "@/components/explorer/LineDiagram";
import { RouteBadge, distinctLines, routeName } from "@/components/explorer/RouteBadge";
import { PX } from "@/components/explorer/SectionHeader";
import { usePlaybackMinute } from "@/components/explorer/right/usePlaybackMinute";

export function TripPanel({ trip_idx, run_dep_s }: { trip_idx: number; run_dep_s: number }) {
  const { t, i18n } = useTranslation("common");
  const dispatch = useAppDispatch();
  const { db, complete } = useExplorer();
  const date = useAppSelector((s) => s.explorer.date);
  const playing = useAppSelector((s) => s.explorer.playing);
  const now = usePlaybackMinute();
  const routes = useRoutes(db);
  const trip = useTripDetail(complete ? db : undefined, trip_idx, run_dep_s, date);
  const routeByIdx = useMemo(() => new Map((routes.data ?? []).map((r) => [r.route_idx, r])), [routes.data]);
  const data = trip.data;
  const route = data ? routeByIdx.get(data.route_idx) : undefined;
  const color = route ? routeColor(route.color, modeOf(route.route_type)) : undefined;

  const timed = data?.stops.filter((s) => (s.dep_s ?? s.arr_s) !== null) ?? [];
  const first = timed[0]?.dep_s ?? timed[0]?.arr_s ?? null;
  const last = timed[timed.length - 1]?.arr_s ?? timed[timed.length - 1]?.dep_s ?? null;
  // While playback is within the run: the stop the vehicle is at, or heading
  // to — the first it has not yet left.
  const running = playing && now !== null && first !== null && last !== null && now >= first && now <= last;
  const current = running
    ? (data?.stops.findIndex((s) => (s.dep_s ?? s.arr_s ?? -Infinity) >= (now as number)) ?? -1)
    : -1;

  const step = (to: { trip_idx: number; run_dep_s: number } | null) =>
    to && dispatch(openExplorerPanel({ item: { kind: "trip", ...to }, step: true }));
  const openStop = (s: TripStop) =>
    dispatch(
      openExplorerPanel({
        item: { kind: "stop", stop: { stop_idx: s.stop_idx, name: s.name, lon: s.lon, lat: s.lat } },
        push: true,
      })
    );

  if (trip.error)
    return (
      <Typography variant="body2" color="error" sx={{ px: PX, mt: 2 }}>
        {trip.error}
      </Typography>
    );
  if (!data || !route)
    return (
      <Box sx={{ px: PX }}>
        <Skeleton variant="text" width="60%" height={32} />
        <Skeleton variant="rounded" height={320} sx={{ mt: 2 }} />
      </Box>
    );

  const destination = data.destination ?? data.headsign;
  const meta = [
    routeName(route, t),
    destination
      ? t("explorer_direction_to", { place: destination, interpolation: { escapeValue: false } })
      : null,
    date ? shortDate(date, i18n.language) : null,
  ].filter(Boolean);
  const minutes = first !== null && last !== null ? Math.round((last - first) / 60) : null;

  return (
    <Stack sx={{ flex: 1, minHeight: 0 }}>
      <Box sx={{ px: PX, pt: 0.5, pb: 1.5 }}>
        <Stack direction="row" spacing={1.25} alignItems="center">
          <RouteBadge route={route} />
          <Typography variant="h6" fontWeight="bold" noWrap sx={{ fontSize: 17 }}>
            {t("explorer_trip_at", { time: clock(first) })}
          </Typography>
        </Stack>
        <Typography variant="caption" color="text.secondary" display="block" sx={{ mt: 0.75 }}>
          {meta.join(" · ")}
        </Typography>
        {/* The trips before and after, in the same direction on the date: a
            pill stepping between them, as the bottom bar's date. */}
        <Stack
          direction="row"
          alignItems="center"
          sx={{ mt: 1.5, height: 32, px: 0.25, borderRadius: "16px", bgcolor: "action.hover" }}>
          <Tooltip title={t("explorer_trip_previous")}>
            <span>
              <IconButton
                size="small"
                aria-label={t("explorer_trip_previous")}
                disabled={!data.prev}
                onClick={() => step(data.prev)}
                sx={{ width: 28, height: 28, color: "text.secondary" }}>
                <Icon iconName={ICON_NAME.CHEVRON_LEFT} style={{ fontSize: 10 }} />
              </IconButton>
            </span>
          </Tooltip>
          <Typography
            variant="caption"
            noWrap
            sx={{ flex: 1, textAlign: "center", fontVariantNumeric: "tabular-nums" }}>
            <Box component="span" sx={{ fontWeight: 700 }}>
              {first !== null && last !== null ? `${clock(first)} – ${clock(last)}` : ""}
            </Box>
            <Box component="span" sx={{ color: "text.secondary" }}>
              {[
                minutes !== null ? `${minutes} ${t("explorer_minutes_short")}` : null,
                t("explorer_stops", { count: data.stops.length }),
              ]
                .filter(Boolean)
                .map((x) => ` · ${x}`)
                .join("")}
            </Box>
          </Typography>
          <Tooltip title={t("explorer_trip_next")}>
            <span>
              <IconButton
                size="small"
                aria-label={t("explorer_trip_next")}
                disabled={!data.next}
                onClick={() => step(data.next)}
                sx={{ width: 28, height: 28, color: "text.secondary" }}>
                <Icon iconName={ICON_NAME.CHEVRON_RIGHT} style={{ fontSize: 10 }} />
              </IconButton>
            </span>
          </Tooltip>
        </Stack>
      </Box>
      <Box
        sx={{
          flex: 1,
          minHeight: 0,
          overflowY: "auto",
          px: PX,
          py: 1,
          borderTop: 1,
          borderColor: "divider",
        }}>
        {data.stops.map((s, i) => (
          <Box key={`${s.stop_idx}:${i}`}>
            <StopLine
              stop={s}
              color={color as string}
              first={i === 0}
              last={i === data.stops.length - 1}
              passed={running && i < current}
              current={i === current}
              routeByIdx={routeByIdx}
              onOpen={() => openStop(s)}
            />
          </Box>
        ))}
      </Box>
    </Stack>
  );
}

/** A stop on the trip's line, as the Stops tab draws a stop: its dot on the
 * rail, its name and the other lines there, its time. */
function StopLine({
  stop,
  color,
  first,
  last,
  passed,
  current,
  routeByIdx,
  onOpen,
}: {
  stop: TripStop;
  color: string;
  first: boolean;
  last: boolean;
  passed: boolean;
  /** Where the vehicle is now, or heading: shaded. */
  current: boolean;
  routeByIdx: Map<number, RouteRow>;
  onOpen: () => void;
}) {
  const end = first || last;
  const others = distinctLines(stop.transfers.map((i) => routeByIdx.get(i)));
  const time = last ? (stop.arr_s ?? stop.dep_s) : (stop.dep_s ?? stop.arr_s);
  return (
    <ButtonBase
      onClick={onOpen}
      sx={{
        position: "relative",
        width: "100%",
        justifyContent: "flex-start",
        textAlign: "left",
        gap: 1.5,
        minHeight: 32,
        borderRadius: 1,
        bgcolor: current ? "action.selected" : "transparent",
        "&:hover": { bgcolor: current ? "action.selected" : "action.hover" },
      }}>
      <Box sx={{ position: "relative", width: RAIL, alignSelf: "stretch", flexShrink: 0 }}>
        <Rail color={color} top={!first} bottom={!last} />
        <StopDot color={color} end={end} />
      </Box>
      <Stack
        direction="row"
        alignItems="center"
        spacing={0.75}
        sx={{ flex: 1, minWidth: 0, py: 0.5, opacity: passed ? 0.55 : 1 }}>
        <Typography variant="body2" fontWeight={end ? 700 : 500} noWrap title={stop.name}>
          {stop.name}
        </Typography>
        {others.slice(0, BADGES).map((r) => (
          <RouteBadge key={r.route_idx} route={r} small />
        ))}
        {others.length > BADGES && (
          <Typography variant="caption" color="text.secondary">
            +{others.length - BADGES}
          </Typography>
        )}
      </Stack>
      <Typography
        variant="body2"
        color={passed ? "text.disabled" : "text.secondary"}
        sx={{ flexShrink: 0, fontVariantNumeric: "tabular-nums", pr: 0.5 }}>
        {clock(time)}
      </Typography>
    </ButtonBase>
  );
}
