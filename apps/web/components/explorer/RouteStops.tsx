/**
 * A route's Stops tab (board 10): the destination picker — directions, each
 * with its variants under it — and the chosen pattern as a line diagram,
 * with distances and minutes between stops, minutes from the start, the
 * other routes at each stop, and where short turns end. Picking a stop
 * shows it on the map.
 */
import { Box, ButtonBase, Stack, Typography } from "@mui/material";
import { useTheme } from "@mui/material/styles";
import { useEffect } from "react";
import { useTranslation } from "react-i18next";

import type { Direction, VariantKind } from "@/lib/explorer/derive/directions";
import type { LineStop } from "@/lib/explorer/derive/stopsLine";
import { count } from "@/lib/explorer/format";
import { modeOf, routeColor } from "@/lib/explorer/modes";
import {
  closeExplorerPanel,
  openExplorerPanel,
  pickExplorerPattern,
  pickedStop,
} from "@/lib/store/explorer/slice";

import type { SelectorItem } from "@/types/map/common";

import { type RouteRow, useRoutes, useStopsLine } from "@/hooks/explorer/useExplorerData";
import { useAppDispatch, useAppSelector } from "@/hooks/store/ContextHooks";

import { useExplorer } from "@/components/explorer/ExplorerContext";
import { BADGES, RAIL, Rail, StopDot } from "@/components/explorer/LineDiagram";
import { RouteBadge, distinctLines } from "@/components/explorer/RouteBadge";
import { directionLabel } from "@/components/explorer/directionWords";
import Selector from "@/components/map/panels/common/Selector";

const KIND_KEY: Record<Exclude<VariantKind, "main">, string> = {
  short_turn: "explorer_variant_short_turn",
  express: "explorer_variant_express",
  branch: "explorer_variant_branch",
};

function km(m: number | null, locale: string): string | null {
  if (m === null) return null;
  return m < 1000
    ? `${count(Math.round(m / 10) * 10, locale)} m`
    : `${new Intl.NumberFormat(locale, { maximumFractionDigits: 1 }).format(m / 1000)} km`;
}

function StopRow({
  stop,
  color,
  first,
  last,
  routeByIdx,
  selected,
  onPick,
}: {
  stop: LineStop;
  color: string;
  first: boolean;
  last: boolean;
  routeByIdx: Map<number, RouteRow>;
  selected: boolean;
  onPick: () => void;
}) {
  const end = first || last;
  const lines = distinctLines(stop.transfers.map((i) => routeByIdx.get(i)));
  return (
    <ButtonBase
      onClick={onPick}
      sx={{
        position: "relative",
        width: "100%",
        justifyContent: "flex-start",
        textAlign: "left",
        gap: 1.5,
        minHeight: 32,
        borderRadius: 1,
        bgcolor: selected ? "action.selected" : "transparent",
        "&:hover": { bgcolor: selected ? "action.selected" : "action.hover" },
      }}>
      <Box sx={{ position: "relative", width: RAIL, alignSelf: "stretch", flexShrink: 0 }}>
        <Rail color={color} top={!first} bottom={!last} />
        <StopDot color={color} end={end} />
      </Box>
      <Stack direction="row" alignItems="center" spacing={0.75} sx={{ flex: 1, minWidth: 0, py: 0.5 }}>
        <Typography variant="body2" fontWeight={end ? 700 : 500} noWrap title={stop.name}>
          {stop.name}
        </Typography>
        {lines.slice(0, BADGES).map((r) => (
          <RouteBadge key={r.route_idx} route={r} small />
        ))}
        {lines.length > BADGES && (
          <Typography variant="caption" color="text.secondary">
            +{lines.length - BADGES}
          </Typography>
        )}
      </Stack>
      <Typography
        variant="body2"
        color="text.secondary"
        sx={{ flexShrink: 0, fontVariantNumeric: "tabular-nums", pr: 0.5 }}>
        {stop.minutes !== null ? `${stop.minutes}′` : "–"}
      </Typography>
    </ButtonBase>
  );
}

/** Between two stops: how far, how long; and where short turns end. */
function HopRow({ stop, color, last }: { stop: LineStop; color: string; last: boolean }) {
  const { t, i18n } = useTranslation("common");
  const theme = useTheme();
  const turn = theme.palette.warning.main;
  const hop = [
    km(stop.next_m, i18n.language),
    stop.next_min !== null ? t("explorer_n_min", { count: stop.next_min }) : null,
  ]
    .filter(Boolean)
    .join(" · ");
  return (
    <>
      {stop.short_turns.map((s, k) => (
        <Stack
          // Two short turns may end here for one destination.
          key={k}
          direction="row"
          alignItems="center"
          spacing={1.5}
          sx={{ position: "relative", minHeight: 24 }}>
          <Box sx={{ position: "relative", width: RAIL, alignSelf: "stretch", flexShrink: 0 }}>
            <Rail color={color} top bottom />
            {/* A dashed tick off the rail: trips leave the line here. */}
            <Box
              sx={{
                position: "absolute",
                left: RAIL / 2 + 2,
                top: "50%",
                width: 12,
                borderTop: `2px dashed ${turn}`,
              }}
            />
          </Box>
          <Typography variant="caption" fontWeight={600} sx={{ color: turn }}>
            {t("explorer_short_turn_ends", { count: s.trips })}
          </Typography>
        </Stack>
      ))}
      {!last && (
        <Stack direction="row" alignItems="center" spacing={1.5} sx={{ position: "relative", minHeight: 22 }}>
          <Box sx={{ position: "relative", width: RAIL, alignSelf: "stretch", flexShrink: 0 }}>
            <Rail color={color} top bottom />
          </Box>
          <Typography variant="caption" color="text.disabled">
            {hop}
          </Typography>
        </Stack>
      )}
    </>
  );
}

function optionValue(direction: number, pattern: number) {
  return `${direction}:${pattern}`;
}

export function RouteStops({ route, directions }: { route: RouteRow; directions: Direction[] }) {
  const { t, i18n } = useTranslation("common");
  const dispatch = useAppDispatch();
  const { db, complete, shapeDistUnit, settings } = useExplorer();
  const date = useAppSelector((s) => s.explorer.date);
  const choice = useAppSelector((s) => s.explorer.pattern);
  const picked = useAppSelector((s) => pickedStop(s.explorer));
  const routes = useRoutes(db);
  const routeByIdx = new Map((routes.data ?? []).map((r) => [r.route_idx, r]));
  const lang = i18n.language;
  const color = routeColor(route.color, modeOf(route.route_type));

  const direction = directions.find((d) => d.key === choice?.direction) ?? directions[0];
  const pattern_idx =
    choice && direction?.patterns.some((p) => p.pattern_idx === choice.pattern_idx)
      ? choice.pattern_idx
      : direction?.key;
  // The picker's choice is the store's, which the map highlights: the first
  // direction's main pattern until another is picked.
  useEffect(() => {
    if (!choice && direction)
      dispatch(pickExplorerPattern({ direction: direction.key, pattern_idx: direction.key }));
  }, [choice, direction, dispatch]);

  const line = useStopsLine(
    complete ? db : undefined,
    route,
    direction,
    pattern_idx,
    date,
    shapeDistUnit,
    settings.periods
  );

  if (!direction) {
    return (
      <Typography variant="body2" color="text.secondary" sx={{ mt: 2 }}>
        {t("explorer_no_service")}
      </Typography>
    );
  }

  const variantLabel = (p: { kind: VariantKind; destination: string }) =>
    t(KIND_KEY[p.kind as Exclude<VariantKind, "main">], {
      destination: p.destination,
      interpolation: { escapeValue: false },
    });
  const items: SelectorItem[] = directions.flatMap((d) => [
    {
      value: optionValue(d.key, d.key),
      label: directionLabel(d.name, t),
      iconNode: <RouteBadge route={route} small />,
    },
    ...d.patterns
      .filter((p) => p.kind !== "main")
      .map((p) => ({
        value: optionValue(d.key, p.pattern_idx),
        // A busy route has many variants to one place: its trips tell them apart.
        label: `${variantLabel(p)} · ${t("explorer_trips", { count: p.n_trips })}`,
        // A variant sits under its direction, where the badge would be.
        iconNode: (
          <Box component="span" sx={{ width: 28, textAlign: "center", color: "text.secondary" }}>
            ↳
          </Box>
        ),
      })),
  ]);
  const variant = direction.patterns.find((p) => p.pattern_idx === pattern_idx);
  const trips = variant && variant.kind !== "main" ? variant.n_trips : direction.n_trips;
  const data = line.data;
  const summary = data
    ? [
        t("explorer_trips", { count: trips }),
        t("explorer_stops", { count: data.stops.length }),
        km(data.length_m, lang),
        [
          data.offpeak_min !== null ? t("explorer_min_offpeak", { count: data.offpeak_min }) : null,
          data.peak_min !== null ? t("explorer_min_peak", { count: data.peak_min }) : null,
        ]
          .filter(Boolean)
          .join(", ") || null,
      ]
        .filter(Boolean)
        .join(" · ")
    : null;

  return (
    <Box sx={{ pt: 2, pb: 2 }}>
      {/* GOAT's own selector, as the toolbox's: directions with the route's
          badge, each with its variants under it. */}
      <Selector
        label={t("explorer_direction")}
        items={items}
        selectedItems={items.find((it) => it.value === optionValue(direction.key, pattern_idx as number))}
        setSelectedItems={(item) => {
          if (!item || Array.isArray(item)) return;
          const [d, p] = String(item.value).split(":").map(Number);
          dispatch(pickExplorerPattern({ direction: d, pattern_idx: p }));
        }}
      />

      <Typography
        variant="caption"
        color="text.secondary"
        display="block"
        sx={{ mt: 1, mb: 1.5, minHeight: 18 }}>
        {summary ?? t("explorer_calculating")}
        {data && !data.of_date && ` · ${t("explorer_times_other_days")}`}
      </Typography>

      {data && (
        <Box>
          {data.stops.map((s, i) => {
            const last = i === data.stops.length - 1;
            return (
              <Box key={`${s.stop_idx}:${i}`}>
                <StopRow
                  stop={s}
                  color={color}
                  first={i === 0}
                  last={last}
                  routeByIdx={routeByIdx}
                  selected={picked?.stop_idx === s.stop_idx}
                  // Its timetable in the right panel; picked again, closed.
                  onPick={() =>
                    dispatch(
                      picked?.stop_idx === s.stop_idx
                        ? closeExplorerPanel()
                        : openExplorerPanel({
                            item: {
                              kind: "stop",
                              stop: { stop_idx: s.stop_idx, name: s.name, lat: s.lat, lon: s.lon },
                            },
                            fromRoute: route.route_id,
                          })
                    )
                  }
                />
                <HopRow stop={s} color={color} last={last} />
              </Box>
            );
          })}
        </Box>
      )}
    </Box>
  );
}
