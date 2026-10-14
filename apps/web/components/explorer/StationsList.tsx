/**
 * The Explorer tab's "Stations and platforms": stations with their platforms
 * folded under them, and stops that belong to no station on their own. A
 * search finds platforms too, and opens their station to show them; a route
 * filter keeps the places and platforms its routes call at.
 */
import { Box, IconButton, ListItemButton, Stack, Typography } from "@mui/material";
import { useTheme } from "@mui/material/styles";
import { type ReactNode, useState } from "react";
import { useTranslation } from "react-i18next";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import type { PlaceRow, PlatformRow } from "@/lib/explorer/derive/stops";
import type { ListedPlace } from "@/lib/explorer/filters";
import { selectExplorerStop } from "@/lib/store/explorer/slice";

import type { RouteRow } from "@/hooks/explorer/useExplorerData";
import { useAppDispatch, useAppSelector } from "@/hooks/store/ContextHooks";

import { RouteBadge } from "@/components/explorer/RouteBadge";
import { PX } from "@/components/explorer/SectionHeader";
import { StopMarker } from "@/components/explorer/StopMarker";

/** Route badges a row shows before "+n". */
const BADGES = 3;

function Badges({ routes }: { routes: RouteRow[] }) {
  return (
    <Stack direction="row" spacing={0.5} alignItems="center" sx={{ flexShrink: 0 }}>
      {routes.slice(0, BADGES).map((r) => (
        <RouteBadge key={r.route_idx} route={r} small />
      ))}
      {routes.length > BADGES && (
        <Typography variant="caption" color="text.secondary">
          +{routes.length - BADGES}
        </Typography>
      )}
    </Stack>
  );
}

/**
 * The rows' grid. Every row: [fold][marker][name][badges], padded and spaced
 * as the route list's rows (`PX`, a gap of 1.5). A platform's marker sits
 * under its station's name; the guide line it hangs off runs through the
 * station's marker.
 */
function useGrid() {
  const theme = useTheme();
  // The theme's spacing is in rem: the offsets are worked out by the browser.
  return { pad: theme.spacing(PX), gap: theme.spacing(1.5), fold: "16px", marker: "16px" };
}

function Row({
  selected,
  onClick,
  indent,
  nested,
  lead,
  station,
  label,
  note,
  routes,
}: {
  selected: boolean;
  onClick: () => void;
  indent: boolean;
  /** Whether any row of the list has platforms, so needs a fold column. */
  nested: boolean;
  lead?: ReactNode;
  /** A station gets the larger marker. */
  station: boolean;
  label: string;
  note?: string;
  routes: RouteRow[];
}) {
  const GRID = useGrid();
  // Where the markers of top-level rows start: after the fold column, if any.
  const markerAt = nested ? `${GRID.pad} + ${GRID.fold} + ${GRID.gap}` : GRID.pad;
  return (
    <ListItemButton
      selected={selected}
      onClick={onClick}
      sx={{
        pl: indent ? `calc(${markerAt} + ${GRID.marker} + ${GRID.gap})` : GRID.pad,
        pr: GRID.pad,
        py: 0.75,
        gap: GRID.gap,
        minHeight: 36,
        position: "relative",
      }}>
      {indent && (
        <Box
          sx={{
            position: "absolute",
            left: `calc(${markerAt} + ${GRID.marker} / 2)`,
            top: 0,
            bottom: 0,
            borderLeft: 1,
            borderColor: "divider",
          }}
        />
      )}
      {!indent && nested && (
        <Box sx={{ width: GRID.fold, display: "flex", justifyContent: "center", flexShrink: 0 }}>{lead}</Box>
      )}
      <Box sx={{ display: "flex", width: GRID.marker, justifyContent: "center", flexShrink: 0 }}>
        <StopMarker station={station} size={indent ? 9 : 11} />
      </Box>
      <Typography variant="body2" noWrap title={label} sx={{ flex: 1, minWidth: 0 }}>
        {label}
        {note && (
          <Typography component="span" variant="caption" color="text.secondary">
            {" · "}
            {note}
          </Typography>
        )}
      </Typography>
      <Badges routes={routes} />
    </ListItemButton>
  );
}

export function StationsList({
  listed,
  routeByIdx,
}: {
  listed: ListedPlace[];
  routeByIdx: Map<number, RouteRow>;
}) {
  const { t } = useTranslation("common");
  const dispatch = useAppDispatch();
  const selected = useAppSelector((s) => s.explorer.selectedStop);
  const [open, setOpen] = useState<Set<number>>(new Set());
  const nested = listed.some((l) => l.platforms.length > 0);

  // One badge per number and colour: a feed may run several routes as "S3"
  // (per operator, per variant), and the row means the line, not the route.
  const routes = (idxs: number[]) => {
    const seen = new Set<string>();
    return idxs
      .map((i) => routeByIdx.get(i))
      .filter((r): r is RouteRow => {
        if (!r) return false;
        const key = `${r.short_name || r.long_name || r.route_id}|${r.color}|${r.route_type}`;
        if (seen.has(key)) return false;
        seen.add(key);
        return true;
      });
  };
  const toggle = (idx: number) =>
    setOpen((s) => {
      const next = new Set(s);
      if (!next.delete(idx)) next.add(idx);
      return next;
    });
  // Picking what is picked already lets go of it.
  const pick = (stop: {
    stop_idx: number;
    stop_id: string;
    name: string | null;
    lat: number | null;
    lon: number | null;
  }) =>
    dispatch(
      selectExplorerStop(
        selected?.stop_idx === stop.stop_idx || stop.lat === null || stop.lon === null
          ? null
          : { stop_idx: stop.stop_idx, name: stop.name ?? stop.stop_id, lon: stop.lon, lat: stop.lat }
      )
    );
  const platformLabel = (place: PlaceRow, p: PlatformRow) => {
    const code = p.platform_code ? t("explorer_platform", { code: p.platform_code }) : null;
    // The station's name again says nothing, nor does an id: a platform with
    // neither code nor name of its own is "Platform", told apart by its routes.
    const own = p.name && p.name !== place.name ? p.name : null;
    return { label: code ?? own ?? t("explorer_platform_unnamed"), note: code && own ? own : undefined };
  };

  return (
    <>
      {listed.map(({ place, platforms, opened }) => {
        const isOpen = opened || open.has(place.stop_idx);
        return (
          <Box key={place.stop_idx}>
            <Row
              selected={selected?.stop_idx === place.stop_idx}
              onClick={() => {
                pick(place);
                if (platforms.length && !isOpen) toggle(place.stop_idx);
              }}
              indent={false}
              nested={nested}
              lead={
                platforms.length > 0 && (
                  <IconButton
                    size="small"
                    aria-label={isOpen ? t("explorer_fold") : t("explorer_unfold")}
                    aria-expanded={isOpen}
                    onClick={(e) => {
                      e.stopPropagation();
                      toggle(place.stop_idx);
                    }}
                    sx={{ p: 0.25 }}>
                    <Icon
                      iconName={isOpen ? ICON_NAME.CHEVRON_DOWN : ICON_NAME.CHEVRON_RIGHT}
                      style={{ fontSize: 10 }}
                    />
                  </IconButton>
                )
              }
              station={place.is_station}
              label={place.name || place.stop_id}
              note={
                place.is_station && platforms.length
                  ? t("explorer_platforms", { count: place.platforms.length })
                  : undefined
              }
              routes={routes(place.route_idxs)}
            />
            {isOpen &&
              platforms.map((p) => {
                const { label, note } = platformLabel(place, p);
                return (
                  <Row
                    key={p.stop_idx}
                    selected={selected?.stop_idx === p.stop_idx}
                    onClick={() => pick(p)}
                    indent
                    nested={nested}
                    station={false}
                    label={label}
                    note={note}
                    routes={routes(p.route_idxs)}
                  />
                );
              })}
          </Box>
        );
      })}
    </>
  );
}
