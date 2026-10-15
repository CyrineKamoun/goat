/**
 * The Explorer's right panel (02-panel-behaviour-spec › Panel regions,
 * Viewers): one item at a time — a stop's timetable (board 13) or a trip
 * (board 7) — opened from the lists, the map, or one another. "‹" goes back
 * to the item it was opened from; ›› folds it to a rail on the map's right
 * edge with the item's badge, × closes it.
 *
 * Folding is the user's choice and sticks: opening another item while it is
 * folded only changes the rail's badge.
 */
import KeyboardDoubleArrowLeftIcon from "@mui/icons-material/KeyboardDoubleArrowLeft";
import KeyboardDoubleArrowRightIcon from "@mui/icons-material/KeyboardDoubleArrowRight";
import { Box, ButtonBase, IconButton, Stack, Tooltip, Typography } from "@mui/material";
import { useTranslation } from "react-i18next";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import { clock } from "@/lib/explorer/format";
import {
  type ExplorerPanelItem,
  closeExplorerPanel,
  explorerPanelBack,
  panelItem,
  setExplorerPanelCollapsed,
} from "@/lib/store/explorer/slice";

import { useRoutes, useTripDetail } from "@/hooks/explorer/useExplorerData";
import { useAppDispatch, useAppSelector } from "@/hooks/store/ContextHooks";

import { FloatingPanel } from "@/components/common/FloatingPanel";
import { useExplorer } from "@/components/explorer/ExplorerContext";
import { RouteBadge, routeName } from "@/components/explorer/RouteBadge";
import { StopMarker } from "@/components/explorer/StopMarker";
import { StopPanel } from "@/components/explorer/right/StopPanel";
import { TripPanel } from "@/components/explorer/right/TripPanel";
import { EXPLORER_RAIL_WIDTH, EXPLORER_RIGHT_PANEL_WIDTH } from "@/components/explorer/right/panelSize";

/** An item as the breadcrumb and the rail name it. */
function useItemName(item: ExplorerPanelItem | null): string {
  const { t } = useTranslation("common");
  const { db, complete } = useExplorer();
  const date = useAppSelector((s) => s.explorer.date);
  const trip = useTripDetail(
    item?.kind === "trip" && complete ? db : undefined,
    item?.kind === "trip" ? item.trip_idx : -1,
    item?.kind === "trip" ? item.run_dep_s : -1,
    date
  );
  if (!item) return "";
  if (item.kind === "stop") return item.stop.name;
  const first = trip.data?.stops.find((s) => s.dep_s !== null)?.dep_s ?? item.run_dep_s;
  return t("explorer_trip_at", { time: clock(first) });
}

/** The trip's route, for its badge on the rail. */
function useTripRoute(item: ExplorerPanelItem | null) {
  const { db, complete } = useExplorer();
  const date = useAppSelector((s) => s.explorer.date);
  const routes = useRoutes(db);
  const trip = useTripDetail(
    item?.kind === "trip" && complete ? db : undefined,
    item?.kind === "trip" ? item.trip_idx : -1,
    item?.kind === "trip" ? item.run_dep_s : -1,
    date
  );
  return routes.data?.find((r) => r.route_idx === trip.data?.route_idx);
}

export function ExplorerRightPanel() {
  const { t } = useTranslation("common");
  const dispatch = useAppDispatch();
  const panel = useAppSelector((s) => s.explorer.panel);
  const item = useAppSelector((s) => panelItem(s.explorer));
  const previous = panel.stack.length > 1 ? panel.stack[panel.stack.length - 2] : null;
  const fromRoute = useRoutes(useExplorer().db).data?.find((r) => r.route_id === panel.fromRoute);
  const previousName = useItemName(previous);
  const itemName = useItemName(item);
  const tripRoute = useTripRoute(item);
  if (!item) return null;

  if (panel.collapsed)
    return (
      <FloatingPanel
        width={EXPLORER_RAIL_WIDTH}
        minHeight={0}
        fillHeight
        sx={{ alignItems: "center", py: 1, gap: 1 }}>
        <Tooltip title={t("explorer_panel_unfold")} placement="left">
          <IconButton
            size="small"
            aria-label={t("explorer_panel_unfold")}
            aria-expanded={false}
            onClick={() => dispatch(setExplorerPanelCollapsed(false))}>
            <KeyboardDoubleArrowLeftIcon fontSize="small" />
          </IconButton>
        </Tooltip>
        {/* The item's badge: a click unfolds it. */}
        <Tooltip title={itemName} placement="left">
          <ButtonBase
            aria-label={itemName}
            onClick={() => dispatch(setExplorerPanelCollapsed(false))}
            sx={{ borderRadius: 1, p: 0.5, "&:hover": { bgcolor: "action.hover" } }}>
            {item.kind === "trip" && tripRoute ? <RouteBadge route={tripRoute} small /> : <StopMarker />}
          </ButtonBase>
        </Tooltip>
      </FloatingPanel>
    );

  return (
    <FloatingPanel width={EXPLORER_RIGHT_PANEL_WIDTH} maxHeight="100%" fillHeight>
      <Stack direction="row" alignItems="center" spacing={0.5} sx={{ px: 1.5, pt: 1, minHeight: 36 }}>
        <Box sx={{ flex: 1, minWidth: 0, display: "flex" }}>
          {previous ? (
            // Back to the item this one was opened from.
            <ButtonBase
              onClick={() => dispatch(explorerPanelBack())}
              sx={{ gap: 0.75, color: "text.secondary", borderRadius: 1, px: 0.5, maxWidth: "100%" }}>
              <Icon iconName={ICON_NAME.CHEVRON_LEFT} style={{ fontSize: 11 }} />
              <Typography variant="caption" noWrap>
                {previousName}
              </Typography>
            </ButtonBase>
          ) : fromRoute ? (
            // The route it belongs to, open in the left panel.
            <Stack direction="row" spacing={0.75} alignItems="center" sx={{ px: 0.5, minWidth: 0 }}>
              <RouteBadge route={fromRoute} small />
              <Typography variant="caption" color="text.secondary" noWrap>
                {routeName(fromRoute, t)}
              </Typography>
            </Stack>
          ) : null}
        </Box>
        <Tooltip title={t("explorer_panel_fold")}>
          <IconButton
            size="small"
            aria-label={t("explorer_panel_fold")}
            aria-expanded
            onClick={() => dispatch(setExplorerPanelCollapsed(true))}>
            <KeyboardDoubleArrowRightIcon fontSize="small" />
          </IconButton>
        </Tooltip>
        <Tooltip title={t("explorer_panel_close")}>
          <IconButton
            size="small"
            aria-label={t("explorer_panel_close")}
            onClick={() => dispatch(closeExplorerPanel())}>
            <Icon iconName={ICON_NAME.XCLOSE} style={{ fontSize: 14 }} htmlColor="inherit" />
          </IconButton>
        </Tooltip>
      </Stack>
      <Box sx={{ flex: 1, minHeight: 0, display: "flex", flexDirection: "column" }}>
        {item.kind === "stop" ? (
          <StopPanel key={item.stop.stop_idx} stop={item.stop} />
        ) : (
          <TripPanel
            key={`${item.trip_idx}:${item.run_dep_s}`}
            trip_idx={item.trip_idx}
            run_dep_s={item.run_dep_s}
          />
        )}
      </Box>
    </FloatingPanel>
  );
}
