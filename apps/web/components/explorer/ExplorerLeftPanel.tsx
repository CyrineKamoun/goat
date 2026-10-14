/**
 * The Explorer's left panel (board 3): one panel headed by a tab bar —
 * Layers | Explorer. It neither closes nor collapses: Finish editing is the
 * only way out of the Explorer
 * (docs/pt-network-editor/docs/02-panel-behaviour-spec.md).
 */
import { Box, LinearProgress, Stack, Tab, Tabs, Typography } from "@mui/material";
import type { ReactNode } from "react";
import { useTranslation } from "react-i18next";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import { type ExplorerLeftTab, setExplorerLeftTab } from "@/lib/store/explorer/slice";

import { useAppDispatch, useAppSelector } from "@/hooks/store/ContextHooks";

import { FloatingPanel } from "@/components/common/FloatingPanel";
import { useExplorer } from "@/components/explorer/ExplorerContext";
import { ExplorerTab } from "@/components/explorer/ExplorerTab";

export const EXPLORER_PANEL_WIDTH = 312;
export const EXPLORER_PANEL_WIDTH_ROUTE = 380;

function Status() {
  const { t } = useTranslation("common");
  const { state } = useExplorer();
  if (state.status === "error") {
    return (
      <Typography variant="body2" color="error" sx={{ p: 2 }}>
        {t("explorer_error", { error: state.error })}
      </Typography>
    );
  }
  if (state.status === "preparing") {
    return (
      <Box sx={{ p: 2 }}>
        <Typography variant="body2" color="text.secondary" sx={{ mb: 1 }}>
          {t("explorer_preparing")}
        </Typography>
        <LinearProgress />
      </Box>
    );
  }
  if (state.status === "loading") {
    const percent = state.progress.total
      ? Math.round((100 * state.progress.received) / state.progress.total)
      : 0;
    return (
      <Box sx={{ px: 2, py: 1 }}>
        <Typography variant="caption" color="text.secondary">
          {t("explorer_loading", { percent })}
        </Typography>
        <LinearProgress variant="determinate" value={percent} />
      </Box>
    );
  }
  return null;
}

export function ExplorerLeftPanel({ layersTab }: { layersTab: ReactNode }) {
  const { t } = useTranslation("common");
  const dispatch = useAppDispatch();
  const tab = useAppSelector((s) => s.explorer.leftTab);
  const routeOpen = useAppSelector((s) => !!s.explorer.selectedRouteId);
  const { db, state } = useExplorer();
  const width = tab === "explorer" && routeOpen ? EXPLORER_PANEL_WIDTH_ROUTE : EXPLORER_PANEL_WIDTH;

  // One panel: the tab bar is its header, the active tab fills the rest. Both
  // tabs stay mounted, so switching is a tab change, never a reload — the
  // route search and scroll position survive a look at the layers.
  return (
    <FloatingPanel width={width} maxHeight="100%" fillHeight={tab === "explorer"}>
      {/* Flush with the panel: plain tabs under its own corners, the active
          one underlined, rather than a control set inside it. */}
      <Tabs
        value={tab}
        variant="fullWidth"
        onChange={(_e, value: ExplorerLeftTab) => dispatch(setExplorerLeftTab(value))}
        sx={{ minHeight: 44, flexShrink: 0, borderBottom: 1, borderColor: "divider" }}>
        <Tab
          value="layers"
          iconPosition="start"
          icon={<Icon iconName={ICON_NAME.LAYERS} style={{ fontSize: 14 }} />}
          label={t("layers")}
          sx={{ minHeight: 44, textTransform: "none", gap: 0.5 }}
        />
        <Tab
          value="explorer"
          iconPosition="start"
          icon={<Icon iconName={ICON_NAME.BUS} style={{ fontSize: 14 }} />}
          label={t("explorer")}
          sx={{ minHeight: 44, textTransform: "none", gap: 0.5 }}
        />
      </Tabs>
      <Box
        sx={{
          display: tab === "layers" ? "flex" : "none",
          flexDirection: "column",
          minHeight: 0,
          overflow: "auto",
        }}>
        {layersTab}
      </Box>
      <Stack sx={{ display: tab === "explorer" ? "flex" : "none", flex: 1, minHeight: 0 }}>
        {state.status !== "ready" && <Status />}
        {db && (
          <Box sx={{ flex: 1, minHeight: 0 }}>
            <ExplorerTab />
          </Box>
        )}
      </Stack>
    </FloatingPanel>
  );
}
