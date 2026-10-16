/**
 * The Explorer's left panel (board 3): one panel headed by a tab bar —
 * Layers | Explorer. It neither closes nor collapses; the Explorer is left
 * by the × on its tab, Exit Explorer in the bottom bar, or the bundle's menu
 * (docs/pt-network-editor/docs/02-panel-behaviour-spec.md).
 */
import { Box, Stack, Tab, Tabs, Tooltip, alpha } from "@mui/material";
import type { ReactNode } from "react";
import { useTranslation } from "react-i18next";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import { type ExplorerLeftTab, closeExplorer, setExplorerLeftTab } from "@/lib/store/explorer/slice";

import { useAppDispatch, useAppSelector } from "@/hooks/store/ContextHooks";

import { FloatingPanel } from "@/components/common/FloatingPanel";
import { CostSection } from "@/components/explorer/CostSection";
import { useExplorer } from "@/components/explorer/ExplorerContext";
import { ExplorerLoadingCard, ExplorerLoadingLine } from "@/components/explorer/ExplorerLoading";
import { ExplorerSettingsDialog } from "@/components/explorer/ExplorerSettingsDialog";
import { ExplorerTab } from "@/components/explorer/ExplorerTab";
import { EXPLORER_RIGHT_PANEL_WIDTH } from "@/components/explorer/right/panelSize";

/** The left panel's width while the Explorer is open: the right panel's, so
 * the two sides match and the route summary has room. It holds for the
 * whole session — opening a route never resizes it — and the data view's
 * own width comes back on exit (DataProjectLayout). */
export const EXPLORER_PANEL_WIDTH = EXPLORER_RIGHT_PANEL_WIDTH;

export function ExplorerLeftPanel({ layersTab }: { layersTab: ReactNode }) {
  const { t } = useTranslation("common");
  const dispatch = useAppDispatch();
  const tab = useAppSelector((s) => s.explorer.leftTab);
  const { db } = useExplorer();

  // One panel: the tab bar is its header, the active tab fills the rest. Both
  // tabs stay mounted, so switching is a tab change, never a reload — the
  // route search and scroll position survive a look at the layers.
  return (
    // The full map height on both tabs, so switching does not resize it.
    <FloatingPanel width={EXPLORER_PANEL_WIDTH} maxHeight="100%" fillHeight>
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
          label={
            <>
              {t("explorer")}
              {/* The way out, at the tab's far edge, centred on its height: a
                  control inside the tab, so its own click does not select it. */}
              <Tooltip title={t("explorer_exit")}>
                <Box
                  component="span"
                  role="button"
                  tabIndex={0}
                  aria-label={t("explorer_exit")}
                  onMouseDown={(e) => e.stopPropagation()}
                  onClick={(e) => {
                    e.stopPropagation();
                    dispatch(closeExplorer());
                  }}
                  onKeyDown={(e) => {
                    if (e.key === "Enter" || e.key === " ") {
                      e.preventDefault();
                      e.stopPropagation();
                      dispatch(closeExplorer());
                    }
                  }}
                  sx={{
                    position: "absolute",
                    right: 10,
                    top: "50%",
                    transform: "translateY(-50%)",
                    display: "inline-flex",
                    alignItems: "center",
                    justifyContent: "center",
                    width: 24,
                    height: 24,
                    borderRadius: "50%",
                    color: "primary.main",
                    "&:hover": {
                      bgcolor: (theme) => alpha(theme.palette.primary.main, 0.12),
                      color: "primary.dark",
                    },
                  }}>
                  <Icon iconName={ICON_NAME.XCLOSE} style={{ fontSize: 14 }} htmlColor="inherit" />
                </Box>
              </Tooltip>
            </>
          }
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
      <Stack
        sx={{ display: tab === "explorer" ? "flex" : "none", flex: 1, minHeight: 0, position: "relative" }}>
        <ExplorerLoadingLine />
        {db ? (
          <>
            <Box sx={{ flex: 1, minHeight: 0 }}>
              <ExplorerTab />
            </Box>
            <CostSection />
            <ExplorerSettingsDialog />
          </>
        ) : (
          <Box sx={{ flex: 1, minHeight: 0, overflowY: "auto" }}>
            <ExplorerLoadingCard />
          </Box>
        )}
      </Stack>
    </FloatingPanel>
  );
}
