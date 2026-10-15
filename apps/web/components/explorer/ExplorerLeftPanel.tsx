/**
 * The Explorer's left panel (board 3): one panel headed by a tab bar —
 * Layers | Explorer. It neither closes nor collapses: Exit Explorer is the
 * only way out of the Explorer
 * (docs/pt-network-editor/docs/02-panel-behaviour-spec.md).
 */
import { Box, Stack, Tab, Tabs } from "@mui/material";
import type { ReactNode } from "react";
import { useTranslation } from "react-i18next";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import { type ExplorerLeftTab, setExplorerLeftTab } from "@/lib/store/explorer/slice";

import { useAppDispatch, useAppSelector } from "@/hooks/store/ContextHooks";

import { FloatingPanel } from "@/components/common/FloatingPanel";
import { CostSection } from "@/components/explorer/CostSection";
import { useExplorer } from "@/components/explorer/ExplorerContext";
import { ExplorerLoadingCard, ExplorerLoadingLine } from "@/components/explorer/ExplorerLoading";
import { ExplorerSettingsDialog } from "@/components/explorer/ExplorerSettingsDialog";
import { ExplorerTab } from "@/components/explorer/ExplorerTab";

/** The data view's left panel width: entering the Explorer or opening a route
 * never resizes the panel under the user. */
export const EXPLORER_PANEL_WIDTH = 300;

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
