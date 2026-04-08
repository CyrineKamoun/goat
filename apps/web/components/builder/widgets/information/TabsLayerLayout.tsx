import { Box, Tab, Tabs, Typography } from "@mui/material";
import { useCallback, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";

import { Icon, ICON_NAME } from "@p4b/ui/components/Icon";

import type { ProjectLayer, ProjectLayerGroup, ProjectLayerTreeUpdate } from "@/lib/validations/project";
import type { LayerInformationSchema } from "@/lib/validations/widget";

import { MaskedImageIcon } from "@/components/map/panels/style/other/MaskedImageIcon";

import { ProjectLayerTree } from "@/components/map/panels/layer/ProjectLayerTree";

interface TabsLayerLayoutProps {
  projectId: string;
  projectLayers: ProjectLayer[];
  projectLayerGroups: ProjectLayerGroup[];
  config: LayerInformationSchema;
  onTreeUpdate: (payload: ProjectLayerTreeUpdate) => Promise<void>;
  viewOnly?: boolean;
  downloadableLayers?: number[];
  hideLegendHeading?: boolean;
  groupIcons?: Record<string, { url: string; source?: string }>;
  dimOutOfZoom?: boolean;
}

const TabsLayerLayout = ({
  projectId,
  projectLayers,
  projectLayerGroups,
  config,
  onTreeUpdate,
  downloadableLayers,
  hideLegendHeading,
  groupIcons,
  dimOutOfZoom,
}: TabsLayerLayoutProps) => {
  const { t } = useTranslation("common");
  const options = config.options;

  const topLevelGroups = useMemo(
    () =>
      projectLayerGroups
        .filter((g) => !g.parent_id)
        .sort((a, b) => (a.order ?? 0) - (b.order ?? 0)),
    [projectLayerGroups]
  );

  const [activeTab, setActiveTab] = useState(0);
  const activeGroup = topLevelGroups[activeTab];

  // Filter layers to active group and strip group ID so they render as root items
  const activeGroupLayers = useMemo(() => {
    const layers = !activeGroup
      ? projectLayers
      : projectLayers.filter((l) => l.layer_project_group_id === activeGroup.id);
    return layers.map((l) => ({ ...l, layer_project_group_id: null }));
  }, [activeGroup, projectLayers]);

  // Wrap onTreeUpdate to restore the original group ID before sending updates
  const handleTreeUpdate = useCallback(
    async (payload: ProjectLayerTreeUpdate) => {
      const fixedPayload: ProjectLayerTreeUpdate = {
        items: payload.items.map((item) => {
          if (item.type === "layer" && activeGroup) {
            // Restore the original group assignment
            return { ...item, parent_id: activeGroup.id };
          }
          return item;
        }),
      };
      await onTreeUpdate(fixedPayload);
    },
    [onTreeUpdate, activeGroup]
  );

  if (topLevelGroups.length === 0) {
    return (
      <Typography variant="body2" color="text.secondary" sx={{ p: 2, textAlign: "center" }}>
        {t("no_groups_found")}
      </Typography>
    );
  }

  return (
    <Box>
      <Tabs
        value={activeTab}
        onChange={(_, newValue) => setActiveTab(newValue)}
        variant="scrollable"
        scrollButtons="auto"
        sx={{ borderBottom: 1, borderColor: "divider", minHeight: 32 }}>
        {topLevelGroups.map((group) => (
          <Tab
            key={group.id}
            label={options?.show_group_name !== false ? group.name : undefined}
            icon={
              options?.show_group_icons ? (
                groupIcons?.[`group_icon_${group.id}`]?.url ? (
                  <MaskedImageIcon
                    imageUrl={groupIcons[`group_icon_${group.id}`].url}
                    dimension="16px"
                    applyMask={groupIcons[`group_icon_${group.id}`].source === "library"}
                    imgColor=""
                  />
                ) : (
                  <Icon iconName={ICON_NAME.LAYERS} style={{ fontSize: 14 }} />
                )
              ) : undefined
            }
            iconPosition="start"
            sx={{ minHeight: 32, textTransform: "none", fontSize: "0.8125rem", py: 0 }}
          />
        ))}
      </Tabs>
      {/* Reuse ProjectLayerTree — same rendering as tree mode, just filtered to active group */}
      <ProjectLayerTree
        projectId={projectId}
        projectLayers={activeGroupLayers}
        projectLayerGroups={[]}
        viewMode="view"
        isLoading={false}
        onTreeUpdate={handleTreeUpdate}
        toggleStyle={options?.toggle_style}
        togglePosition={options?.toggle_position}
        moreOptionsStyle={options?.more_options_style}
        allowedActions={{
          style: options?.show_style_action ?? true,
          viewData: options?.show_view_data_action ?? true,
          properties: options?.show_properties_action ?? true,
          zoomTo: options?.show_zoom_to_action ?? true,
        }}
        downloadableLayers={downloadableLayers}
        hideLegendHeading={hideLegendHeading}
        groupIcons={groupIcons}
        dimOutOfZoom={dimOutOfZoom}
      />
    </Box>
  );
};

export default TabsLayerLayout;
