import { createSelector } from "@reduxjs/toolkit";

import { SYSTEM_LAYERS_IDS } from "@/lib/constants";
import type { RootState } from "@/lib/store";
import { orderLayersByTree } from "@/lib/utils/map/layerTreeOrder";

export const selectProjectLayers = (state: RootState) => state.layers.projectLayers;
export const selectProjectLayerGroups = (state: RootState) => state.layers.projectLayerGroups;
export const selectProject = (state: RootState) => state.map.project;

export const selectFilteredProjectLayers = createSelector(
  [
    selectProjectLayers,
    selectProjectLayerGroups,
    (_: RootState, excludeLayerTypes: string[] = []) => excludeLayerTypes,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (_: RootState, _1: any, _2: any, excludeLayerIds: string[] = [...SYSTEM_LAYERS_IDS]) => excludeLayerIds,
  ],
  (projectLayers, projectLayerGroups, excludeLayerTypes, excludeLayerIds) => {
    if (!projectLayers) return [];

    // First filter by layer type and system layers
    const filteredLayers = projectLayers.filter(
      (layer) => !excludeLayerTypes.includes(layer.type) && !excludeLayerIds.includes(layer.layer_id)
    );

    // Then order to match the visual layer panel hierarchy and filter out
    // layers that belong to invisible groups.
    return orderLayersByTree(filteredLayers, projectLayerGroups);
  }
);
