import { createSelector } from "@reduxjs/toolkit";

import { SYSTEM_LAYERS_IDS } from "@/lib/constants";
import type { RootState } from "@/lib/store";
import { filterSelectableProjectLayers } from "@/lib/utils/map/layer";
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
    // First filter by layer type, system layers, and (D7) any locked layer —
    // the public map must never request its tiles either.
    const filteredLayers = filterSelectableProjectLayers(
      projectLayers,
      excludeLayerTypes,
      excludeLayerIds
    );

    // Then order to match the visual layer panel hierarchy and filter out
    // layers that belong to invisible groups.
    return orderLayersByTree(filteredLayers, projectLayerGroups);
  }
);
