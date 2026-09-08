import type { FeatureLayerPointProperties, Layer } from "@/lib/validations/layer";
import type { ProjectLayer } from "@/lib/validations/project";

export const getLayerKey = (layer: ProjectLayer | Layer) => {
  let id = layer.id.toString();
  if (layer.type === "feature") {
    const geometry_type = layer.feature_layer_geometry_type;
    if (geometry_type === "point") {
      const pointFeature = layer.properties as FeatureLayerPointProperties;
      const renderAs =
        pointFeature.custom_marker && (pointFeature.marker?.name || pointFeature.marker_field)
          ? "marker"
          : "circle";
      id = `${id}-${renderAs}`;
    }
  }
  return id;
};

/**
 * Which layers `Layers.tsx` may turn into MapLibre sources and layers.
 *
 * Drops the system layers (rendered through their own path, e.g. the
 * street-network layer carried across a basemap swap) and, per D7, a
 * `locked` project layer: its `properties` are blanked by the backend, so
 * it carries no style to draw, and it must never be requested as a tile
 * source, legend, or extent — a locked row simply never reaches the map.
 */
export function selectDataLayers(
  layers: (ProjectLayer | Layer)[] | undefined,
  systemLayerIds: readonly string[]
): (ProjectLayer | Layer)[] {
  const dataLayers: (ProjectLayer | Layer)[] = [];
  (layers ?? []).forEach((layer) => {
    // Only a ProjectLayer can be `locked` (a bare Layer, e.g. the thumbnail
    // and report renderers' input, was never added to a project link).
    if ("locked" in layer && layer.locked) return;
    const layerId = "layer_id" in layer ? layer.layer_id : layer.id;
    if (systemLayerIds.indexOf(layerId) === -1) {
      dataLayers.push(layer);
    }
  });
  return dataLayers;
}

/**
 * The predicate `useFilteredProjectLayers` applies for every layer picker
 * (starting points, statistics field, chart/table/number widget setup):
 * drops an excluded type, an excluded dataset id (typically the system
 * layers), and — per D7 — any `locked` layer, so an editor can never pick
 * one they have no access to in the first place.
 */
export function filterSelectableProjectLayers(
  layers: ProjectLayer[] | undefined,
  excludeLayerTypes: readonly string[],
  excludeLayerIds: readonly string[]
): ProjectLayer[] {
  if (!layers) return [];
  return layers.filter(
    (layer) =>
      !excludeLayerTypes.includes(layer.type) &&
      !excludeLayerIds.includes(layer.layer_id) &&
      !layer.locked
  );
}

/**
 * Resolves a saved config's `layer_project_id` (a chart/table/number
 * widget's setup, a tool's starting-points/statistics layer, …) to its
 * project layer, refusing to hand out `layerId` — the dataset UUID every
 * caller uses to fetch feature data/fields/unique values from geoapi — when
 * that layer is `locked` (D7).
 *
 * A saved config can point at a layer that was pickable when it was
 * configured but has since become locked for the current viewer (the
 * picker itself already excludes locked layers via
 * {@link filterSelectableProjectLayers}, so this only bites an *existing*
 * config) — `isLayerLocked` lets the caller render a hint instead of
 * silently requesting nothing.
 */
export function resolveProjectLayer(
  layers: ProjectLayer[] | undefined,
  layerProjectId: number | undefined
): { layer: ProjectLayer | undefined; layerId: string | undefined; isLayerLocked: boolean } {
  const layer = layerProjectId ? layers?.find((l) => l.id === layerProjectId) : undefined;
  const isLayerLocked = !!layer?.locked;
  return { layer, layerId: isLayerLocked ? undefined : layer?.layer_id, isLayerLocked };
}

