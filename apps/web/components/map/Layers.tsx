import { setColorFunction } from "@geomatico/maplibre-cog-protocol";
import React, { useEffect, useMemo, useRef } from "react";
import type { FilterSpecification } from "maplibre-gl";
import type { LayerProps, MapGeoJSONFeature } from "react-map-gl/maplibre";
import { Layer as MapLayer, Source, useMap } from "react-map-gl/maplibre";

import { GEOAPI_BASE_URL, SYSTEM_LAYERS_IDS } from "@/lib/constants";
import { excludes as excludeOp } from "@/lib/transformers/filter";
import {
  getHightlightStyleSpec,
  getSymbolStyleSpec,
  transformToMapboxLayerStyleSpec,
} from "@/lib/transformers/layer";
import { generateCOGColorFunction } from "@/lib/utils/map/cog-styling";
import { getLayerKey } from "@/lib/utils/map/layer";
import type { FeatureLayerProperties, Layer, RasterLayerProperties } from "@/lib/validations/layer";
import type { ProjectLayer } from "@/lib/validations/project";
import { type ScenarioFeatures, scenarioEditTypeEnum } from "@/lib/validations/scenario";

import { useAppSelector } from "@/hooks/store/ContextHooks";

interface LayersProps {
  layers?: ProjectLayer[] | Layer[];
  selectedScenarioLayer?: ProjectLayer | null;
  highlightFeature?: MapGeoJSONFeature | null;
  scenarioFeatures?: ScenarioFeatures | null;
}

const Layers = (props: LayersProps) => {
  const { current: mapRef } = useMap();
  const temporaryFilters = useAppSelector((state) => state.map.temporaryFilters);
  const mapMode = useAppSelector((state) => state.map.mapMode);
  const pendingFeatures = useAppSelector((state) => state.featureEditor.pendingFeatures);
  const editLayerId = useAppSelector((state) => state.featureEditor.activeLayerId);

  // Get editing layer to copy its style to the overlay
  const editingLayer = useMemo(() => {
    if (!editLayerId || !props.layers) return null;
    return props.layers.find((l) => (l as ProjectLayer).layer_id === editLayerId) as ProjectLayer | undefined;
  }, [editLayerId, props.layers]);

  // Build list of feature IDs to hide from the tile layer (features being edited)
  const editExcludeIds = useMemo(() => {
    return Object.values(pendingFeatures)
      .filter((f) => f.action === "update" || f.action === "delete")
      .map((f) => f.id);
  }, [pendingFeatures]);

  // Build GeoJSON FeatureCollection from pending features for overlay rendering
  const pendingGeoJSON = useMemo(() => {
    const features = Object.values(pendingFeatures)
      .filter((f) => f.geometry !== null && f.committed && f.action !== "delete" && !f.drawFeatureId)
      .map((f) => ({
        type: "Feature" as const,
        id: f.id,
        geometry: f.geometry!,
        properties: { ...f.properties, _pending: true, _pendingId: f.id },
      }));
    return { type: "FeatureCollection" as const, features };
  }, [pendingFeatures]);

  const splitLayerFilter = (style: LayerProps) => {
    const styleWithFilter = style as LayerProps & { filter?: unknown };
    const { filter, ...layerStyleSpec } = styleWithFilter;
    return { filter, layerStyleSpec: layerStyleSpec as LayerProps };
  };

  const getMapLayerFilter = (filter: unknown): FilterSpecification | undefined => {
    return Array.isArray(filter) ? (filter as FilterSpecification) : undefined;
  };

  const scenarioFeaturesToExclude = useMemo(() => {
    const featuresToExclude: { [key: string]: string[] } = {};
    props.scenarioFeatures?.features.forEach((feature) => {
      // Exclude deleted and modified features
      if (
        feature.properties?.edit_type === scenarioEditTypeEnum.Enum.d ||
        feature.properties?.edit_type === scenarioEditTypeEnum.Enum.m
      ) {
        const projectLayerId = feature.properties.layer_project_id;
        if (!projectLayerId || !feature.properties?.feature_id) return;

        if (!featuresToExclude[projectLayerId]) featuresToExclude[projectLayerId] = [];

        if (feature.properties?.feature_id)
          featuresToExclude[projectLayerId].push(feature.properties?.feature_id);
      }
    });

    return featuresToExclude;
  }, [props.scenarioFeatures]);

  const getLayerQueryFilter = (layer: ProjectLayer | Layer) => {
    const cqlFilter = layer["query"]?.cql;
    if (!layer["layer_id"] || (!Object.keys(scenarioFeaturesToExclude).length && mapMode === "data"))
      return cqlFilter;

    const extendedFilter = JSON.parse(JSON.stringify(cqlFilter || {}));
    if (scenarioFeaturesToExclude[layer.id]?.length && mapMode === "data") {
      const scenarioFeaturesExcludeFilter = excludeOp("id", scenarioFeaturesToExclude[layer.id]);
      const parsedScenarioFeaturesExcludeFilter = JSON.parse(scenarioFeaturesExcludeFilter);
      // Append the filter to the existing filters
      if (extendedFilter["op"] === "and" && extendedFilter["args"]) {
        extendedFilter["args"].push(parsedScenarioFeaturesExcludeFilter);
      } else {
        // Create a new filter
        extendedFilter["op"] = "and";
        extendedFilter["args"] = [parsedScenarioFeaturesExcludeFilter];
      }
    }

    if (mapMode !== "data" && temporaryFilters.length > 0) {
      // Primary layer filters (filter.layer_id matches this layer)
      const primaryFilters = temporaryFilters
        .filter((filter) => filter.layer_id === layer.id)
        .map((filter) => filter.filter);

      // Additional target filters (from multi-layer attribute filtering)
      const additionalTargetFilters = temporaryFilters
        .flatMap((f) => f.additional_targets || [])
        .filter((t) => t.layer_id === layer.id)
        .map((t) => t.filter);

      // Merge all filters
      const allFilters = [...primaryFilters, ...additionalTargetFilters];
      if (allFilters.length === 0) return extendedFilter;
      const extendedWithTemporaryFilters = {
        op: "and",
        args: allFilters,
      };
      if (extendedFilter["args"]) {
        extendedWithTemporaryFilters["args"].push(extendedFilter);
      }
      return extendedWithTemporaryFilters;
    }
    return extendedFilter;
  };

  const getFeatureTileUrl = (layer: ProjectLayer | Layer, label = false) => {
    const extendedQuery = getLayerQueryFilter(layer);
    const parts: string[] = [];

    if (extendedQuery && Object.keys(extendedQuery).length > 0) {
      parts.push(`filter=${encodeURIComponent(JSON.stringify(extendedQuery))}`);
    }
    if (label) {
      parts.push("label=true");
    }
    // Cache-busting: if layer was updated within the last hour, append a version
    // param so the browser doesn't serve stale cached tiles after schema changes
    const updatedAt = layer["updated_at"];
    if (updatedAt) {
      const updatedMs = new Date(updatedAt).getTime();
      const ageMs = Date.now() - updatedMs;
      if (ageMs < 3600_000) {
        parts.push(`v=${Math.floor(updatedMs / 1000)}`);
      }
    }

    const query = parts.length > 0 ? `?${parts.join("&")}` : "";
    const layerId = layer["layer_id"] || layer["id"];
    return `${GEOAPI_BASE_URL}/collections/${layerId}/tiles/WebMercatorQuad/{z}/{x}/{y}${query}`;
  };
  const { useDataLayers, systemLayers } = useMemo(() => {
    const dataLayers = [] as ProjectLayer[] | Layer[];
    const sysLayers = [] as ProjectLayer[] | Layer[];

    props.layers?.forEach((layer) => {
      const layerId = layer["layer_id"] ?? layer.id;
      if (SYSTEM_LAYERS_IDS.indexOf(layerId) === -1) {
        dataLayers.push(layer);
      } else {
        sysLayers.push(layer);
      }
    });
    return { useDataLayers: dataLayers, systemLayers: sysLayers };
  }, [props.layers]);

  // Render in reverse order for correct initial stacking (MapLibre stacks bottom-to-top).
  // Reordering is handled imperatively via map.moveLayer() to avoid cross-Source timing
  // issues with beforeId (layers in different Sources don't mount synchronously).
  const reversedDataLayers = useMemo(
    () => (useDataLayers ? [...useDataLayers].reverse() : []),
    [useDataLayers]
  );

  // Imperatively reorder layers when the layer order changes (e.g., drag-and-drop).
  const prevOrderRef = useRef<string>("");
  useEffect(() => {
    if (!mapRef || !useDataLayers || useDataLayers.length < 2) return;

    const orderKey = useDataLayers.map((l) => l.id).join(",");
    // On initial mount, just record the order (reversed rendering handles it)
    if (!prevOrderRef.current) {
      prevOrderRef.current = orderKey;
      return;
    }
    // Skip if order hasn't changed
    if (orderKey === prevOrderRef.current) return;
    prevOrderRef.current = orderKey;

    const map = mapRef.getMap();
    const styleLayers = map.getStyle()?.layers || [];

    // For each data layer, find all MapLibre layers sharing its source
    const desiredTopToBottom: string[] = [];
    for (const layer of useDataLayers) {
      const mainLayerId = layer.id.toString();
      const mainLayer = styleLayers.find((l) => l.id === mainLayerId);
      if (!mainLayer || !("source" in mainLayer)) continue;
      const sourceId = mainLayer.source;
      // Collect all layers with this source (styleLayers is bottom-to-top, reverse for top-to-bottom)
      const group = styleLayers
        .filter((l) => "source" in l && l.source === sourceId)
        .map((l) => l.id)
        .reverse();
      desiredTopToBottom.push(...group);
    }

    // Reorder by moving each layer before the previous one in the desired order
    for (let i = 1; i < desiredTopToBottom.length; i++) {
      try {
        map.moveLayer(desiredTopToBottom[i], desiredTopToBottom[i - 1]);
      } catch {
        // Layer might not be on map yet
      }
    }
  }, [useDataLayers, mapRef]);


  return (
    <>
      {reversedDataLayers.length
        ? reversedDataLayers.map((layer: ProjectLayer | Layer) =>
            (() => {
              if (layer.type === "feature") {
                const { filter: layerFilter, layerStyleSpec } = splitLayerFilter(
                  transformToMapboxLayerStyleSpec(layer) as LayerProps
                );
                const mapLayerFilter = getMapLayerFilter(layerFilter);
                const { filter: strokeFilter, layerStyleSpec: strokeStyleSpec } = splitLayerFilter(
                  transformToMapboxLayerStyleSpec({
                    ...layer,
                    feature_layer_geometry_type: "line",
                    properties: {
                      ...layer.properties,
                      opacity: 1,
                      visibility:
                        layer.properties?.visibility &&
                        (layer.properties as FeatureLayerProperties)?.stroked,
                    },
                  }) as LayerProps
                );
                const mapStrokeFilter = getMapLayerFilter(strokeFilter);

                const { filter: labelFilter, layerStyleSpec: labelStyleSpec } = splitLayerFilter(
                  getSymbolStyleSpec((layer.properties as FeatureLayerProperties)?.text_label, layer) as LayerProps
                );
                const mapLabelFilter = getMapLayerFilter(labelFilter);

                const needsLabel =
                  layer.feature_layer_geometry_type === "polygon" &&
                  !!(layer.properties as FeatureLayerProperties)?.text_label;

                // Build exclusion filter for features being edited on this layer
                const isEditingThisLayer = editLayerId === (layer as ProjectLayer).layer_id;
                const mergeEditExclusion = (baseFilter: FilterSpecification | undefined): FilterSpecification | undefined => {
                  if (!isEditingThisLayer || editExcludeIds.length === 0) return baseFilter;
                  // MapLibre filter: exclude features whose "id" property is in the list
                  // Vector tiles use numeric sequential IDs stored as property "id"
                  // Try both string and number versions for matching
                  const excludeFilter: FilterSpecification = [
                    "!",
                    ["any",
                      ["in", ["get", "id"], ["literal", editExcludeIds.map(Number)]],
                      ["in", ["to-string", ["get", "id"]], ["literal", editExcludeIds]],
                    ],
                  ];
                  if (baseFilter) {
                    return ["all", baseFilter, excludeFilter] as FilterSpecification;
                  }
                  return excludeFilter;
                };

                return (
                  <Source
                    key={`${layer.id}-${layer.updated_at || ""}`}
                    type="vector"
                    tiles={[getFeatureTileUrl(layer, needsLabel)]}
                    maxzoom={14}>
                    {!layer.properties?.["custom_marker"] && (
                      <MapLayer
                        key={getLayerKey(layer)}
                        minzoom={layer.properties.min_zoom || 0}
                        maxzoom={layer.properties.max_zoom || 24}
                        id={layer.id.toString()}
                        {...layerStyleSpec}
                        {...(mergeEditExclusion(mapLayerFilter) ? { filter: mergeEditExclusion(mapLayerFilter) } : {})}
                        source-layer="default"
                      />
                    )}
                    {layer.feature_layer_geometry_type === "polygon" && (
                      <MapLayer
                        key={`stroke-${layer.id.toString()}`}
                        id={`stroke-${layer.id.toString()}`}
                        minzoom={layer.properties.min_zoom || 0}
                        maxzoom={layer.properties.max_zoom || 24}
                        {...strokeStyleSpec}
                        {...(mergeEditExclusion(mapStrokeFilter) ? { filter: mergeEditExclusion(mapStrokeFilter) } : {})}
                        source-layer="default"
                      />
                    )}

                    {/* Labels for all layers that aren't a custom marker*/}
                    {((layer.properties as FeatureLayerProperties)?.text_label ||
                      layer.properties?.["custom_marker"]) && (
                      <MapLayer
                        key={
                          layer.properties?.["custom_marker"] ? getLayerKey(layer) : `text-label-${layer.id}`
                        }
                        id={
                          layer.properties?.["custom_marker"] ? layer.id.toString() : `text-label-${layer.id}`
                        }
                        source-layer={
                          layer.feature_layer_geometry_type === "polygon" ? "default_anchor" : "default"
                        }
                        minzoom={layer.properties.min_zoom || 0}
                        maxzoom={layer.properties.max_zoom || 24}
                        {...labelStyleSpec}
                        {...(layer.properties?.["custom_marker"] || layer.feature_layer_geometry_type === "polygon"
                          ? (mergeEditExclusion(mapLabelFilter) ? { filter: mergeEditExclusion(mapLabelFilter) } : {})
                          : (mapLabelFilter ? { filter: mapLabelFilter } : {})
                        )}
                      />
                    )}

                    {/* HighlightLayer */}
                    {props.highlightFeature &&
                      props.highlightFeature.properties?.id &&
                      props.highlightFeature.layer.id === layer.id.toString() && (
                        <MapLayer
                          id={`highlight-${layer.id}`}
                          source-layer="default"
                          {...(getHightlightStyleSpec(props.highlightFeature) as LayerProps)}
                        />
                      )}
                  </Source>
                );
              } else if (layer.type === "raster" && layer.url) {
                const rasterProperties = layer.properties as RasterLayerProperties;

                // Register color function for COG layers with custom styling
                // Only needed for color_range, categories, hillshade (not image - use native MapLibre properties)
                if (layer.data_type === "cog" && rasterProperties?.style?.style_type !== "image") {
                  const colorFunction = generateCOGColorFunction(rasterProperties.style);
                  if (colorFunction) {
                    setColorFunction(layer.url, colorFunction);
                  }
                }

                return (
                  <Source
                    key={layer.id}
                    type="raster"
                    {...(layer.data_type === "cog" ? { url: `cog://${layer.url}` } : { tiles: [layer.url] })}
                    tileSize={layer.other_properties?.tile_size || 256}>
                    <MapLayer
                      key={getLayerKey(layer)}
                      id={layer.id.toString()}
                      minzoom={layer.properties?.min_zoom || 0}
                      maxzoom={layer.properties?.max_zoom || 24}
                      type="raster"
                      source-layer="default"
                      layout={{
                        visibility: layer.properties?.visibility ? "visible" : "none",
                      }}
                      paint={{
                        "raster-opacity": rasterProperties?.opacity || 1.0,
                        ...(rasterProperties?.style?.style_type === "image" && {
                          "raster-brightness-min": rasterProperties.style.brightness_min ?? 0,
                          "raster-brightness-max": rasterProperties.style.brightness_max ?? 1,
                          "raster-contrast": rasterProperties.style.contrast ?? 0,
                          "raster-saturation": rasterProperties.style.saturation ?? 0,
                        }),
                      }}
                    />
                  </Source>
                );
              } else {
                return null;
              }
            })()
          )
        : null}
      {systemLayers?.length
        ? systemLayers.map((layer: ProjectLayer | Layer) =>
            props.selectedScenarioLayer?.id === layer.id ? (
              (() => {
                const { filter: layerFilter, layerStyleSpec } = splitLayerFilter(
                  transformToMapboxLayerStyleSpec(layer) as LayerProps
                );
                const mapLayerFilter = getMapLayerFilter(layerFilter);
                return (
              <Source
                key={`${layer.id}-${layer.updated_at || ""}`}
                type="vector"
                tiles={[getFeatureTileUrl(layer)]}
                minzoom={14}
                maxzoom={22}>
                <MapLayer
                  key={getLayerKey(layer)}
                  id={layer.id.toString()}
                  {...layerStyleSpec}
                  {...(mapLayerFilter ? { filter: mapLayerFilter } : {})}
                  source-layer="default"
                  minzoom={14}
                  maxzoom={22}
                />
              </Source>
                );
              })()
            ) : null
          )
        : null}
      {/* Pending features overlay — uses editing layer's original style */}
      {editLayerId && editingLayer && pendingGeoJSON.features.length > 0 && (() => {
        const layerStyle = transformToMapboxLayerStyleSpec(editingLayer) as LayerProps & { paint?: Record<string, unknown> };
        const geomType = editingLayer.feature_layer_geometry_type;
        const isCustomMarker = !!editingLayer.properties?.["custom_marker"];
        const symbolStyle = isCustomMarker
          ? getSymbolStyleSpec(undefined, editingLayer) as LayerProps & { layout?: Record<string, unknown>; paint?: Record<string, unknown> }
          : null;

        return (
          <Source key="pending-features" type="geojson" data={pendingGeoJSON}>
            {geomType === "polygon" && layerStyle.type === "fill" && (
              <MapLayer
                id="pending-features-fill"
                type="fill"
                paint={layerStyle.paint as Record<string, unknown>}
                filter={["==", "$type", "Polygon"]}
              />
            )}
            {(geomType === "line" || geomType === "polygon") && (
              <MapLayer
                id="pending-features-line"
                type="line"
                paint={{
                  "line-color": "#ef4444",
                  "line-dasharray": [3, 2],
                  "line-width": 2,
                }}
                filter={["any", ["==", "$type", "LineString"], ["==", "$type", "Polygon"]]}
              />
            )}
            {geomType === "point" && isCustomMarker && symbolStyle && (
              <MapLayer
                id="pending-features-ring"
                type="circle"
                paint={{
                  "circle-radius": Math.max(12, Math.round(((editingLayer.properties as Record<string, unknown>)?.marker_size as number ?? 100) / 200 * 16) + 4),
                  "circle-color": "transparent",
                  "circle-stroke-color": "#ef4444",
                  "circle-stroke-width": 2,
                }}
                filter={["==", "$type", "Point"]}
              />
            )}
            {geomType === "point" && isCustomMarker && symbolStyle && (
              <MapLayer
                id="pending-features-symbol"
                type="symbol"
                layout={{
                  ...symbolStyle.layout,
                  visibility: "visible",
                  "icon-allow-overlap": true,
                }}
                paint={symbolStyle.paint as Record<string, unknown>}
                filter={["==", "$type", "Point"]}
              />
            )}
            {geomType === "point" && !isCustomMarker && layerStyle.type === "circle" && (
              <MapLayer
                id="pending-features-circle"
                type="circle"
                paint={layerStyle.paint as Record<string, unknown>}
                filter={["==", "$type", "Point"]}
              />
            )}
            {/* Fallbacks if style type doesn't match */}
            {geomType === "polygon" && layerStyle.type !== "fill" && (
              <MapLayer
                id="pending-features-fill"
                type="fill"
                paint={{ "fill-color": "#ef4444", "fill-opacity": 0.15 }}
                filter={["==", "$type", "Polygon"]}
              />
            )}
            {geomType === "point" && !isCustomMarker && layerStyle.type !== "circle" && (
              <MapLayer
                id="pending-features-circle"
                type="circle"
                paint={{ "circle-radius": 6, "circle-color": "#ef4444", "circle-stroke-width": 2, "circle-stroke-color": "#fff" }}
                filter={["==", "$type", "Point"]}
              />
            )}
          </Source>
        );
      })()}
    </>
  );
};

export default Layers;
