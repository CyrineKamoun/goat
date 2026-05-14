import { cogProtocol } from "@geomatico/maplibre-cog-protocol";
import MapboxDraw from "@mapbox/mapbox-gl-draw";
import type { Theme } from "@mui/material";
import { Box, useMediaQuery, useTheme } from "@mui/material";
import maplibregl from "maplibre-gl";
import { useCallback, useEffect, useMemo, useRef } from "react";
import { useTranslation } from "react-i18next";
import { Map, type MapLayerMouseEvent, type MapRef, type ViewState } from "react-map-gl/maplibre";
import type { ViewStateChangeEvent } from "react-map-gl/maplibre";
import { v4 } from "uuid";

import { PATTERN_IMAGES } from "@/lib/constants/pattern-images";
import { setClickedFeatureForFilter, setCurrentZoom, setHighlightedFeature, setPopupInfo } from "@/lib/store/map/slice";
import { addOrUpdateMarkerImages, addPatternImages } from "@/lib/transformers/map-image";
import { applyMapLanguage } from "@/hooks/map/MapHooks";
import createPulsingDot from "@/lib/utils/map/pulsing-dot-image";
import type { FormatNumberTypes } from "@/lib/validations/common";
import type { LayerInteractionFieldListContent } from "@/lib/validations/layer";
import {
  type FeatureLayerPointProperties,
  type Layer,
  layerInteractionContentType,
  layerInteractionType,
} from "@/lib/validations/layer";
import type { ProjectLayer } from "@/lib/validations/project";
import type { ScenarioFeatures } from "@/lib/validations/scenario";

import { useAppDispatch, useAppSelector } from "@/hooks/store/ContextHooks";

import { useFeatureEditor } from "@/hooks/map/useFeatureEditor";
import GeocoderLayer from "@/components/map/GeocoderLayer";
import Layers from "@/components/map/Layers";
import ScenarioLayer from "@/components/map/ScenarioLayer";
import ToolboxLayers from "@/components/map/ToolboxLayers";
import UserLocationLayer from "@/components/map/UserLocationLayer";
import DrawControl from "@/components/map/controls/Draw";
import FeatureEditToolbar from "@/components/map/controls/FeatureEditToolbar";
import ConfirmModal from "@/components/modals/Confirm";
import { MapPopoverInfo } from "@/components/map/controls/LayerInfo";
import MapPopoverEditor from "@/components/map/controls/PopoverEditor";
import { MeasureLabels } from "@/components/map/controls/measure";

maplibregl.addProtocol("cog", cogProtocol);

interface MapProps {
  mapRef: React.RefObject<MapRef> | null;
  initialViewState?: Partial<ViewState> & {
    bounds?: [number, number, number, number];
    fitBoundsOptions?: {
      offset?: [number, number];
      minZoom?: number;
      maxZoom?: number;
      padding?: number | { top: number; bottom: number; left: number; right: number };
    };
  };
  maxExtent?: [number, number, number, number];
  mapStyle: string | maplibregl.StyleSpecification;
  layers: ProjectLayer[] | Layer[];
  scenarioFeatures?: ScenarioFeatures;
  onMove?: ((e: ViewStateChangeEvent) => void | undefined) | undefined;
  onMoveEnd?: ((e: ViewStateChangeEvent) => void | undefined) | undefined;
  onClick?: (e: MapLayerMouseEvent) => void;
  onLoad?: () => void;
  dragRotate?: boolean | undefined;
  touchZoomRotate?: boolean | undefined;
  children?: React.ReactNode;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  containerSx?: any;
  isEditor?: boolean;
}

const MapViewer: React.FC<MapProps> = ({
  mapRef,
  initialViewState,
  mapStyle,
  layers,
  scenarioFeatures,
  onMove,
  onMoveEnd,
  onClick,
  onLoad,
  dragRotate = false,
  touchZoomRotate = false,
  maxExtent,
  children,
  containerSx,
  isEditor,
}) => {
  const { t, i18n } = useTranslation("common");
  const theme = useTheme();
  const dispatch = useAppDispatch();
  const isGetInfoActive = useAppSelector((state) => state.map.isMapGetInfoActive);
  const mapCursor = useAppSelector((state) => state.map.mapCursor);
  const highlightedFeature = useAppSelector((state) => state.map.highlightedFeature);
  const popupInfo = useAppSelector((state) => state.map.popupInfo);
  const popupEditor = useAppSelector((state) => state.map.popupEditor);
  const mapMode = useAppSelector((state) => state.map.mapMode);

  const _selectedScenarioEditLayer = useAppSelector((state) => state.map.selectedScenarioLayer);
  const selectedScenarioEditLayer = useMemo(() => {
    return layers?.find((layer) => layer.id === _selectedScenarioEditLayer?.value);
  }, [_selectedScenarioEditLayer, layers]);

  const featureEditorHandlers = useFeatureEditor(mapRef);

  const pendingFeaturesExist = useAppSelector(
    (state) => Object.keys(state.featureEditor.pendingFeatures).length > 0
  );
  const interactiveLayerIds = useMemo(() => {
    const ids = layers?.map((layer) => layer.id.toString()) || [];
    // For point layers with clustering enabled, include the cluster
    // sub-layer ids so MapLibre returns them in click events (needed for
    // cluster expand-on-click; layer-id convention from Layers.tsx).
    // Non-existent ids are silently ignored by MapLibre, so adding all
    // four covers both circle-cluster and marker-cluster modes.
    layers?.forEach((l) => {
      if (
        l.type === "feature" &&
        l.feature_layer_geometry_type === "point" &&
        (l.properties as { cluster?: { enabled?: boolean } } | undefined)?.cluster?.enabled
      ) {
        ids.push(
          `${l.id}-cluster-bubble`,
          `${l.id}-cluster-count`,
          `${l.id}-cluster-icon`,
          `${l.id}-cluster-badge`,
        );
      }
    });
    if (pendingFeaturesExist) {
      ids.push("pending-features-fill", "pending-features-line", "pending-features-circle", "pending-features-symbol");
    }
    return ids;
  }, [layers, pendingFeaturesExist]);

  const hiddenSystemProperties = useMemo(
    () =>
      new Set([
        "layer_id",
        "id",
        "_rowid",
        "feature_id",
        "h3_3",
        "h3_6",
        "cluster",
        "clustered",
        "point_count",
        "point_count_abbreviated",
        "sqrt_point_count",
        "ags_gemeinde",
        "ags_landkreis",
      ]),
    []
  );

  const isSystemPropertyKey = useCallback(
    (key: string) => {
      return hiddenSystemProperties.has(key);
    },
    [hiddenSystemProperties]
  );

  const handlePopoverClose = () => {
    dispatch(setPopupInfo(undefined));
    dispatch(setHighlightedFeature(undefined));
  };

  const handleMapClick = (e: MapLayerMouseEvent) => {
    const { features } = e;

    // Cluster click: zoom to the cluster's expansion zoom.
    // Layer-id convention from Layers.tsx: cluster layers contain `-cluster-`
    // (bubble / count / icon / badge).
    const clusterFeature = features?.find(
      (f) =>
        typeof f.layer.id === "string" &&
        f.layer.id.includes("-cluster-") &&
        f.properties?.cluster_id != null,
    );
    if (clusterFeature && mapRef?.current) {
      const map = mapRef.current.getMap();
      const sourceId = clusterFeature.layer.source;
      const source = map.getSource(sourceId) as maplibregl.GeoJSONSource | undefined;
      const clusterId = clusterFeature.properties?.cluster_id;
      if (source && clusterId != null && "getClusterExpansionZoom" in source) {
        source
          .getClusterExpansionZoom(clusterId)
          .then((zoom: number) => {
            const geom = clusterFeature.geometry;
            if (geom?.type === "Point") {
              map.easeTo({
                center: geom.coordinates as [number, number],
                zoom,
                duration: 500,
              });
            }
          })
          .catch(() => {
            // Cluster source may have been torn down between click and resolve; ignore.
          });
      }
      return;
    }

    // Track whether the popup block already highlighted a feature
    let didHighlight = false;

    if (features && features.length > 0 && isGetInfoActive) {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      let interactiveFeature = null as any;
      let interactiveLayer: Layer | ProjectLayer | undefined = undefined;

      // Find the first feature that has a click interaction by iterating through them
      for (const feature of features) {
        const layer = layers?.find(
          (l) =>
            l.id.toString() === feature.layer.id &&
            ![layerInteractionType.Enum.none, layerInteractionType.Enum.hover].includes(
              l.properties.interaction?.type
            )
        );

        if (layer) {
          interactiveFeature = feature;
          interactiveLayer = layer;
          break; // Found the topmost interactive feature, so we stop.
        }
      }

      if (interactiveFeature && interactiveLayer) {
        dispatch(setHighlightedFeature(interactiveFeature));
        didHighlight = true;

        const layerName = interactiveLayer.name;
        let lngLat: [number, number] = [e.lngLat.lng, e.lngLat.lat];
        if (interactiveFeature.geometry.type === "Point" && interactiveFeature.geometry.coordinates) {
          lngLat = [interactiveFeature.geometry.coordinates[0], interactiveFeature.geometry.coordinates[1]];
        }

        const interactionFieldLists = interactiveLayer.properties?.interaction?.content?.filter(
          (content) => content.type === layerInteractionContentType.Enum.field_list
        ) as LayerInteractionFieldListContent[] | undefined;

        // Build field list metadata (labels, order, decorators) from all field_list
        // interaction contents. Raw values stay keyed by column name so that
        // LayerInfo can apply kind-aware formatting (e.g. m² → ha for area fields).
        const fieldLabels: Record<string, string> = {};
        const fieldOrder: string[] = [];
        const fieldDecorators: Record<string, { prefix?: string; suffix?: string; format?: FormatNumberTypes }> = {};
        interactionFieldLists?.forEach((content) => {
          content.attributes.forEach((attr) => {
            if (fieldOrder.includes(attr.name)) return; // first definition wins
            fieldOrder.push(attr.name);
            fieldLabels[attr.name] = attr.label || attr.name;
            if (attr.format || attr.prefix || attr.suffix) {
              fieldDecorators[attr.name] = {
                format: attr.format as FormatNumberTypes | undefined,
                prefix: attr.prefix,
                suffix: attr.suffix,
              };
            }
          });
        });
        const hasFieldList = fieldOrder.length > 0;

        // When a field list is configured, filter to the listed columns (raw values).
        // Otherwise, pass all feature properties.
        const rawProperties = hasFieldList
          ? fieldOrder.reduce(
              (acc, name) => {
                if (!isSystemPropertyKey(name)) {
                  acc[name] = interactiveFeature.properties[name];
                }
                return acc;
              },
              {} as Record<string, unknown>
            )
          : interactiveFeature.properties;

        const jsonProperties = {};
        const primitiveProperties = {};
        if (rawProperties) {
          for (const key in rawProperties) {
            if (!isSystemPropertyKey(key)) {
              const value = rawProperties[key];
              try {
                // Type assertion to satisfy JSON.parse
                const parsedValue = JSON.parse(value as string);
                if (typeof parsedValue === "object" && parsedValue !== null) {
                  jsonProperties[key] = parsedValue;
                } else {
                  throw new Error("Parsed value is not an object");
                }
              } catch (error) {
                primitiveProperties[key] = value;
              }
            }
          }
        }
        dispatch(
          setPopupInfo({
            lngLat,
            properties: primitiveProperties,
            jsonProperties: jsonProperties,
            title: layerName ?? "",
            // ProjectLayer.id is the project-layer-link id, not the
            // underlying dataset id. queryables expects the dataset id,
            // which lives on `.layer_id` for ProjectLayers and on `.id`
            // for plain Layers. Prefer `layer_id` when present.
            layerId: (
              (interactiveLayer as { layer_id?: string }).layer_id ??
              interactiveLayer.id
            )?.toString(),
            ...(hasFieldList && {
              fieldLabels,
              fieldOrder,
              ...(Object.keys(fieldDecorators).length > 0 && { fieldDecorators }),
            }),
            onClose: handlePopoverClose,
          })
        );
      } else {
        // No interactive features were found in the click stack.
        dispatch(setHighlightedFeature(undefined));
        dispatch(setPopupInfo(undefined));
      }
    } else {
      // No features clicked or get info tool is not active.
      dispatch(setHighlightedFeature(undefined));
      dispatch(setPopupInfo(undefined));
    }

    // Click-to-filter: dispatch clicked feature independently of popup interaction settings.
    // This finds the topmost feature from ANY layer (regardless of interaction type).
    if (features && features.length > 0 && (mapMode === "builder" || mapMode === "public")) {
      for (const feature of features) {
        const matchedLayer = layers?.find((l) => l.id.toString() === feature.layer.id);
        if (matchedLayer) {
          // Only highlight if the popup block didn't already handle it
          if (!didHighlight) {
            dispatch(setHighlightedFeature(feature));
          }
          dispatch(
            setClickedFeatureForFilter({
              layerProjectId: matchedLayer.id as number,
              properties: feature.properties,
              timestamp: Date.now(),
            })
          );
          break;
        }
      }
    }

    if (onClick) {
      onClick(e);
    }
  };

  const handleMapOverImmediate = (e: MapLayerMouseEvent) => {
    // Extract features immediately
    const features = e.features;
    if (mapRef?.current) {
      // This is a hack to change the cursor to a pointer when hovering over a feature
      // It's not recommended to change the state of a component through internal methods
      // However, this is the only way to do it with the current version of react-map-gl
      // See https://github.com/visgl/react-map-gl/issues/579#issuecomment-1275163348
      const map = mapRef.current.getMap();
      if (mapCursor) {
        map.getCanvas().style.cursor = mapCursor;
      } else {
        map.getCanvas().style.cursor = features?.length ? "pointer" : "";
      }
    }
  };

  // Store ref to last dispatched zoom to avoid unnecessary Redux updates
  const lastDispatchedZoomRef = useRef<number | undefined>(undefined);
  const pendingZoomRef = useRef<number | undefined>(undefined);
  const zoomDebounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const handleMapLoad = useCallback(() => {
    if (mapRef?.current) {
      // get all icon layers and add icons to map using addOrUpdateMarkerImages method

      layers?.forEach((layer) => {
        if (layer.type === "feature" && layer.feature_layer_geometry_type === "point") {
          const pointFeatureProperties = layer.properties as FeatureLayerPointProperties;
          addOrUpdateMarkerImages(layer.id, pointFeatureProperties, mapRef.current);
        }
      });

      // load pattern images
      addPatternImages(PATTERN_IMAGES ?? [], mapRef.current);

      // load geolocation images
      const geolocationPulsingDot = createPulsingDot(mapRef.current);
      mapRef.current.addImage("geolocation-pulsing-dot", geolocationPulsingDot, { pixelRatio: 2 });

      // set current zoom (rounded to 1 decimal place for consistency)
      const map = mapRef.current.getMap();
      const zoom = map.getZoom();
      const roundedZoom = Math.round(zoom * 10) / 10;
      lastDispatchedZoomRef.current = roundedZoom;
      dispatch(setCurrentZoom(roundedZoom));

      // Apply label language based on current locale
      applyMapLanguage(map, i18n.language);
    }
    onLoad && onLoad();
  }, [layers, mapRef, onLoad, dispatch, i18n.language]);

  // Re-apply label language when locale changes or basemap style reloads
  useEffect(() => {
    if (!mapRef?.current) return;
    const map = mapRef.current.getMap();
    const apply = () => applyMapLanguage(map, i18n.language);
    // Apply now if style is already loaded
    if (map.isStyleLoaded()) {
      apply();
    }
    // Also apply whenever a new style finishes loading (basemap switch)
    map.on("styledata", apply);
    return () => {
      map.off("styledata", apply);
    };
  }, [i18n.language, mapRef, mapStyle]);

  const _onMove = useCallback(
    (e: ViewStateChangeEvent) => {
      if (onMove) {
        onMove(e);
      }
      if (mapRef?.current) {
        const map = mapRef.current.getMap();
        const zoom = map.getZoom();
        // Round to 1 decimal place to avoid excessive Redux updates during smooth zoom/pan
        // This prevents re-renders of components subscribed to currentZoom (like LayerTree)
        const roundedZoom = Math.round(zoom * 10) / 10;
        if (lastDispatchedZoomRef.current !== roundedZoom) {
          // Debounce the Redux dispatch — zoom visibility dimming in the layer tree
          // doesn't need real-time updates during zoom animation
          pendingZoomRef.current = roundedZoom;
          if (zoomDebounceRef.current) {
            clearTimeout(zoomDebounceRef.current);
          }
          zoomDebounceRef.current = setTimeout(() => {
            if (pendingZoomRef.current !== undefined) {
              lastDispatchedZoomRef.current = pendingZoomRef.current;
              dispatch(setCurrentZoom(pendingZoomRef.current));
            }
          }, 150);
        }
      }
    },
    [dispatch, onMove, mapRef]
  );

  const isMobile = useMediaQuery((theme: Theme) => theme.breakpoints.down("md"));

  return (
    <>
      <Box
        sx={{
          position: "relative",
          width: "100%",
          ".maplibregl-ctrl .maplibregl-ctrl-logo": {
            display: "none",
          },
          height: `100%`,
          ".maplibregl-popup-content": {
            padding: 0,
            borderRadius: "6px",
            background: theme.palette.background.paper,
          },
          ".maplibregl-popup-anchor-top .maplibregl-popup-tip, .maplibregl-popup-anchor-top-left .maplibregl-popup-tip, .maplibregl-popup-anchor-top-right .maplibregl-popup-tip":
            {
              borderBottomColor: theme.palette.background.paper,
            },
          ".maplibregl-popup-anchor-bottom .maplibregl-popup-tip, .maplibregl-popup-anchor-bottom-right .maplibregl-popup-tip, .maplibregl-popup-anchor-bottom-left .maplibregl-popup-tip":
            {
              borderTopColor: theme.palette.background.paper,
            },
          ".maplibregl-popup-anchor-left .maplibregl-popup-tip": {
            borderRightColor: theme.palette.background.paper,
          },
          ".maplibregl-popup-anchor-right .maplibregl-popup-tip": {
            borderLeftColor: theme.palette.background.paper,
          },
          // Measure label popup styles (chip-like, no arrow, no pointer events)
          ".measure-label-popup": {
            pointerEvents: "none",
          },
          ".measure-label-popup .maplibregl-popup-content": {
            padding: 0,
            background: "transparent",
            boxShadow: "none",
            borderRadius: "16px",
            pointerEvents: "none",
          },
          ".measure-label-popup .maplibregl-popup-tip": {
            display: "none",
          },
          ...containerSx,
        }}>
        <Map
          id="map"
          ref={mapRef}
          style={{ width: "100%", height: "100%" }}
          initialViewState={initialViewState}
          mapStyle={mapStyle}
          interactiveLayerIds={interactiveLayerIds}
          dragRotate={dragRotate}
          touchZoomRotate={touchZoomRotate}
          attributionControl={false}
          onMoveEnd={onMoveEnd}
          onClick={handleMapClick}
          onMouseMove={handleMapOverImmediate}
          onMove={_onMove}
          maxBounds={maxExtent}
          onLoad={handleMapLoad}>
          <DrawControl
            position="top-right"
            displayControlsDefault={false}
            defaultMode={MapboxDraw.constants.modes.SIMPLE_SELECT}
          />
          <Layers
            layers={layers}
            highlightFeature={highlightedFeature}
            scenarioFeatures={scenarioFeatures}
            selectedScenarioLayer={selectedScenarioEditLayer as ProjectLayer}
          />
          <ScenarioLayer scenarioLayerData={scenarioFeatures} projectLayers={layers as ProjectLayer[]} />
          <GeocoderLayer />
          <UserLocationLayer />
          <ToolboxLayers />
          <MeasureLabels />
          {!isMobile && popupInfo && <MapPopoverInfo key={highlightedFeature?.id ?? v4()} {...popupInfo} />}
          {!isMobile && popupEditor && isEditor && (
            <MapPopoverEditor
              key={popupEditor.feature?.id || popupEditor.feature?.properties?.id || v4()}
              {...popupEditor}
            />
          )}
        </Map>
        <FeatureEditToolbar
          onSave={featureEditorHandlers.handleSave}
          onDiscard={featureEditorHandlers.handleDiscardRequest}
          onStopEditing={featureEditorHandlers.handleStopEditingRequest}
          onUndo={featureEditorHandlers.handleUndo}
          onRedo={featureEditorHandlers.handleRedo}
          hasUndo={featureEditorHandlers.hasUndo}
          hasRedo={featureEditorHandlers.hasRedo}
        />
        <ConfirmModal
          open={featureEditorHandlers.discardConfirmOpen}
          title={t("discard_edits")}
          body={t("discard_edits_confirmation")}
          closeText={t("cancel")}
          confirmText={t("discard_edits")}
          onClose={featureEditorHandlers.handleDiscardCancel}
          onConfirm={featureEditorHandlers.handleDiscardConfirm}
        />
        <ConfirmModal
          open={featureEditorHandlers.stopConfirmOpen}
          title={t("stop_editing")}
          body={t("discard_edits_confirmation")}
          closeText={t("cancel")}
          confirmText={t("stop_editing")}
          onClose={featureEditorHandlers.handleStopCancel}
          onConfirm={featureEditorHandlers.handleStopConfirm}
        />
        {children}
      </Box>
    </>
  );
};

export default MapViewer;
