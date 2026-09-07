import { Box, Paper, useTheme } from "@mui/material";
import bbox from "@turf/bbox";
import "maplibre-gl/dist/maplibre-gl.css";
import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { MapRef } from "react-map-gl/maplibre";
import { MapProvider } from "react-map-gl/maplibre";

import { getBasemapUrl } from "@/lib/constants/basemaps";
import { DrawProvider } from "@/lib/providers/DrawProvider";
import { setHighlightedFeature, setPopupInfo } from "@/lib/store/map/slice";
import { hasLegend } from "@/lib/utils/datasets";
import { detailPopupConfig } from "@/lib/utils/map/detailPopup";
import { globalExtent, wktToGeoJSON } from "@/lib/utils/map/wkt";
import type { Layer } from "@/lib/validations/layer";
import type { ProjectLayer } from "@/lib/validations/project";

import { useCatalogBasemapStyle } from "@/hooks/catalog/useCatalogBasemapStyle";
import useLayerFields from "@/hooks/map/CommonHooks";
import { useAppDispatch } from "@/hooks/store/ContextHooks";

import DetailMapAttribution from "@/components/dashboard/common/DetailMapAttribution";
import MapViewer from "@/components/map/MapViewer";
import { LayerLegendPanel } from "@/components/map/panels/layer/legend/LayerLegend";
import { MapFixedPopupSlot } from "@/components/map/popover/MapFixedPopupSlot";

/** Room kept clear for the credit strip, which sits in the corner opposite the pinned panel. */
const ATTRIBUTION_ROOM = 24;

/** `useLayerFields` defaults this to a fresh array, which would give its result
 * a new identity every render and defeat the memos below. */
const HIDDEN_FIELDS = ["layer_id", "id", "h3_3", "h3_6", "geom", "geometry"];

interface DatasetMapPreviewProps {
  dataset: Layer;
}

/** An owned layer on a map: the layer's own tiles, styled by its own
 * properties, under the chrome the catalog's detail map wears. It fills the
 * box it is placed in, which is where its height comes from. */
const DatasetMapPreview: React.FC<DatasetMapPreviewProps> = ({ dataset }) => {
  const theme = useTheme();
  const mapRef = useRef<MapRef | null>(null);
  // `MapViewer` needs a style from the first render; the light basemap stands
  // in where the theme lookup finds none.
  const basemapStyle = useCatalogBasemapStyle() ?? getBasemapUrl(null);
  const geojson = wktToGeoJSON(dataset.extent || globalExtent);
  const boundingBox = bbox(geojson);
  const dispatch = useAppDispatch();
  const { layerFields } = useLayerFields(dataset.id, undefined, HIDDEN_FIELDS);

  // `popupInfo` and the highlight live in the shared map store, so a panel left
  // open here would survive into the next dialog or a project map.
  useEffect(
    () => () => {
      dispatch(setPopupInfo(undefined));
      dispatch(setHighlightedFeature(undefined));
    },
    [dispatch]
  );

  /** The map's rendered height, which the pinned feature panel is sized
   * against. Measured on the box wrapping `MapViewer`, which fills the frame
   * the map is placed in. */
  const mapBox = useRef<HTMLDivElement | null>(null);
  const [panelHeight, setPanelHeight] = useState(420);
  const measure = useCallback(() => {
    const node = mapBox.current;
    if (node) setPanelHeight(node.getBoundingClientRect().height);
  }, []);
  useEffect(() => {
    const node = mapBox.current;
    if (!node) return;
    const observer = new ResizeObserver(() => measure());
    observer.observe(node);
    return () => observer.disconnect();
  }, [measure]);

  /** A popup configuration for a dataset that has none: hover previews a
   * feature, a click pins it. Raster layers have no features to read. */
  const popup = useMemo(
    () =>
      dataset.type === "raster" ? undefined : detailPopupConfig(layerFields, panelHeight, ATTRIBUTION_ROOM),
    [dataset.type, layerFields, panelHeight]
  );

  // Ensure the layer has visibility enabled for preview
  const layerWithVisibility = useMemo(() => {
    return {
      ...dataset,
      properties: {
        ...dataset.properties,
        visibility: true,
        ...(popup ? { popup } : {}),
      },
    };
  }, [dataset, popup]);

  return (
    <DrawProvider>
      <MapProvider>
        <Box ref={mapBox} sx={{ position: "relative", height: "100%" }}>
          <MapViewer
            mapRef={mapRef}
            layers={[layerWithVisibility]}
            initialViewState={{
              bounds: boundingBox as [number, number, number, number],
              fitBoundsOptions: { padding: 40 },
            }}
            mapStyle={basemapStyle}
            dragRotate={false}
            touchZoomRotate={false}
            // The highlight alone marks the hovered feature, as on the catalog map.
            activeFeatureMarker={false}
            containerSx={{
              position: "relative",
              display: "flex",
              height: "100%",
              overflow: "hidden",
            }}>
            {/* Capped: the legend owns the bottom-left of this map. */}
            <DetailMapAttribution maxWidth="62%" />

            {/* `MapViewer` renders in-place popups only; a pinned one is drawn by
                this slot, which anchors to the nearest positioned box — here
                `MapViewer`'s own root. The slot reads `id`, `layer_id`, `name`,
                `properties`, `layer_type` and `feature_layer_geometry_type`,
                all of which a plain `Layer` carries or leaves undefined. */}
            <MapFixedPopupSlot layers={[layerWithVisibility] as unknown as ProjectLayer[]} />

            {/* The dataset's legend, drawn by the Layers panel's own component
                from the same style the features are rendered with — only where
                that style has something to explain. */}
            {hasLegend(dataset) && (
              <Paper
                elevation={0}
                sx={{
                  position: "absolute",
                  left: 8,
                  bottom: 8,
                  maxWidth: 280,
                  maxHeight: "70%",
                  overflowY: "auto",
                  px: 2,
                  py: 1.5,
                  borderRadius: 1.5,
                  backgroundColor: theme.palette.background.paper,
                  opacity: 0.96,
                  // Above the credit strip, which sets a z-index of its own and
                  // would otherwise cover the legend where the two meet on a
                  // narrow map.
                  zIndex: 2,
                }}>
                <LayerLegendPanel
                  properties={dataset.properties as Record<string, unknown>}
                  geometryType={dataset.feature_layer_geometry_type ?? ""}
                />
              </Paper>
            )}
          </MapViewer>
        </Box>
      </MapProvider>
    </DrawProvider>
  );
};

export default DatasetMapPreview;
