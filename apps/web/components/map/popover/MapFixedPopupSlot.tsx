"use client";

import centroid from "@turf/centroid";
import { useMemo } from "react";

import { setPopupPreview } from "@/lib/store/map/slice";
import type { PopupProperties } from "@/lib/validations/layer";
import type { ProjectLayer } from "@/lib/validations/project";

import { useAppDispatch, useAppSelector } from "@/hooks/store/ContextHooks";

import { buildLayerIcon } from "@/components/map/panels/layer/legend/LayerIcon";
import { seedPopupFromInteraction } from "@/components/map/panels/style/popup/seedFromLegacy";
import { MapFeaturePopover } from "@/components/map/popover/MapFeaturePopover";

export interface MapFixedPopupSlotProps {
  layers: ProjectLayer[] | undefined;
}

/**
 * Layout-level renderer for fixed-anchor popups.
 *
 * Why this exists: the maplibre `<Map>` creates its own stacking context
 * and clips child positioning to its own bounds, so rendering the fixed
 * popup inside MapViewer made it dependent on the map's parent
 * positioning rather than the layout's own chrome (toolbar / panels).
 * Layouts now mount this slot inside a Box that already sits within the
 * visible map area, and the popup positions to one of that Box's
 * corners via PopupFixedHost — same pattern as the top-left controls
 * (Geocoder, ToolboxCtrl, MeasureButton).
 *
 * In-place popups stay inside MapViewer because they anchor to map
 * coordinates and need `useMap`.
 */
export function MapFixedPopupSlot({ layers }: MapFixedPopupSlotProps) {
  const dispatch = useAppDispatch();
  const popupInfo = useAppSelector((s) => s.map.popupInfo);
  const popupPreview = useAppSelector((s) => s.map.popupPreview);
  const highlightedFeature = useAppSelector((s) => s.map.highlightedFeature);

  // Mirror MapViewer's lookup: popupInfo.layerId is set to `layer_id ?? id`
  // at dispatch time, so try both — ProjectLayer stores the dataset id on
  // `layer_id` while plain Layer uses `id`.
  const clickedLayer = useMemo(() => {
    if (!popupInfo?.layerId || !layers) return undefined;
    const target = popupInfo.layerId;
    return layers.find(
      (l) =>
        (l as { layer_id?: string }).layer_id === target ||
        l.id.toString() === target,
    );
  }, [popupInfo?.layerId, layers]);

  const activePopupConfig = useMemo<PopupProperties | undefined>(() => {
    if (!clickedLayer) return undefined;
    const props = clickedLayer.properties as
      | { popup?: PopupProperties; interaction?: { type?: string; content?: never[] } }
      | undefined;
    if (props?.popup) return props.popup;
    // Auto-seed from the legacy `interaction` config so layers that have
    // never been touched in the new popup editor still render the new
    // popup. Matches the editor's auto-seed in PopupSection.tsx.
    return seedPopupFromInteraction(props?.interaction);
  }, [clickedLayer]);

  const previewLayer = useMemo(() => {
    if (!popupPreview?.layerId || !layers) return undefined;
    const target = popupPreview.layerId;
    return layers.find(
      (l) =>
        (l as { layer_id?: string }).layer_id === target ||
        l.id.toString() === target,
    );
  }, [popupPreview?.layerId, layers]);

  const previewPopupConfig = useMemo<PopupProperties | undefined>(() => {
    if (!previewLayer) return undefined;
    const props = previewLayer.properties as
      | { popup?: PopupProperties; interaction?: { type?: string; content?: never[] } }
      | undefined;
    if (props?.popup) return props.popup;
    return seedPopupFromInteraction(props?.interaction);
  }, [previewLayer]);

  const clickedLayerIcon = useMemo(() => buildLayerIcon(clickedLayer), [clickedLayer]);
  const previewIcon = useMemo(() => buildLayerIcon(previewLayer), [previewLayer]);

  const previewCentroid = useMemo(() => {
    if (!popupPreview) return null;
    try {
      const c = centroid({
        type: "Feature",
        properties: {},
        geometry: popupPreview.feature.geometry as GeoJSON.Geometry,
      });
      const [lng, lat] = c.geometry.coordinates;
      return { lng, lat };
    } catch {
      return null;
    }
  }, [popupPreview]);

  const showClicked =
    Boolean(popupInfo) && activePopupConfig?.position === "fixed";
  const showPreview =
    !popupInfo &&
    Boolean(popupPreview) &&
    Boolean(previewLayer) &&
    previewPopupConfig?.position === "fixed" &&
    Boolean(previewCentroid);

  return (
    <>
      {showClicked && popupInfo && activePopupConfig && (
        <MapFeaturePopover
          layerId={popupInfo.layerId ?? ""}
          layerName={popupInfo.title}
          popup={activePopupConfig}
          properties={
            (popupInfo.featureProperties ??
              highlightedFeature?.properties ??
              popupInfo.properties ??
              {}) as Record<string, unknown>
          }
          lngLat={{ lng: popupInfo.lngLat[0], lat: popupInfo.lngLat[1] }}
          onClose={popupInfo.onClose}
          layerIcon={clickedLayerIcon}
        />
      )}
      {showPreview &&
        popupPreview &&
        previewLayer &&
        previewPopupConfig &&
        previewCentroid && (
          <MapFeaturePopover
            layerId={popupPreview.layerId}
            layerName={previewLayer.name ?? ""}
            popup={previewPopupConfig}
            properties={popupPreview.feature.properties}
            lngLat={previewCentroid}
            onClose={() => dispatch(setPopupPreview(null))}
            layerIcon={previewIcon}
          />
        )}
    </>
  );
}
