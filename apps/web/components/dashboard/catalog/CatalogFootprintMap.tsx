import { Alert, Box, Paper, Skeleton, useTheme } from "@mui/material";
import bboxOf from "@turf/bbox";
import "maplibre-gl/dist/maplibre-gl.css";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import type { MapGeoJSONFeature, MapLayerMouseEvent } from "react-map-gl/maplibre";
import { Layer as MapLayer, Map as MapLibre, Source } from "react-map-gl/maplibre";

import { useCatalogPreview, useCatalogStyle } from "@/lib/api/catalog";
import { catalogPaint } from "@/lib/catalog/style";
import { getHightlightStyleSpec } from "@/lib/transformers/layer";
import { detailPopupConfig } from "@/lib/utils/map/detailPopup";
import type { CatalogItem } from "@/lib/validations/catalog";
import type { PopupProperties } from "@/lib/validations/layer";

import { useCatalogBasemapStyle } from "@/hooks/catalog/useCatalogBasemapStyle";

import DetailMapAttribution from "@/components/dashboard/common/DetailMapAttribution";
import { LayerLegendPanel } from "@/components/map/panels/layer/legend/LayerLegend";
import { MapFeaturePopover } from "@/components/map/popover/MapFeaturePopover";
import type { LayerField } from "@/components/map/popover/formatFeatureProperties";

/** Where the dataset is, and — where the deployment allows it — what it contains. */

const FOOTPRINT_COLOR = "#2278CF";

/** Room kept clear for the credit strip, which sits in the same corner the panel is pinned to. */
const ATTRIBUTION_ROOM = 24;

type PickedFeature = {
  lngLat: { lng: number; lat: number };
  properties: Record<string, unknown>;
  /** The queried feature, which the shared highlight spec reads id + paint off. */
  feature: MapGeoJSONFeature;
};

const STYLED_LAYER = "catalog-geometry-styled";
const PLAIN_LAYERS = ["catalog-geometry-fill", "catalog-geometry-line", "catalog-geometry-point"];

const CatalogFootprintMap = ({
  item,
  /** Fills its container instead of standing at a fixed height. */
  fill,
}: {
  item: CatalogItem;
  fill?: boolean;
}) => {
  const { t } = useTranslation("common");
  const theme = useTheme();
  const basemapStyle = useCatalogBasemapStyle();
  const { preview, isLoading } = useCatalogPreview(item.id);

  const footprint = useMemo<GeoJSON.FeatureCollection | null>(() => {
    if (!item.geometry) return null;
    return {
      type: "FeatureCollection",
      features: [{ type: "Feature", geometry: item.geometry, properties: {} }],
    };
  }, [item.geometry]);

  const sample = useMemo<GeoJSON.FeatureCollection | null>(() => {
    // A preview of a dataset without geometry is rows, not shapes — there is no
    // map for this component to be on in that case, but the sample is still
    // filtered rather than trusted to be drawable.
    const drawable = (preview?.features ?? []).filter(
      (feature): feature is GeoJSON.Feature => !!feature.geometry
    );
    return drawable.length ? { type: "FeatureCollection", features: drawable } : null;
  }, [preview]);
  const shown = sample ?? footprint;

  // Only worth fetching a style when there is data to draw with it.
  const { style } = useCatalogStyle(item.assets?.style?.href, !!sample);
  const geometryType = item.properties["goat:geometryType"] ?? undefined;
  const styled = catalogPaint(style, geometryType);
  /** The dataset's own rendering, and some of its data to draw with it. */
  const styledSample = !!sample && !!styled && !!geometryType;

  /** Field definitions for the popup, from what the dataset published. */
  const fields = useMemo<LayerField[]>(
    () =>
      (item.properties["table:columns"] ?? []).map((column) => ({
        name: column.name,
        type: column.type ?? "text",
      })),
    [item.properties]
  );

  /** The map's rendered height, and how much of its bottom the attribution takes. */
  const mapBox = useRef<HTMLDivElement | null>(null);
  const [panelHeight, setPanelHeight] = useState(420);
  const [attributionRoom, setAttributionRoom] = useState(ATTRIBUTION_ROOM);
  const measure = useCallback(() => {
    const node = mapBox.current;
    if (!node) return;
    const box = node.getBoundingClientRect();
    setPanelHeight(box.height);
    // Distance from the top of the credit strip to the bottom of the map.
    const attribution = node.querySelector("[data-catalog-attribution]");
    if (!attribution) return;
    const room = box.bottom - attribution.getBoundingClientRect().top;
    if (room > 0 && room < box.height) setAttributionRoom(room);
  }, []);
  useEffect(() => {
    const node = mapBox.current;
    if (!node) return;
    const observer = new ResizeObserver(() => measure());
    observer.observe(node);
    return () => observer.disconnect();
  }, [measure]);

  /** A popup configuration for a dataset that has none. */
  const popup = useMemo<PopupProperties>(
    () => detailPopupConfig(fields, panelHeight, attributionRoom),
    [fields, panelHeight, attributionRoom]
  );

  /** Hover previews, a click pins; a pin outranks the hover and ignores it until dismissed. */
  const [picked, setPicked] = useState<PickedFeature | null>(null);
  const [hovered, setHovered] = useState<PickedFeature | null>(null);
  const active = picked ?? hovered;
  /** Whether the cursor is over a feature, so the pointer means something. */
  const [hovering, setHovering] = useState(false);

  /** The sample feature under a pointer position. */
  const featureAt = (event: MapLayerMouseEvent) => {
    const wanted = styledSample ? [STYLED_LAYER] : PLAIN_LAYERS;
    // Read from `getStyle()`: `getLayer()` returns nothing through react-map-gl's
    // wrapper, and querying a layer the style has yet to add logs an error.
    const present = new Set((event.target.getStyle()?.layers ?? []).map((layer) => layer.id));
    const layers = wanted.filter((id) => present.has(id));
    if (!layers.length) return undefined;
    const { x, y } = event.point;
    // A two-point box, never `event.point`: an unrecognised point argument makes
    // `queryRenderedFeatures` fall back to the whole viewport and return the first
    // feature anywhere on the map. The 5px reach also makes a 3px dot clickable.
    const box: [[number, number], [number, number]] = [
      [x - 5, y - 5],
      [x + 5, y + 5],
    ];
    return event.target.queryRenderedFeatures(box, { layers }).at(0);
  };

  const asPicked = (event: MapLayerMouseEvent, hit: MapGeoJSONFeature): PickedFeature => ({
    lngLat: { lng: event.lngLat.lng, lat: event.lngLat.lat },
    properties: (hit.properties ?? {}) as Record<string, unknown>,
    feature: hit,
  });

  const onMapClick = (event: MapLayerMouseEvent) => {
    if (!sample) return;
    const hit = featureAt(event);
    // A click on the basemap unpins. Same gesture, both directions -- which is
    // what lets the panel do without a close control.
    setPicked(hit ? asPicked(event, hit) : null);
  };

  const onMapMouseMove = (event: MapLayerMouseEvent) => {
    if (!sample) return;
    const hit = featureAt(event);
    setHovering(!!hit);
    // A pinned panel is the reader's, not the cursor's.
    if (picked) return;
    setHovered((prev) => {
      if (!hit) return prev ? null : prev;
      // Same feature under the cursor: keep the previous object.
      if (prev && prev.feature.id === hit.id) return prev;
      return asPicked(event, hit);
    });
  };

  /** Escape dismisses the panel. */
  useEffect(() => {
    if (!active) return;
    const onKey = (event: KeyboardEvent) => {
      if (event.key !== "Escape") return;
      setPicked(null);
      setHovered(null);
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [active]);

  const initialViewState = useMemo(() => {
    const box = item.bbox ?? (shown ? (bboxOf(shown) as number[]) : null);
    if (!box || box.length < 4) return { longitude: 10, latitude: 51, zoom: 3 };
    return {
      bounds: [box[0], box[1], box[2], box[3]] as [number, number, number, number],
      fitBoundsOptions: { padding: 40 },
    };
  }, [item.bbox, shown]);

  if (!shown) {
    return <Alert severity="info">{t("catalog_no_geometry")}</Alert>;
  }

  return (
    <Box sx={fill ? { position: "absolute", inset: 0 } : undefined}>
      {isLoading && <Skeleton variant="rectangular" height={4} sx={{ mb: 1 }} />}
      <Box
        ref={mapBox}
        sx={{
          ...(fill
            ? { position: "absolute", inset: 0 }
            : { height: 420, borderRadius: 1, overflow: "hidden" }),
        }}>
        <MapLibre
          // Named for the same reason as the catalog's other maps: it must not
          // collide with a host page's own map inside a shared `MapProvider`.
          id="catalog-footprint"
          initialViewState={initialViewState}
          style={{ width: "100%", height: "100%" }}
          mapStyle={basemapStyle}
          // GOAT's own strip replaces it, below.
          attributionControl={false}
          cursor={hovering ? "pointer" : "grab"}
          onClick={onMapClick}
          onMouseMove={onMapMouseMove}
          onMouseOut={() => {
            setHovering(false);
            // The preview follows the cursor off the map; a pinned panel stays.
            setHovered(null);
          }}
          // The credit strip only exists once the map has loaded.
          onLoad={measure}
          // Moving the map would leave the panel describing a feature that is no
          // longer in view, so it drops both the pin and the preview.
          onMoveStart={() => {
            setPicked(null);
            setHovered(null);
          }}>
          {/* `generateId`: the highlight spec targets a feature by id, and preview
              GeoJSON carries none. */}
          <Source id="catalog-geometry" type="geojson" data={shown} generateId>
            {/* Layers stay direct children — `Source` injects its id only into
                children it can see, and a wrapping fragment hides them. */}
            {styledSample && (
              <MapLayer
                {...({
                  id: STYLED_LAYER,
                  ...styled,
                  // No geometry-type filter, unlike the plain layers: this layer's
                  // type already comes from the dataset's declared geometry, and a
                  // filter repeating it would blank the layer if the two disagreed.
                } as React.ComponentProps<typeof MapLayer>)}
              />
            )}
            {!styledSample && (
              <MapLayer
                id="catalog-geometry-fill"
                type="fill"
                paint={{ "fill-color": FOOTPRINT_COLOR, "fill-opacity": sample ? 0.35 : 0.12 }}
                filter={["==", ["geometry-type"], "Polygon"]}
              />
            )}
            {!styledSample && (
              <MapLayer
                id="catalog-geometry-line"
                type="line"
                paint={{ "line-color": FOOTPRINT_COLOR, "line-width": 1.5 }}
              />
            )}
            {!styledSample && (
              <MapLayer
                id="catalog-geometry-point"
                type="circle"
                paint={{ "circle-color": FOOTPRINT_COLOR, "circle-radius": 3 }}
                filter={["==", ["geometry-type"], "Point"]}
              />
            )}

            {/* The app's own highlight for an active feature. */}
            {active && !!getHightlightStyleSpec(active.feature) && (
              <MapLayer
                {...({
                  id: "catalog-geometry-highlight",
                  ...getHightlightStyleSpec(active.feature),
                } as React.ComponentProps<typeof MapLayer>)}
              />
            )}
          </Source>

          {active && (
            <Box
              sx={{
                // A hover preview must not take the pointer: it opens over the
                // features being swept, and the `mouseout` that follows would clear
                // the hover and flicker the panel. Pinned, it becomes interactive.
                ...(picked ? null : { "&, & *": { pointerEvents: "none !important" } }),
              }}>
              <MapFeaturePopover
                fields={fields}
                popup={popup}
                properties={active.properties}
                lngLat={active.lngLat}
                onClose={() => {
                  setPicked(null);
                  setHovered(null);
                }}
              />
            </Box>
          )}

          {/* Capped: the legend owns the bottom-left of this map. */}
          <DetailMapAttribution maxWidth={styledSample ? "62%" : "100%"} />
        </MapLibre>

        {/* The dataset's legend, drawn by the Layers panel's own component from the
            same style the features use. */}
        {styledSample && (
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
              // Above the credit strip, which sets a z-index of its own and would
              // otherwise cover the legend where the two meet on a narrow map.
              zIndex: 2,
            }}>
            {/* Headings kept: they read "Fill color based on: measure", which is what turns a column of swatches into an explanation. */}
            <LayerLegendPanel properties={style as Record<string, unknown>} geometryType={geometryType} />
          </Paper>
        )}
      </Box>
    </Box>
  );
};

export default CatalogFootprintMap;
