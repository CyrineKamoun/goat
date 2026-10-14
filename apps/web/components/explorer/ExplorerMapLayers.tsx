/**
 * The selected route on the map: its lines from the bundle's shape_lines
 * tiles, straight lines between stops where a pattern has no shape, and its
 * stops; and the stop picked in a list, ringed. Rendered inside <Map>; reads
 * what the panels put in the store. Layer ids start with `explorer-`, which the layer restack keeps
 * above the data (Layers.tsx).
 */
import { useTheme } from "@mui/material/styles";
import type { FeatureCollection } from "geojson";
import { useEffect, useMemo } from "react";
import { Layer, Source, useMap } from "react-map-gl/maplibre";

import { GEOAPI_BASE_URL } from "@/lib/constants";

import { useAppSelector } from "@/hooks/store/ContextHooks";

import { EXPLORER_PANEL_WIDTH } from "@/components/explorer/ExplorerLeftPanel";

export function ExplorerMapLayers() {
  const drawing = useAppSelector((s) => (s.explorer.active ? s.explorer.drawing : null));
  const picked = useAppSelector((s) => (s.explorer.active ? s.explorer.selectedStop : null));
  const { current: map } = useMap();
  const theme = useTheme();

  const pickedPoint = useMemo<FeatureCollection>(
    () => ({
      type: "FeatureCollection",
      features: picked
        ? [
            {
              type: "Feature",
              geometry: { type: "Point", coordinates: [picked.lon, picked.lat] },
              properties: { name: picked.name },
            },
          ]
        : [],
    }),
    [picked]
  );

  // Fly to a picked stop, clear of the left panel; zoom in only as far as a
  // stop needs, never out from a closer look.
  useEffect(() => {
    if (!map || !picked) return;
    map.flyTo({
      center: [picked.lon, picked.lat],
      zoom: Math.max(map.getZoom(), 15),
      padding: { top: 0, bottom: 0, left: EXPLORER_PANEL_WIDTH, right: 0 },
    });
  }, [map, picked]);

  const stops = useMemo<FeatureCollection>(
    () => ({
      type: "FeatureCollection",
      features: (drawing?.stops ?? []).map((s) => ({
        type: "Feature",
        geometry: { type: "Point", coordinates: [s.lon, s.lat] },
        properties: { name: s.name },
      })),
    }),
    [drawing?.stops]
  );
  const chains = useMemo<FeatureCollection>(
    () => ({
      type: "FeatureCollection",
      features: (drawing?.stopChains ?? [])
        .filter((c) => c.length > 1)
        .map((coordinates) => ({
          type: "Feature",
          geometry: { type: "LineString", coordinates },
          properties: {},
        })),
    }),
    [drawing?.stopChains]
  );

  // Fit the view to a route when it is selected, clear of the left panel.
  useEffect(() => {
    if (!map || !drawing?.stops.length) return;
    const lons = drawing.stops.map((s) => s.lon);
    const lats = drawing.stops.map((s) => s.lat);
    map.fitBounds(
      [
        [Math.min(...lons), Math.min(...lats)],
        [Math.max(...lons), Math.max(...lats)],
      ],
      { padding: { top: 80, bottom: 120, left: EXPLORER_PANEL_WIDTH + 60, right: 80 }, maxZoom: 15 }
    );
    // Only when another route is chosen, not on every update of the same one.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [map, drawing?.routeId]);

  const pickedLayers = (
    <Source key="explorer-picked-stop" id="explorer-picked-stop" type="geojson" data={pickedPoint}>
      <Layer
        id="explorer-picked-stop-halo"
        type="circle"
        paint={{ "circle-radius": 14, "circle-color": theme.palette.primary.main, "circle-opacity": 0.2 }}
      />
      <Layer
        id="explorer-picked-stop-dot"
        type="circle"
        paint={{
          "circle-radius": 6,
          "circle-color": "#FFFFFF",
          "circle-stroke-color": theme.palette.primary.main,
          "circle-stroke-width": 3,
        }}
      />
    </Source>
  );

  // Keyed sources: with or without a route drawn, each keeps its own place,
  // because react-map-gl refuses a source whose id changes under it.
  if (!drawing) return pickedLayers;
  const color = drawing.color ?? "#2E7D32";
  return (
    <>
      {drawing.shapeLinesLayerId && drawing.shapeIds.length > 0 && (
        <Source
          key="explorer-route-shapes"
          id="explorer-route-shapes"
          type="vector"
          tiles={[
            `${GEOAPI_BASE_URL}/collections/${drawing.shapeLinesLayerId}/tiles/WebMercatorQuad/{z}/{x}/{y}`,
          ]}
          maxzoom={14}>
          <Layer
            id="explorer-route-casing"
            type="line"
            source-layer="default"
            filter={["in", ["get", "shape_id"], ["literal", drawing.shapeIds]]}
            layout={{ "line-cap": "round", "line-join": "round" }}
            paint={{ "line-color": "#FFFFFF", "line-width": 8 }}
          />
          <Layer
            id="explorer-route-line"
            type="line"
            source-layer="default"
            filter={["in", ["get", "shape_id"], ["literal", drawing.shapeIds]]}
            layout={{ "line-cap": "round", "line-join": "round" }}
            paint={{ "line-color": color, "line-width": 4.5 }}
          />
        </Source>
      )}
      <Source key="explorer-route-chains" id="explorer-route-chains" type="geojson" data={chains}>
        <Layer
          id="explorer-route-chain-line"
          type="line"
          paint={{ "line-color": color, "line-width": 3, "line-dasharray": [2, 1.5] }}
        />
      </Source>
      <Source key="explorer-route-stops" id="explorer-route-stops" type="geojson" data={stops}>
        <Layer
          id="explorer-route-stop"
          type="circle"
          paint={{
            "circle-radius": 5,
            "circle-color": "#FFFFFF",
            "circle-stroke-color": color,
            "circle-stroke-width": 2.5,
          }}
        />
      </Source>
      {pickedLayers}
    </>
  );
}
