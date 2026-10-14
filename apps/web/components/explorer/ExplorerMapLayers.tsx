/**
 * The selected route on the map: its lines from the bundle's shape_lines
 * tiles, straight lines between stops where a pattern has no shape, and its
 * stops. Rendered inside <Map>; reads the drawing the route summary puts in
 * the store. Layer ids start with `explorer-`, which the layer restack keeps
 * above the data (Layers.tsx).
 */
import type { FeatureCollection } from "geojson";
import { useEffect, useMemo } from "react";
import { Layer, Source, useMap } from "react-map-gl/maplibre";

import { GEOAPI_BASE_URL } from "@/lib/constants";

import { useAppSelector } from "@/hooks/store/ContextHooks";

import { EXPLORER_PANEL_WIDTH } from "@/components/explorer/ExplorerLeftPanel";

export function ExplorerMapLayers() {
  const drawing = useAppSelector((s) => (s.explorer.active ? s.explorer.drawing : null));
  const { current: map } = useMap();

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

  if (!drawing) return null;
  const color = drawing.color ?? "#2E7D32";
  return (
    <>
      {drawing.shapeLinesLayerId && drawing.shapeIds.length > 0 && (
        <Source
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
      <Source id="explorer-route-chains" type="geojson" data={chains}>
        <Layer
          id="explorer-route-chain-line"
          type="line"
          paint={{ "line-color": color, "line-width": 3, "line-dasharray": [2, 1.5] }}
        />
      </Source>
      <Source id="explorer-route-stops" type="geojson" data={stops}>
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
    </>
  );
}
