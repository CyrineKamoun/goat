/**
 * The Explorer on the map, drawn by itself whatever GOAT's layers beneath
 * show: the bundle's whole network in its routes' colours (shape_lines
 * tiles, each shape styled from the snapshot), the selected route over it
 * with its stops, the others faded; playback's vehicles; the picked stop.
 * Clicking a line selects its route.
 *
 * Rendered inside <Map>, outside the Explorer's provider: it reads what the
 * panels put in the store, and the network and playback stores. Sources are
 * always mounted, in drawing order; layer ids start with `explorer-`, which
 * the layer restack keeps above the data (Layers.tsx).
 */
import { Box, ButtonBase, Stack, Typography } from "@mui/material";
import { useTheme } from "@mui/material/styles";
import type { FeatureCollection } from "geojson";
import type {
  ExpressionSpecification,
  FilterSpecification,
  GeoJSONSource,
  MapLayerMouseEvent,
} from "maplibre-gl";
import { useEffect, useMemo, useState, useSyncExternalStore } from "react";
import { Layer, Popup, Source, useMap } from "react-map-gl/maplibre";

import { GEOAPI_BASE_URL } from "@/lib/constants";
import { routeMatches } from "@/lib/explorer/filters";
import { networkStore } from "@/lib/explorer/network";
import { playbackClock } from "@/lib/explorer/playback";
import { selectExplorerRoute } from "@/lib/store/explorer/slice";

import { useAppDispatch, useAppSelector } from "@/hooks/store/ContextHooks";

import { EXPLORER_PANEL_WIDTH } from "@/components/explorer/ExplorerLeftPanel";

const EMPTY: FeatureCollection = { type: "FeatureCollection", features: [] };
const NETWORK = "explorer-network";
const FADED = "#9E9E9E";
/** The shape_lines tiles name their layer this. */
const TILE_LAYER = "default";

export function ExplorerMapLayers() {
  const dispatch = useAppDispatch();
  const active = useAppSelector((s) => s.explorer.active);
  const drawing = useAppSelector((s) => (s.explorer.active ? s.explorer.drawing : null));
  const picked = useAppSelector((s) => (s.explorer.active ? s.explorer.selectedStop : null));
  const selectedRouteId = useAppSelector((s) => (s.explorer.active ? s.explorer.selectedRouteId : null));
  const filters = useAppSelector((s) => s.explorer.filters);
  const network = useSyncExternalStore(
    networkStore.subscribe,
    networkStore.getSnapshot,
    networkStore.getSnapshot
  );
  const { current: map } = useMap();
  const theme = useTheme();
  // One array for as long as the layer is the same: a new one makes the map
  // replace the source, and with it every shape's styling.
  const networkTiles = useMemo(
    () => [`${GEOAPI_BASE_URL}/collections/${network.shapeLinesLayerId}/tiles/WebMercatorQuad/{z}/{x}/{y}`],
    [network.shapeLinesLayerId]
  );
  const [choice, setChoice] = useState<{ lon: number; lat: number; routeIds: string[] } | null>(null);

  // Playback's frames go to the map directly, not through React: many a
  // second, and nothing else on the map changes with them.
  const pushVehicles = () => {
    const source = map?.getSource("explorer-vehicles") as GeoJSONSource | undefined;
    source?.setData(playbackClock.getSnapshot().vehicles);
  };
  useEffect(() => playbackClock.subscribe(pushVehicles));
  useEffect(pushVehicles);

  // Each shape's colour, and whether it is faded: by the selected route, and
  // by the Explorer tab's filter. Set as feature-state, which the map keeps
  // for shapes whose tiles are not loaded yet.
  useEffect(() => {
    if (!map || !active) return;
    const onlyStops = filters.types.length === 1 && filters.types[0] === "stops";
    const narrowing = filters.modes.length > 0 || filters.agencies.length > 0;
    const apply = () => {
      if (!map.getSource(NETWORK)) return;
      for (const [id, shape] of network.shapes) {
        const shown = shape.route_ids.some((r) => {
          const route = network.routes.get(r);
          return route ? routeMatches(route, filters) : false;
        });
        const faded =
          onlyStops || (selectedRouteId ? !shape.route_ids.includes(selectedRouteId) : narrowing && !shown);
        map.setFeatureState({ source: NETWORK, sourceLayer: TILE_LAYER, id }, { color: shape.color, faded });
      }
    };
    apply();
    // Whenever the source is (re)added — after this first runs, or when the
    // style reloads — its features start without state: style them again.
    const onData = (e: { sourceId?: string; sourceDataType?: string }) => {
      if (e.sourceId === NETWORK && e.sourceDataType === "metadata") apply();
    };
    map.on("sourcedata", onData);
    return () => {
      map.off("sourcedata", onData);
    };
  }, [map, active, network, selectedRouteId, filters]);

  // A click on a line selects its route; where several routes share the
  // shapes under the pointer, they are offered to choose from.
  useEffect(() => {
    if (!map || !active) return;
    const layer = "explorer-network-line";
    const onClick = (e: MapLayerMouseEvent) => {
      const routeIds = [
        ...new Set(
          (e.features ?? []).flatMap(
            (f) => network.shapes.get(String(f.properties?.shape_id ?? f.id))?.route_ids ?? []
          )
        ),
      ];
      if (routeIds.length === 1) dispatch(selectExplorerRoute(routeIds[0]));
      else if (routeIds.length > 1) setChoice({ lon: e.lngLat.lng, lat: e.lngLat.lat, routeIds });
    };
    const enter = () => (map.getCanvas().style.cursor = "pointer");
    const leave = () => (map.getCanvas().style.cursor = "");
    map.on("click", layer, onClick);
    map.on("mouseenter", layer, enter);
    map.on("mouseleave", layer, leave);
    return () => {
      map.off("click", layer, onClick);
      map.off("mouseenter", layer, enter);
      map.off("mouseleave", layer, leave);
    };
  }, [map, active, network, dispatch]);

  const pickedPoint = useMemo<FeatureCollection>(
    () =>
      picked
        ? {
            type: "FeatureCollection",
            features: [
              {
                type: "Feature",
                geometry: { type: "Point", coordinates: [picked.lon, picked.lat] },
                properties: { name: picked.name },
              },
            ],
          }
        : EMPTY,
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

  if (!active) return null;
  const color = drawing?.color ?? theme.palette.primary.main;
  const selectedShapes: FilterSpecification = [
    "in",
    ["get", "shape_id"],
    ["literal", drawing?.shapeIds ?? []],
  ];
  const isMine: FilterSpecification = ["==", ["get", "route_id"], selectedRouteId ?? ""];
  const none: FilterSpecification = ["boolean", false];
  const faded: ExpressionSpecification = ["boolean", ["feature-state", "faded"], false];

  return (
    <>
      {network.shapeLinesLayerId && (
        <Source
          key={NETWORK}
          id={NETWORK}
          type="vector"
          tiles={networkTiles}
          promoteId="shape_id"
          maxzoom={14}>
          {/* Every line in its route's colour; faded ones grey and thin. */}
          <Layer
            id="explorer-network-line"
            type="line"
            source-layer={TILE_LAYER}
            layout={{ "line-cap": "round", "line-join": "round" }}
            paint={{
              "line-color": ["case", faded, FADED, ["coalesce", ["feature-state", "color"], FADED]],
              "line-opacity": ["case", faded, 0.35, 0.9],
              "line-width": [
                "interpolate",
                ["linear"],
                ["zoom"],
                8,
                ["case", faded, 0.6, 1.2],
                14,
                ["case", faded, 1.5, 3.5],
              ],
            }}
          />
          {/* The selected route over the rest, on a white casing. */}
          <Layer
            id="explorer-route-casing"
            type="line"
            source-layer={TILE_LAYER}
            filter={selectedShapes}
            layout={{ "line-cap": "round", "line-join": "round" }}
            paint={{ "line-color": "#FFFFFF", "line-width": 8 }}
          />
          <Layer
            id="explorer-route-line"
            type="line"
            source-layer={TILE_LAYER}
            filter={selectedShapes}
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
      {/* Playback's vehicles: dots in their route's colour; with a route
          chosen, the others go grey and its own carry its number. */}
      <Source key="explorer-vehicles" id="explorer-vehicles" type="geojson" data={EMPTY}>
        <Layer
          id="explorer-vehicle-dot"
          type="circle"
          filter={selectedRouteId ? ["!", isMine] : ["all"]}
          paint={{
            "circle-radius": ["interpolate", ["linear"], ["zoom"], 8, 2, 13, 4, 16, 6],
            "circle-color": selectedRouteId ? FADED : ["get", "color"],
            "circle-stroke-color": "#FFFFFF",
            "circle-stroke-width": 1,
          }}
        />
        <Layer
          id="explorer-vehicle-mine"
          type="circle"
          filter={selectedRouteId ? isMine : none}
          paint={{
            "circle-radius": 11,
            "circle-color": ["get", "color"],
            "circle-stroke-color": "#FFFFFF",
            "circle-stroke-width": 2,
          }}
        />
        <Layer
          id="explorer-vehicle-mine-label"
          type="symbol"
          filter={selectedRouteId ? isMine : none}
          layout={{
            "text-field": ["get", "label"],
            "text-size": 10,
            "text-font": ["Roboto Bold", "Roboto Regular"],
            "text-allow-overlap": true,
            "text-ignore-placement": true,
          }}
          paint={{ "text-color": "#FFFFFF" }}
        />
      </Source>
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
      {choice && (
        <Popup
          longitude={choice.lon}
          latitude={choice.lat}
          closeOnClick
          onClose={() => setChoice(null)}
          maxWidth="260px">
          <Stack sx={{ py: 0.5 }}>
            {choice.routeIds.map((id) => {
              const r = network.routes.get(id);
              if (!r) return null;
              return (
                <ButtonBase
                  key={id}
                  onClick={() => {
                    dispatch(selectExplorerRoute(id));
                    setChoice(null);
                  }}
                  sx={{ justifyContent: "flex-start", gap: 1, px: 1, py: 0.5, borderRadius: 1 }}>
                  <Box
                    component="span"
                    sx={{
                      px: 0.75,
                      minWidth: 28,
                      borderRadius: 0.75,
                      fontSize: 11,
                      fontWeight: 700,
                      color: "#FFFFFF",
                      bgcolor: r.color,
                      textAlign: "center",
                    }}>
                    {r.label}
                  </Box>
                  <Typography variant="body2" noWrap>
                    {r.name ?? r.label}
                  </Typography>
                </ButtonBase>
              );
            })}
          </Stack>
        </Popup>
      )}
    </>
  );
}
