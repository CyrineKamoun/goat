/**
 * The Explorer on the map, drawn by itself whatever GOAT's layers beneath
 * show: the bundle's whole network in its routes' colours (shape_lines
 * tiles, each shape styled from the snapshot), the selected route over it
 * with its stops, the others faded; the network's stops from street level;
 * playback's vehicles (a WebGL layer of their own, vehicleLayer.ts); the
 * stop and the trip the right panel shows.
 * Clicking a vehicle opens its trip, a stop its timetable, a line its route.
 *
 * Rendered inside <Map>, outside the Explorer's provider: it reads what the
 * panels put in the store, and the network and playback stores. Sources are
 * always mounted, in drawing order; layer ids start with `explorer-`, which
 * the layer restack keeps above the data (Layers.tsx).
 */
import { Box, GlobalStyles, ListItemButton, Typography } from "@mui/material";
import { useTheme } from "@mui/material/styles";
import type { FeatureCollection, Point, Polygon } from "geojson";
import type { ExpressionSpecification, FilterSpecification, MapLayerMouseEvent } from "maplibre-gl";
import { useEffect, useMemo, useRef, useState, useSyncExternalStore } from "react";
import { useTranslation } from "react-i18next";
import { Layer, Popup, Source, useMap } from "react-map-gl/maplibre";

import { GEOAPI_BASE_URL } from "@/lib/constants";
import { routeMatches } from "@/lib/explorer/filters";
import { networkStore } from "@/lib/explorer/network";
import { playbackClock } from "@/lib/explorer/playback";
import {
  closeExplorerPanel,
  openExplorerPanel,
  panelItem,
  pickedStop,
  selectExplorerRoute,
} from "@/lib/store/explorer/slice";

import type { RouteRow } from "@/hooks/explorer/useExplorerData";
import { useAppDispatch, useAppSelector } from "@/hooks/store/ContextHooks";

import { EXPLORER_PANEL_WIDTH } from "@/components/explorer/ExplorerLeftPanel";
import { RouteBadge } from "@/components/explorer/RouteBadge";
import { STOP_FILL } from "@/components/explorer/StopMarker";
import { EXPLORER_RIGHT_PANEL_WIDTH } from "@/components/explorer/right/panelSize";
import { VEHICLE_LAYER_ID, VehicleLayer } from "@/components/explorer/vehicleLayer";

const EMPTY: FeatureCollection = { type: "FeatureCollection", features: [] };
const NETWORK = "explorer-network";
const FADED = "#9E9E9E";
/** The Explorer's layers, bottom to top. */
const DRAW_ORDER = [
  "explorer-station-area",
  "explorer-station-edge",
  "explorer-network-line",
  "explorer-stop-dot",
  "explorer-route-casing",
  "explorer-route-line",
  "explorer-route-chain-line",
  "explorer-pattern-casing",
  "explorer-pattern-line",
  "explorer-pattern-chain-line",
  "explorer-route-stop",
  VEHICLE_LAYER_ID,
  "explorer-picked-stop-halo",
  "explorer-picked-stop-dot",
];

/** The line chooser's class, and its look: the panels' card. */
const LINE_CHOICE = "explorer-line-choice";
const LINE_CHOICE_SX = {
  [`.${LINE_CHOICE} .maplibregl-popup-content`]: {
    padding: 0,
    borderRadius: "12px",
    boxShadow: "rgba(0, 0, 0, 0.2) 0px 0px 10px",
    overflow: "hidden",
  },
  [`.${LINE_CHOICE} .maplibregl-popup-tip`]: { display: "none" },
};

/** What a click opens, first found first: a vehicle, a stop, a line under
 * the pointer, the station it is in, a line near it. */
const STOP_LAYERS = ["explorer-route-stop", "explorer-stop-dot"];
const LINE_LAYERS = ["explorer-network-line"];
const STATION_LAYERS = ["explorer-station-area"];

/** How far (px) from the pointer a feature is looked for, nearest first. */
const HIT_RADII = [2, 5, 9];

/** The shape_lines tiles name their layer this. */
const TILE_LAYER = "default";

export function ExplorerMapLayers() {
  const { t } = useTranslation("common");
  const dispatch = useAppDispatch();
  const active = useAppSelector((s) => s.explorer.active);
  const drawing = useAppSelector((s) => (s.explorer.active ? s.explorer.drawing : null));
  const picked = useAppSelector((s) => (s.explorer.active ? pickedStop(s.explorer) : null));
  const openTrip = useAppSelector((s) => {
    const item = s.explorer.active ? panelItem(s.explorer) : null;
    return item?.kind === "trip" ? item : null;
  });
  const panelOpen = useAppSelector(
    (s) => s.explorer.active && s.explorer.panel.stack.length > 0 && !s.explorer.panel.collapsed
  );
  const selectedRouteId = useAppSelector((s) => (s.explorer.active ? s.explorer.selectedRouteId : null));
  const filters = useAppSelector((s) => s.explorer.filters);
  const playing = useAppSelector((s) => s.explorer.playing);
  // The Stops tab's pattern, highlighted over the rest of its route.
  const patternIdx = useAppSelector((s) =>
    s.explorer.routeTab === "stops" ? (s.explorer.pattern?.pattern_idx ?? null) : null
  );
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
  // What a click on nothing lets go of, as it is when the click lands.
  const panelShown = useAppSelector((s) => s.explorer.active && s.explorer.panel.stack.length > 0);
  const open = useRef({ choice: false, panel: false, route: false });
  open.current = { choice: choice !== null, panel: panelShown, route: selectedRouteId !== null };

  // Playback's vehicles, drawn by a layer of their own (vehicleLayer.ts).
  // Frames reach it directly, not through React: many a second, and nothing
  // else on the map changes with them. Paused, the map shows the network,
  // not vehicles standing still on it.
  const vehicles = useMemo(() => new VehicleLayer(), []);
  const pushVehicles = () => {
    const frame = playbackClock.getSnapshot();
    vehicles.setFrame(playing ? frame.vehicles : [], frame.styles);
  };
  useEffect(() => playbackClock.subscribe(pushVehicles));
  useEffect(pushVehicles);
  useEffect(() => {
    vehicles.setLook({
      selectedRouteId,
      openTrip: openTrip ? { trip_idx: openTrip.trip_idx, run_dep_s: openTrip.run_dep_s } : null,
      ring: theme.palette.primary.main,
      faded: FADED,
    });
  }, [vehicles, selectedRouteId, openTrip, theme.palette.primary.main]);

  // The Explorer's layers in drawing order, bottom to top. Sources mount as
  // their data arrives — the network after the route's stops — and a layer
  // added later lands on top; this puts each back in its place. The
  // vehicles' layer is added here, and again whenever a new basemap style
  // drops it.
  useEffect(() => {
    if (!map || !active) return;
    const raw = map.getMap();
    const order = () => {
      if (!raw.getLayer(VEHICLE_LAYER_ID)) {
        try {
          raw.addLayer(vehicles);
        } catch {
          return; // the style is still loading: the next styledata adds it
        }
      }
      const ids = raw.getLayersOrder().filter((id) => DRAW_ORDER.includes(id));
      const want = DRAW_ORDER.filter((id) => ids.includes(id));
      if (ids.every((id, i) => id === want[i])) return;
      for (const id of want) raw.moveLayer(id);
    };
    order();
    raw.on("styledata", order);
    return () => {
      raw.off("styledata", order);
      if (raw.getLayer(VEHICLE_LAYER_ID)) raw.removeLayer(VEHICLE_LAYER_ID);
    };
  }, [map, active, vehicles]);

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

  // A click opens what it lands on: a vehicle its trip, a stop its
  // timetable, a line its route — where several routes share the shapes
  // under the pointer, they are offered to choose from. Lines, dots and
  // vehicles are a few pixels across, so the pointer need only come near
  // one: the nearest are taken, looking further out only while none is
  // found, so a click in a dense part of the network still finds one thing,
  // not all around it.
  useEffect(() => {
    if (!map || !active) return;
    const near = (point: { x: number; y: number }, layers: string[], radii = HIT_RADII) => {
      const present = layers.filter((l) => map.getLayer(l));
      if (!present.length) return [];
      for (const r of radii) {
        const found = map.queryRenderedFeatures(
          [
            [point.x - r, point.y - r],
            [point.x + r, point.y + r],
          ],
          { layers: present }
        );
        if (found.length) return found;
      }
      return [];
    };
    const vehicleAt = (point: { x: number; y: number }) => {
      for (const r of HIT_RADII) {
        const found = vehicles.hit(point, r);
        if (found) return found;
      }
      return null;
    };
    const onClick = (e: MapLayerMouseEvent) => {
      const vehicle = vehicleAt(e.point);
      if (vehicle) {
        dispatch(
          openExplorerPanel({
            item: { kind: "trip", trip_idx: vehicle.trip_idx, run_dep_s: vehicle.run_dep_s },
          })
        );
        return;
      }
      const [stop] = near(e.point, STOP_LAYERS);
      if (stop) {
        const [lon, lat] = (stop.geometry as Point).coordinates;
        const p = stop.properties ?? {};
        dispatch(
          openExplorerPanel({
            item: {
              kind: "stop",
              stop: { stop_idx: Number(p.stop_idx), name: String(p.name ?? ""), lon, lat },
            },
            // A stop of the selected route goes when the route does.
            fromRoute: stop.layer.id === "explorer-route-stop" ? selectedRouteId : null,
          })
        );
        return;
      }
      // A station's area covers the lines through it: a line right under
      // the pointer wins, the station takes the rest of its area.
      const routesOf = (radii: number[]) => [
        ...new Set(
          near(e.point, LINE_LAYERS, radii).flatMap(
            (f) => network.shapes.get(String(f.properties?.shape_id ?? f.id))?.route_ids ?? []
          )
        ),
      ];
      let routeIds = routesOf(HIT_RADII.slice(0, 1));
      if (!routeIds.length) {
        const [station] = near(e.point, STATION_LAYERS, [0]);
        if (station) {
          const p = station.properties ?? {};
          const ring = (station.geometry as Polygon).coordinates[0] ?? [];
          const mid = (k: 0 | 1) => ring.reduce((sum, c) => sum + c[k], 0) / Math.max(ring.length, 1);
          dispatch(
            openExplorerPanel({
              item: {
                kind: "stop",
                stop: { stop_idx: Number(p.stop_idx), name: String(p.name ?? ""), lon: mid(0), lat: mid(1) },
              },
            })
          );
          return;
        }
        routeIds = routesOf(HIT_RADII);
      }
      if (routeIds.length === 1) dispatch(selectExplorerRoute(routeIds[0]));
      else if (routeIds.length > 1) setChoice({ lon: e.lngLat.lng, lat: e.lngLat.lat, routeIds });
      else dismiss();
    };
    // A click on nothing lets go of what is open, the latest first: the line
    // chooser, then the right panel's stop or trip, then the selected route.
    // A tool holding the cursor (measuring) owns its clicks.
    const dismiss = () => {
      const cursor = map.getCanvas().style.cursor;
      if (cursor !== "" && cursor !== "pointer") return;
      if (open.current.choice) return; // the popup closes itself
      if (open.current.panel) dispatch(closeExplorerPanel());
      else if (open.current.route) dispatch(selectExplorerRoute(null));
    };
    // Registered after the map viewer's own handler, which sets the cursor
    // for GOAT's layers on every move: over anything that opens, this one
    // has the last word, unless a tool (measuring) holds the cursor.
    const all = [...STOP_LAYERS, ...STATION_LAYERS, ...LINE_LAYERS];
    const onMove = (e: MapLayerMouseEvent) => {
      const canvas = map.getCanvas();
      if (canvas.style.cursor === "" && (vehicleAt(e.point) || near(e.point, all).length))
        canvas.style.cursor = "pointer";
    };
    map.on("click", onClick);
    map.on("mousemove", onMove);
    return () => {
      map.off("click", onClick);
      map.off("mousemove", onMove);
    };
  }, [map, active, network, selectedRouteId, dispatch, vehicles]);

  const pickedPoint = useMemo<FeatureCollection>(
    () =>
      picked && picked.lon !== null && picked.lat !== null
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

  // Fly to a picked stop, clear of the panels; zoom in only as far as a
  // stop needs, never out from a closer look. A stop already in plain view
  // at street level (clicked on the map) is left where it is.
  useEffect(() => {
    if (!map || !picked || picked.lon === null || picked.lat === null) return;
    const right = panelOpen ? EXPLORER_RIGHT_PANEL_WIDTH : 0;
    const { x, y } = map.project([picked.lon, picked.lat]);
    const { clientWidth: w, clientHeight: h } = map.getCanvas();
    const inView = x > EXPLORER_PANEL_WIDTH + 40 && x < w - right - 40 && y > 60 && y < h - 120;
    if (inView && map.getZoom() >= 14) return;
    map.flyTo({
      center: [picked.lon, picked.lat],
      zoom: Math.max(map.getZoom(), 15),
      padding: { top: 0, bottom: 0, left: EXPLORER_PANEL_WIDTH, right },
    });
    // Only when another stop is picked.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [map, picked]);

  const stops = useMemo<FeatureCollection>(
    () => ({
      type: "FeatureCollection",
      features: (drawing?.stops ?? []).map((s) => ({
        type: "Feature",
        geometry: { type: "Point", coordinates: [s.lon, s.lat] },
        properties: { stop_idx: s.stop_idx, name: s.name },
      })),
    }),
    [drawing?.stops]
  );
  const chains = useMemo<FeatureCollection>(
    () => ({
      type: "FeatureCollection",
      features: Object.entries(drawing?.patternChains ?? {})
        .filter(([, c]) => c.length > 1)
        .map(([pattern_idx, coordinates]) => ({
          type: "Feature",
          geometry: { type: "LineString", coordinates },
          properties: { pattern_idx: Number(pattern_idx) },
        })),
    }),
    [drawing?.patternChains]
  );

  // Fit the view to the network on opening, clear of the left panel and the
  // bottom bar.
  const bounds = useAppSelector((s) => (s.explorer.active ? s.explorer.bounds : null));
  useEffect(() => {
    if (!map || !bounds) return;
    map.fitBounds(
      [
        [bounds[0], bounds[1]],
        [bounds[2], bounds[3]],
      ],
      { padding: { top: 60, bottom: 100, left: EXPLORER_PANEL_WIDTH + 40, right: 80 }, maxZoom: 14 }
    );
  }, [map, bounds]);

  // Fit the view to the Stops tab's pattern when it switches to another,
  // clear of the panels and the bottom bar.
  useEffect(() => {
    const b = patternIdx !== null ? drawing?.patternBounds?.[patternIdx] : undefined;
    if (!map || !b) return;
    map.fitBounds(
      [
        [b[0], b[1]],
        [b[2], b[3]],
      ],
      {
        padding: {
          top: 80,
          bottom: 120,
          left: EXPLORER_PANEL_WIDTH + 60,
          right: (panelOpen ? EXPLORER_RIGHT_PANEL_WIDTH : 0) + 80,
        },
        maxZoom: 15,
      }
    );
    // Only when another pattern is chosen.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [map, patternIdx, drawing?.routeId]);

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
  // With a pattern picked, the rest of the route steps back for it.
  const highlighting = patternIdx !== null && !!drawing;
  const patternShapes: FilterSpecification = [
    "in",
    ["get", "shape_id"],
    ["literal", (patternIdx !== null && drawing?.patternShapes[patternIdx]) || []],
  ];
  const isPatternChain: FilterSpecification = ["==", ["get", "pattern_idx"], patternIdx ?? -1];
  const routeOpacity = highlighting ? 0.15 : 1;
  const none: FilterSpecification = ["boolean", false];
  const faded: ExpressionSpecification = ["boolean", ["feature-state", "faded"], false];

  return (
    <>
      <GlobalStyles styles={LINE_CHOICE_SX} />
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
            paint={{ "line-color": color, "line-width": 4.5, "line-opacity": routeOpacity }}
          />
          <Layer
            id="explorer-pattern-casing"
            type="line"
            source-layer={TILE_LAYER}
            filter={highlighting ? patternShapes : none}
            layout={{ "line-cap": "round", "line-join": "round" }}
            paint={{ "line-color": "#FFFFFF", "line-width": 12 }}
          />
          <Layer
            id="explorer-pattern-line"
            type="line"
            source-layer={TILE_LAYER}
            filter={highlighting ? patternShapes : none}
            layout={{ "line-cap": "round", "line-join": "round" }}
            paint={{ "line-color": color, "line-width": 7 }}
          />
        </Source>
      )}
      {/* Each station as a faint area round its stops, under the lines:
          clicked, the timetable of all its stops. */}
      <Source key="explorer-stations" id="explorer-stations" type="geojson" data={network.stations}>
        <Layer
          id="explorer-station-area"
          type="fill"
          minzoom={14}
          paint={{
            "fill-color": STOP_FILL,
            "fill-opacity": ["interpolate", ["linear"], ["zoom"], 14, 0, 15, selectedRouteId ? 0.1 : 0.16],
          }}
        />
        <Layer
          id="explorer-station-edge"
          type="line"
          minzoom={14}
          paint={{
            "line-color": STOP_FILL,
            "line-width": 1,
            "line-opacity": ["interpolate", ["linear"], ["zoom"], 14, 0, 15, selectedRouteId ? 0.3 : 0.55],
          }}
        />
      </Source>
      {/* The network's stops from street level, to click for a timetable:
          the stops layer's grey dot in a white ring (StopMarker). */}
      <Source key="explorer-stops" id="explorer-stops" type="geojson" data={network.stops}>
        <Layer
          id="explorer-stop-dot"
          type="circle"
          minzoom={13}
          paint={{
            "circle-radius": ["interpolate", ["linear"], ["zoom"], 13, 2, 16, 4],
            "circle-color": STOP_FILL,
            "circle-stroke-color": "#FFFFFF",
            "circle-stroke-width": ["interpolate", ["linear"], ["zoom"], 13, 1, 16, 2],
            "circle-opacity": selectedRouteId ? 0.6 : 1,
            "circle-stroke-opacity": selectedRouteId ? 0.6 : 1,
          }}
        />
      </Source>
      <Source key="explorer-route-chains" id="explorer-route-chains" type="geojson" data={chains}>
        <Layer
          id="explorer-route-chain-line"
          type="line"
          paint={{
            "line-color": color,
            "line-width": 3,
            "line-dasharray": [2, 1.5],
            "line-opacity": routeOpacity,
          }}
        />
        <Layer
          id="explorer-pattern-chain-line"
          type="line"
          filter={highlighting ? isPatternChain : none}
          paint={{ "line-color": color, "line-width": 5, "line-dasharray": [2, 1.5] }}
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
        // The panels' card, not MapLibre's bubble (styles in LINE_CHOICE_SX).
        <Popup
          longitude={choice.lon}
          latitude={choice.lat}
          closeOnClick
          closeButton={false}
          offset={10}
          className={LINE_CHOICE}
          onClose={() => setChoice(null)}
          maxWidth="280px">
          <Box sx={{ py: 1, minWidth: 220 }}>
            <Typography
              variant="caption"
              color="text.secondary"
              fontWeight={600}
              display="block"
              sx={{ px: 1.5, pb: 0.5 }}>
              {t("explorer_lines_here", { count: choice.routeIds.length })}
            </Typography>
            {choice.routeIds.map((id) => {
              const r = network.routes.get(id);
              if (!r) return null;
              return (
                <ListItemButton
                  key={id}
                  dense
                  onClick={() => {
                    dispatch(selectExplorerRoute(id));
                    setChoice(null);
                  }}
                  sx={{ px: 1.5, gap: 1.25, minHeight: 34 }}>
                  <RouteBadge route={r as unknown as RouteRow} small />
                  <Typography variant="body2" noWrap>
                    {r.long_name || r.short_name || r.route_id}
                  </Typography>
                </ListItemButton>
              );
            })}
          </Box>
        </Popup>
      )}
    </>
  );
}
