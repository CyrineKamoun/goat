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
import { Box, GlobalStyles, ListItemButton, Typography } from "@mui/material";
import { useTheme } from "@mui/material/styles";
import type { FeatureCollection } from "geojson";
import type {
  ExpressionSpecification,
  FilterSpecification,
  GeoJSONSource,
  MapLayerMouseEvent,
} from "maplibre-gl";
import { useEffect, useMemo, useState, useSyncExternalStore } from "react";
import { useTranslation } from "react-i18next";
import { Layer, Popup, Source, useMap } from "react-map-gl/maplibre";

import { GEOAPI_BASE_URL } from "@/lib/constants";
import { routeMatches } from "@/lib/explorer/filters";
import { networkStore } from "@/lib/explorer/network";
import { playbackClock } from "@/lib/explorer/playback";
import { selectExplorerRoute } from "@/lib/store/explorer/slice";

import type { RouteRow } from "@/hooks/explorer/useExplorerData";
import { useAppDispatch, useAppSelector } from "@/hooks/store/ContextHooks";

import { EXPLORER_PANEL_WIDTH } from "@/components/explorer/ExplorerLeftPanel";
import { RouteBadge } from "@/components/explorer/RouteBadge";

const EMPTY: FeatureCollection = { type: "FeatureCollection", features: [] };
const NETWORK = "explorer-network";
const FADED = "#9E9E9E";
/** The Explorer's layers, bottom to top. */
const DRAW_ORDER = [
  "explorer-network-line",
  "explorer-route-casing",
  "explorer-route-line",
  "explorer-route-chain-line",
  "explorer-pattern-casing",
  "explorer-pattern-line",
  "explorer-pattern-chain-line",
  "explorer-route-stop",
  "explorer-vehicle-dot",
  "explorer-vehicle-mine",
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

/** How far (px) from the pointer a line is looked for, nearest first. */
const HIT_RADII = [2, 5, 9];

/** The shape_lines tiles name their layer this. */
const TILE_LAYER = "default";

/** A vehicle of the selected route is a badge with its number, as the
 * panels' RouteBadge: a rounded rectangle in the route's colour, edged in
 * white, that stretches to the number. One image per colour, drawn when the
 * map first asks for it. */
const BADGE = "explorer-badge-";
function drawBadge(color: string) {
  // CSS pixels, drawn at twice the resolution: the corners and the edge stay
  // put as the badge stretches between them.
  const R = 2;
  const [w, h, radius, edge] = [24 * R, 18 * R, 5 * R, 1.5 * R];
  const canvas = document.createElement("canvas");
  canvas.width = w;
  canvas.height = h;
  const ctx = canvas.getContext("2d");
  if (!ctx) return null;
  ctx.fillStyle = "#FFFFFF";
  ctx.beginPath();
  ctx.roundRect(0, 0, w, h, radius);
  ctx.fill();
  ctx.fillStyle = color;
  ctx.beginPath();
  ctx.roundRect(edge, edge, w - 2 * edge, h - 2 * edge, radius - edge);
  ctx.fill();
  return {
    image: ctx.getImageData(0, 0, w, h),
    options: {
      pixelRatio: R,
      stretchX: [[6 * R, 18 * R]] as [number, number][],
      stretchY: [[6 * R, 12 * R]] as [number, number][],
      content: [4 * R, 3 * R, 20 * R, 15 * R] as [number, number, number, number],
    },
  };
}

export function ExplorerMapLayers() {
  const { t } = useTranslation("common");
  const dispatch = useAppDispatch();
  const active = useAppSelector((s) => s.explorer.active);
  const drawing = useAppSelector((s) => (s.explorer.active ? s.explorer.drawing : null));
  const picked = useAppSelector((s) => (s.explorer.active ? s.explorer.selectedStop : null));
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

  // Playback's frames go to the map directly, not through React: many a
  // second, and nothing else on the map changes with them.
  // Paused, the map shows the network, not vehicles standing still on it.
  const pushVehicles = () => {
    const source = map?.getSource("explorer-vehicles") as GeoJSONSource | undefined;
    source?.setData(playing ? playbackClock.getSnapshot().vehicles : EMPTY);
  };
  useEffect(() => playbackClock.subscribe(pushVehicles));
  useEffect(pushVehicles);

  // Badges for the vehicles of the selected route, in its colour, drawn the
  // first time the map needs one (and again after the basemap changes).
  useEffect(() => {
    if (!map) return;
    const missing = (e: { id: string }) => {
      if (!e.id.startsWith(BADGE) || map.hasImage(e.id)) return;
      const badge = drawBadge(e.id.slice(BADGE.length));
      if (badge) map.addImage(e.id, badge.image, badge.options);
    };
    map.on("styleimagemissing", missing);
    return () => {
      map.off("styleimagemissing", missing);
    };
  }, [map]);

  // The Explorer's layers in drawing order, bottom to top. Sources mount as
  // their data arrives — the network after the route's stops — and a layer
  // added later lands on top; this puts each back in its place.
  useEffect(() => {
    if (!map || !active) return;
    const order = () => {
      const ids = (map.getStyle()?.layers ?? []).map((l) => l.id).filter((id) => DRAW_ORDER.includes(id));
      const want = DRAW_ORDER.filter((id) => ids.includes(id));
      if (ids.every((id, i) => id === want[i])) return;
      for (const id of want) map.moveLayer(id);
    };
    order();
    map.on("styledata", order);
    return () => {
      map.off("styledata", order);
    };
  }, [map, active]);

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
  // shapes under the pointer, they are offered to choose from. The lines are
  // a few pixels wide, so the pointer need only come near one: the nearest
  // lines are taken, looking further out only while none is found, so a
  // click in a dense part of the network still finds one line, not all
  // around it.
  useEffect(() => {
    if (!map || !active) return;
    const layer = "explorer-network-line";
    const linesNear = (point: { x: number; y: number }) => {
      if (!map.getLayer(layer)) return [];
      for (const r of HIT_RADII) {
        const found = map.queryRenderedFeatures(
          [
            [point.x - r, point.y - r],
            [point.x + r, point.y + r],
          ],
          { layers: [layer] }
        );
        if (found.length) return found;
      }
      return [];
    };
    const onClick = (e: MapLayerMouseEvent) => {
      const routeIds = [
        ...new Set(
          linesNear(e.point).flatMap(
            (f) => network.shapes.get(String(f.properties?.shape_id ?? f.id))?.route_ids ?? []
          )
        ),
      ];
      if (routeIds.length === 1) dispatch(selectExplorerRoute(routeIds[0]));
      else if (routeIds.length > 1) setChoice({ lon: e.lngLat.lng, lat: e.lngLat.lat, routeIds });
    };
    // Registered after the map viewer's own handler, which sets the cursor
    // for GOAT's layers on every move: over a line, this one has the last
    // word, unless a tool (measuring) holds the cursor.
    const onMove = (e: MapLayerMouseEvent) => {
      const canvas = map.getCanvas();
      if (canvas.style.cursor === "" && linesNear(e.point).length) canvas.style.cursor = "pointer";
    };
    map.on("click", onClick);
    map.on("mousemove", onMove);
    return () => {
      map.off("click", onClick);
      map.off("mousemove", onMove);
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
  const isMine: FilterSpecification = ["==", ["get", "route_id"], selectedRouteId ?? ""];
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
      {/* Playback's vehicles: dots in their route's colour; with a route
          chosen, the others go grey and its own are badges with its number. */}
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
          type="symbol"
          filter={selectedRouteId ? isMine : none}
          layout={{
            "icon-image": ["concat", BADGE, ["get", "color"]],
            "icon-text-fit": "both",
            "icon-text-fit-padding": [1, 3, 1, 3],
            "icon-allow-overlap": true,
            "icon-ignore-placement": true,
            "text-field": ["get", "label"],
            "text-size": 10,
            "text-font": ["Roboto Bold", "Roboto Regular"],
            "text-allow-overlap": true,
            "text-ignore-placement": true,
          }}
          paint={{ "text-color": ["get", "text_color"] }}
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
