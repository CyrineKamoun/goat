/**
 * @p4b/draw - Enhanced polygon mode
 *
 * Extends the default draw_polygon mode to show ALL vertices while drawing,
 * not just start/end. Also handles display of circle and great-circle features.
 */
import MapboxDraw from "@mapbox/mapbox-gl-draw";
import { circle } from "@turf/circle";
import { distance } from "@turf/distance";
import { point } from "@turf/helpers";

import { DrawHistory } from "./draw-history";
import { GREAT_CIRCLE_PROPERTY, generateGreatCirclePath } from "./great-circle";

const Constants = MapboxDraw.constants;
const DrawPolygon = MapboxDraw.modes.draw_polygon;

const USER_GREAT_CIRCLE_PROPERTY = `user_${GREAT_CIRCLE_PROPERTY}`;

function createVertex(parentId: string, coordinates: [number, number], path: string, selected: boolean) {
  return {
    type: Constants.geojsonTypes.FEATURE,
    properties: {
      meta: Constants.meta.VERTEX,
      parent: parentId,
      coord_path: path,
      active: selected ? Constants.activeStates.ACTIVE : Constants.activeStates.INACTIVE,
    },
    geometry: {
      type: Constants.geojsonTypes.POINT,
      coordinates,
    },
  };
}

function regenerateCircleFromLine(lineFeature: GeoJSON.Feature): GeoJSON.Feature | null {
  if (lineFeature.geometry.type !== "LineString") return null;
  const coords = (lineFeature.geometry as GeoJSON.LineString).coordinates;
  if (coords.length < 2) return null;

  const center = point(coords[0] as [number, number]);
  const edge = point(coords[1] as [number, number]);
  const radius = distance(center, edge, { units: "kilometers" });
  const featureId = lineFeature.properties?.id || lineFeature.id;

  return circle(center, radius, {
    steps: 64,
    units: "kilometers",
    properties: {
      meta: "feature",
      parent: featureId,
      parentRadiusLine: featureId,
      id: `${featureId}-circle-display`,
      active: lineFeature.properties?.active,
      isCircle: true,
      isDisplayOnly: true,
    },
  });
}

function transformGreatCircleIfNeeded(
  geojson: GeoJSON.Feature & { properties: Record<string, unknown> },
  display: (feature: GeoJSON.Feature) => void
) {
  const isGreatCircle =
    geojson.properties &&
    (geojson.properties[GREAT_CIRCLE_PROPERTY] || geojson.properties[USER_GREAT_CIRCLE_PROPERTY]);

  if (
    isGreatCircle &&
    geojson.geometry.type === "LineString" &&
    (geojson.geometry as GeoJSON.LineString).coordinates.length >= 2
  ) {
    const originalCoords = (geojson.geometry as GeoJSON.LineString).coordinates as [number, number][];
    const greatCircleCoords = generateGreatCirclePath(originalCoords);
    return display({
      ...geojson,
      geometry: { ...geojson.geometry, coordinates: greatCircleCoords },
    } as GeoJSON.Feature);
  }

  return display(geojson);
}

const PolygonMode: typeof DrawPolygon = { ...DrawPolygon };

// eslint-disable-next-line @typescript-eslint/no-explicit-any
(PolygonMode as any).onSetup = function (this: any, opts: Record<string, unknown>) {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const state = (DrawPolygon as any).onSetup.call(this, opts);
  state._drawHistory = new DrawHistory();
  state._drawHistory.setVertexCallbacks(
    () => {
      try {
        const ring = state.polygon.coordinates[0];
        if (!ring || ring.length < 3) return;
        if (ring.length === 3) {
          state.polygon.setCoordinates([[]]);
          state.currentVertexPosition = 0;
          return;
        }
        state.polygon.removeCoordinate(`0.${ring.length - 2}`);
        state.currentVertexPosition--;
      } catch { /* ignore */ }
    },
    (coord: [number, number]) => {
      try {
        const ring = state.polygon.coordinates[0];
        if (!ring) return;
        state.polygon.addCoordinate(`0.${ring.length - 1}`, coord[0], coord[1]);
        state.currentVertexPosition++;
      } catch { /* ignore */ }
    }
  );
  state._drawHistory.activate();
  return state;
};

// eslint-disable-next-line @typescript-eslint/no-explicit-any
(PolygonMode as any).clickAnywhere = function (this: any, state: any, e: any) {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  (DrawPolygon as any).clickAnywhere.call(this, state, e);
  const ring = state.polygon.coordinates[0];
  if (ring && ring.length >= 2) {
    const v = ring[ring.length - 2];
    if (v) state._drawHistory.pushVertex([v[0], v[1]] as [number, number]);
  }
};

// eslint-disable-next-line @typescript-eslint/no-explicit-any
PolygonMode.onStop = function (this: any, state: any) {
  if (state._drawHistory) {
    state._drawHistory.deactivate();
    state._drawHistory.clear();
  }
  if (DrawPolygon.onStop) DrawPolygon.onStop.call(this, state);
};

PolygonMode.toDisplayFeatures = function (
  state: { polygon: { id: string } },
  geojson: GeoJSON.Feature & { properties: Record<string, unknown> },
  display: (feature: GeoJSON.Feature) => void
) {
  const isActivePolygon = geojson.properties.id === state.polygon.id;
  geojson.properties.active = isActivePolygon
    ? Constants.activeStates.ACTIVE
    : Constants.activeStates.INACTIVE;

  if (!isActivePolygon) {
    const isRadiusLine = geojson.properties?.isRadiusLine || geojson.properties?.user_isRadiusLine;
    const isCircle = geojson.properties?.isCircle || geojson.properties?.user_isCircle;
    if (
      (isRadiusLine || isCircle) &&
      geojson.geometry.type === "LineString" &&
      (geojson.geometry as GeoJSON.LineString).coordinates.length === 2
    ) {
      display(geojson);
      const circlePolygon = regenerateCircleFromLine(geojson);
      if (circlePolygon) display(circlePolygon);
      return;
    }

    if (geojson.geometry.type === "LineString") {
      return transformGreatCircleIfNeeded(geojson, display);
    }
    return display(geojson);
  }

  if (geojson.geometry.type !== "Polygon") {
    return display(geojson);
  }

  if (geojson.geometry.coordinates.length === 0) return;

  const coords = geojson.geometry.coordinates[0];
  if (!coords) return;
  const coordinateCount = coords.length;

  if (coordinateCount < 3) return;

  geojson.properties.meta = Constants.meta.FEATURE;

  for (let i = 0; i < coordinateCount - 2; i++) {
    const isLastVertex = i === coordinateCount - 3;
    display(
      createVertex(state.polygon.id, coords[i] as [number, number], `0.${i}`, isLastVertex) as GeoJSON.Feature
    );
  }

  if (coordinateCount <= 4 && coords[0] && coords[1]) {
    const lineCoordinates = [
      [coords[0][0], coords[0][1]],
      [coords[1][0], coords[1][1]],
    ];
    display({
      type: Constants.geojsonTypes.FEATURE as "Feature",
      properties: geojson.properties,
      geometry: {
        coordinates: lineCoordinates,
        type: Constants.geojsonTypes.LINE_STRING as "LineString",
      },
    });

    if (coordinateCount === 3) return;
  }

  display(geojson);
};

export default PolygonMode;
