import type { MapGeoJSONFeature } from "react-map-gl/maplibre";

import { rgbToHex } from "@/lib/utils/helpers";
import type {
  FeatureLayerLineProperties,
  FeatureLayerPointProperties,
  Layer,
  TextLabelSchemaData,
} from "@/lib/validations/layer";
import type { ProjectLayer } from "@/lib/validations/project";

import type { RGBColor } from "@/types/map/color";

const HIGHLIGHT_COLOR = "#FFC300";

// Remove duplicates from colorMaps. Mapbox throws an error if the steps are duplicated or not ordered
// The function will take the last colorMap value if there are duplicates
//  const colorMaps = [
//   [["3"], "#F70958"],
//   [["3"], "#F71958"],
//   [["12"], "#214CDB"],
//   [["12"], "#204CDB"],
//   [["12"], "#860A5A"]
// ];
// Output:
// [[["3"], "#F71958"], [["12"], "#860A5A"]]
export function removeColorMapsDuplicates(colorMaps) {
  const map = new Map();
  for (let i = colorMaps.length - 1; i >= 0; i--) {
    const key = colorMaps[i][0][0];
    if (!map.has(key)) {
      map.set(key, colorMaps[i][1]);
    }
  }
  const result = Array.from(map.entries()).map(([key, value]) => [[key], value]);
  return result.reverse();
}

export function getMapboxStyleColor(data: ProjectLayer | Layer, type: "color" | "stroke_color") {
  const colors = data.properties[`${type}_range`]?.colors;

  const fieldName = data.properties[`${type}_field`]?.name;
  const colorScale = data.properties[`${type}_scale`];
  const colorMaps = data.properties[`${type}_range`]?.color_map;
  const rawNoData = data.properties[`${type}_no_data`] as string | undefined;
  const noDataColor = rawNoData === "transparent" ? "rgba(0,0,0,0)" : rawNoData;
  if (colorMaps && fieldName && Array.isArray(colorMaps) && colorScale === "ordinal") {
    const valuesAndColors = [] as string[];
    const seenValues = new Set<string>();
    colorMaps.forEach((colorMap) => {
      const colorMapValue = colorMap[0];
      const colorMapHex = colorMap[1];
      if (!colorMapValue || !colorMapHex) return;
      const values = Array.isArray(colorMapValue) ? colorMapValue : [colorMapValue];
      values.forEach((value: string) => {
        if (value === null || seenValues.has(String(value))) return;
        seenValues.add(String(value));
        valuesAndColors.push(String(value));
        valuesAndColors.push(colorMapHex);
      });
    });

    // A match expression needs at least one label+output pair; fall back to default if empty.
    if (valuesAndColors.length < 2) {
      return data.properties[type] ? rgbToHex(data.properties[type] as RGBColor) : "#AAAAAA";
    }
    // Use to-string to ensure string comparison — MapLibre match only supports
    // integer numeric labels, so string matching is needed for float/decimal values.
    const matchExpr = ["match", ["to-string", ["get", fieldName]], ...valuesAndColors, "#AAAAAA"];
    if (noDataColor) {
      return ["case", ["==", ["get", fieldName], null], noDataColor, matchExpr];
    }
    return matchExpr;
  }

  if (
    (colorScale !== "custom_breaks" &&
      (!fieldName ||
        !colors ||
        data.properties[`${type}_scale_breaks`]?.breaks.length !== colors.length - 1)) ||
    (colorScale === "custom_breaks" && (!colorMaps || !fieldName))
  ) {
    return data.properties[type] ? rgbToHex(data.properties[type] as RGBColor) : "#AAAAAA";
  }

  if (colorScale === "custom_breaks" && colorMaps) {
    // Info:
    // For custom breaks value we get the color and break values from colorMap.
    // Similar to "ordinal" above but we treat them in a different way.
    const colorSteps = [] as unknown[];
    const colorMapsFiltered = removeColorMapsDuplicates(colorMaps);
    // With only 1 unique entry, we can't build a valid step expression — fall back to simple color
    if (colorMapsFiltered.length < 2) {
      return colorMapsFiltered[0]?.[1] || (data.properties[type] ? rgbToHex(data.properties[type] as RGBColor) : "#AAAAAA");
    }
    colorMapsFiltered.forEach((colorMap, index) => {
      if (index < colorMapsFiltered.length - 1) {
        colorSteps.push(colorMap[1], Number(colorMapsFiltered[index + 1]?.[0]?.[0]) || 0);
      } else if (
        index === colorMapsFiltered.length - 1 &&
        data?.properties?.[`${type}_scale_breaks`]?.max !== undefined
      ) {
        const maxValue = data?.properties?.[`${type}_scale_breaks`]?.max;
        if (maxValue && Number(colorMapsFiltered[index]?.[0]?.[0]) < maxValue) {
          colorSteps.push(colorMap[1], data.properties[`${type}_scale_breaks`]?.max, colorMap[1]);
        } else {
          colorSteps.push(colorMap[1]);
        }
      }
    });
    const config = ["step", ["get", fieldName], ...colorSteps];
    if (noDataColor) {
      return ["case", ["==", ["get", fieldName], null], noDataColor, config];
    }
    return config;
  }
  const breakValues = data.properties[`${type}_scale_breaks`];

  let _breakValues = breakValues?.breaks ? [...breakValues?.breaks] : [];
  if (_breakValues && breakValues?.max !== undefined) _breakValues.push(breakValues?.max);
  let _colors = [...colors];
  if (_breakValues) {
    const combined = _breakValues.map((value, index) => [[value], colors[index]]);
    const filtered = removeColorMapsDuplicates(combined);
    _breakValues = filtered.map((value) => value[0][0]);
    _colors = filtered.map((value) => value[1]);
  }

  // A step expression needs at least one stop (default color + stop value + color),
  // so if we have fewer than 2 colors after dedup, fall back to simple color.
  if (_colors.length < 2) {
    return _colors[0] || (data.properties[type] ? rgbToHex(data.properties[type] as RGBColor) : "#AAAAAA");
  }

  const colorSteps = _colors
    .map((color, index) => {
      if (index === _colors.length - 1 || !_breakValues) {
        return [_colors[index]];
      } else {
        return [color, _breakValues[index] || 0];
      }
    })
    .flat();
  const config = ["step", ["get", fieldName], ...colorSteps];
  if (noDataColor) {
    return ["case", ["==", ["get", fieldName], null], noDataColor, config];
  }
  return config;
}

export function getMapboxStyleMarker(data: ProjectLayer | Layer) {
  const properties = data.properties as FeatureLayerPointProperties;
  const markerMaps = properties.marker_mapping;
  const fieldName = properties.marker_field?.name;
  const marker = `${data.id}-${properties.marker?.name}`;
  if (markerMaps && fieldName) {
    const valuesAndIcons = [] as string[];
    const seenValues = new Set<string>();
    markerMaps.forEach((markerMap) => {
      const markerMapValue = markerMap[0];
      const markerMapIcon = markerMap[1];
      if (!markerMapValue || !markerMapIcon) return;
      const values = Array.isArray(markerMapValue) ? markerMapValue : [markerMapValue];
      values.forEach((value: string) => {
        if (value === null || seenValues.has(String(value))) return;
        seenValues.add(String(value));
        valuesAndIcons.push(String(value));
        valuesAndIcons.push(`${data.id}-${markerMapIcon.name}`);
      });
    });

    return ["match", ["to-string", ["get", fieldName]], ...valuesAndIcons, marker];
  }

  return marker;
}

export function transformToMapboxLayerStyleSpec(data: ProjectLayer | Layer) {
  const type = data.feature_layer_geometry_type;
  if (type === "point") {
    const pointProperties = data.properties as FeatureLayerPointProperties;
    return {
      type: "circle",
      layout: {
        visibility: data.properties.visibility ? "visible" : "none",
      },
      paint: {
        "circle-color": getMapboxStyleColor(data, "color"),
        "circle-opacity": pointProperties.filled ? pointProperties.opacity : 0,
        "circle-radius": pointProperties.radius || 5,
        "circle-stroke-color": getMapboxStyleColor(data, "stroke_color"),
        "circle-stroke-width": pointProperties.stroked ? pointProperties.stroke_width || 1 : 0,
      },
    };
  } else if (type === "polygon") {
    const polygonProperties = data.properties as FeatureLayerLineProperties;
    return {
      type: "fill",
      layout: {
        visibility: data.properties.visibility ? "visible" : "none",
      },
      paint: {
        "fill-color": getMapboxStyleColor(data, "color"),
        "fill-opacity": polygonProperties.filled ? polygonProperties.opacity : 0,
        "fill-outline-color": getMapboxStyleColor(data, "stroke_color"),
        "fill-antialias": false,
      },
    };
  } else if (type === "line") {
    const lineProperties = data.properties as FeatureLayerLineProperties;

    return {
      type: "line",
      layout: {
        visibility: data.properties.visibility ? "visible" : "none",
      },
      paint: {
        "line-color": getMapboxStyleColor(data, "stroke_color"),
        "line-opacity": lineProperties.opacity,
        "line-width": lineProperties.stroke_width || 1,
      },
    };
  } else {
    throw new Error(`Invalid type: ${type}`);
  }
}
/** Used for both text labels and custom markers */
export function getSymbolStyleSpec(data: TextLabelSchemaData | undefined, layer: ProjectLayer | Layer) {
  const iconLayout = {};
  const iconPaint = {};
  const textLayout = {};
  const textPaint = {};
  if (layer.properties["custom_marker"]) {
    const pointProperties = layer.properties as FeatureLayerPointProperties;
    const markerSize = pointProperties.marker_size ?? 100;
    iconLayout["icon-image"] = getMapboxStyleMarker(layer);
    iconLayout["icon-size"] = markerSize / 200;
    iconLayout["icon-allow-overlap"] = pointProperties.marker_allow_overlap || false;
    iconLayout["icon-anchor"] = pointProperties.marker_anchor || "center";
    iconLayout["icon-offset"] = pointProperties.marker_offset || [0, 0];
    iconPaint["icon-opacity"] = pointProperties.filled ? pointProperties.opacity : 1;
    iconPaint["icon-color"] = pointProperties.filled ? getMapboxStyleColor(layer, "color") : "#000000";
  }
  if (data?.field) {
    textLayout["text-field"] = ["get", data.field];
    textLayout["text-size"] = data.size ?? 14;
    // TODO: FIND A BETTER SOLUTION FOR THIS. THE GERMAN BASEMAPS ONLY SUPPORT ROBOTO REGULAR
    textLayout["text-font"] = ["Roboto Regular"];
    textLayout["text-allow-overlap"] = data.allow_overlap || false;
    textLayout["text-anchor"] = data.anchor || "top";
    textLayout["text-offset"] = data.offset || [0, 0];
    textPaint["text-color"] = data.color ? rgbToHex(data.color as RGBColor) : "#000000";
    textPaint["text-halo-color"] = data.outline_color ? rgbToHex(data.outline_color as RGBColor) : "#FFFFFF";
    textPaint["text-halo-width"] = data.outline_width ?? 1;
  }

  return {
    type: "symbol",
    layout: {
      "symbol-placement": layer?.feature_layer_geometry_type === "line" ? "line" : "point",
      visibility: layer?.properties?.visibility ? "visible" : "none",
      ...textLayout,
      ...iconLayout,
    },
    paint: {
      ...textPaint,
      ...iconPaint,
    },
  };
}

export function getHightlightStyleSpec(highlightFeature: MapGeoJSONFeature) {
  if (!highlightFeature) return null;

  const layerType = highlightFeature.layer?.type;
  let type;
  let paint;
  switch (layerType) {
    case "symbol":
    case "circle":
      type = "circle";
      const strokeWidth = highlightFeature.layer.paint?.["circle-stroke-width"] ?? 0;
      let radius;
      if (highlightFeature.layer.type === "symbol") {
        radius = 5;
      } else {
        radius =
          ((highlightFeature.layer.paint?.["circle-radius"] as number) < 8
            ? 8
            : highlightFeature.layer.paint?.["circle-radius"]) + strokeWidth;
      }

      paint = {
        "circle-color": HIGHLIGHT_COLOR,
        "circle-opacity": 0.8,
        "circle-radius": radius,
      };
      break;
    case "fill":
    case "line":
      type = "line";
      paint = {
        "line-color": HIGHLIGHT_COLOR,
        "line-width": highlightFeature.layer.paint?.["line-width"] ?? 2,
      };
      break;
    default:
      return null;
  }

  // Use MVT feature ID (rowid+1) when available, fall back to properties.id for legacy tiles
  const mvtId = highlightFeature.id;
  const propsId = highlightFeature.properties?.id;
  const filter = mvtId != null
    ? ["==", ["id"], mvtId]
    : propsId != null
      ? ["any", ["==", ["get", "id"], propsId], ["==", ["to-string", ["get", "id"]], String(propsId)]]
      : undefined;
  return {
    type,
    paint,
    ...(filter && { filter }),
  };
}

export const scenarioFeatureStateColor = [
  "match",
  ["get", "edit_type"],
  "n",
  "#007DC7",
  "m",
  "#FFC300",
  "d",
  "#C70039",
  "#000202",
];
export function scenarioLayerStyleSpec(data: ProjectLayer | Layer) {
  const geometryType = data.feature_layer_geometry_type;

  let style;
  if (geometryType === "point") {
    if (data.properties["custom_marker"]) {
      const markerSize = (data.properties as FeatureLayerPointProperties).marker_size ?? 100;
      style = {
        type: "symbol",
        layout: {
          "icon-image": getMapboxStyleMarker(data),
          "icon-allow-overlap": true,
          "icon-size": markerSize / 600,
        },
        paint: {
          "icon-opacity": 1,
        },
      };
    } else {
      const circleRadius = data.properties["radius"] || 20;
      style = {
        type: "circle",
        paint: {
          "circle-opacity": 1,
          "circle-blur": 0.2,
          "circle-radius": circleRadius,
          "circle-stroke-width": 2,
        },
      };
    }
  } else if (geometryType === "line") {
    const width = data.properties["stroke_width"] || 2;
    style = {
      type: "line",
      paint: {
        "line-blur": 1,
        "line-color": scenarioFeatureStateColor,
        "line-width": width,
        "line-dasharray": [3, 1],
      },
    };
  } else if (geometryType === "polygon") {
    style = {
      type: "fill",
      paint: {
        "fill-opacity": 0,
        "fill-color": scenarioFeatureStateColor,
        "fill-outline-color": scenarioFeatureStateColor,
      },
    };
  }

  if (style) {
    style.layout = {
      ...(style.layout || {}),
      visibility: data.properties.visibility ? "visible" : "none",
    };
  }

  return style;
}
