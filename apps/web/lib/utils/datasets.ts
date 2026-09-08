import { getLegendColorMap, getLegendMarkerMap } from "@/lib/utils/map/legend";
import type { Layer } from "@/lib/validations/layer";

/** Whether the layer's style produces a legend, decided from the same maps
 * `LayerLegendPanel` draws one from: it renders a section only where a map has
 * more than one entry, and nothing at all for a raster without a style. */
export const hasLegend = (dataset: Layer): boolean => {
  const properties = dataset.properties as Record<string, unknown> | undefined;
  if (!properties) return false;
  if (dataset.type === "raster") return !!properties.style;
  return (
    getLegendColorMap(properties, "color").length > 1 ||
    getLegendColorMap(properties, "stroke_color").length > 1 ||
    getLegendMarkerMap(properties).length > 1
  );
};
