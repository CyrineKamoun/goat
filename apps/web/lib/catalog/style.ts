import { transformToMapboxLayerStyleSpec } from "@/lib/transformers/layer";
import type { Layer } from "@/lib/validations/layer";

/** A catalog dataset's own rendering, as MapLibre paint. */

/** The published style object: a layer's `properties`, before it is a layer. */
export type CatalogStyle = Layer["properties"];

export type CatalogPaint = {
  type: string;
  layout?: Record<string, unknown>;
  paint: Record<string, unknown>;
};

/**
 * `undefined` when the style cannot be drawn — no geometry type, a type this
 * renderer has no shape for, or a malformed style. Callers fall back to their
 * plain footprint colour, so a bad published style must never throw.
 */
export const catalogPaint = (
  style: CatalogStyle | undefined,
  geometryType: string | undefined
): CatalogPaint | undefined => {
  if (!style || !geometryType) return undefined;
  if (geometryType !== "point" && geometryType !== "line" && geometryType !== "polygon") {
    return undefined;
  }
  try {
    // The transformer reads only `properties` and `feature_layer_geometry_type`,
    // so a catalog style needs no layer — hence the cast rather than a fake one.
    return transformToMapboxLayerStyleSpec({
      properties: style,
      feature_layer_geometry_type: geometryType,
    } as unknown as Layer) as CatalogPaint;
  } catch {
    return undefined;
  }
};
