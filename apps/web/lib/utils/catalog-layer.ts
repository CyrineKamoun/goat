/**
 * What marks a layer as a promoted catalog layer, in one place.
 *
 * Promote stamps two things into `other_properties`: the full item snapshot
 * (`catalog_item`) and the materialize lifecycle (`catalog_materialize`).
 * The backend's own identity columns are not part of the layer read schema,
 * so the snapshot's presence is the frontend's signal — the same data either
 * way, since only promote writes it.
 */

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type WithOtherProperties = { other_properties?: Record<string, any> | null };

export type CatalogMaterializeStatus = "pending" | "running" | "ready" | "failed";

export const isCatalogLayer = (layer?: WithOtherProperties | null): boolean =>
  !!layer?.other_properties?.["catalog_item"] ||
  !!layer?.other_properties?.["catalog_materialize"];

export const catalogMaterializeStatus = (
  layer?: WithOtherProperties | null
): CatalogMaterializeStatus | undefined =>
  layer?.other_properties?.["catalog_materialize"]?.["status"];

/** The layer's data is not on disk yet — nothing to draw. */
export const isCatalogLayerPending = (layer?: WithOtherProperties | null): boolean => {
  const status = catalogMaterializeStatus(layer);
  return status === "pending" || status === "running";
};

export const isCatalogLayerFailed = (layer?: WithOtherProperties | null): boolean =>
  catalogMaterializeStatus(layer) === "failed";

/**
 * The identity a catalog layer was promoted at, from its snapshot: the STAC
 * item id and the version. Compared against the live mirror to detect that
 * the provider has published newer data.
 */
export const catalogItemSnapshot = (
  layer?: WithOtherProperties | null
): { id?: string; version?: string } => {
  const item = layer?.other_properties?.["catalog_item"];
  return {
    id: item?.["id"],
    version: item?.["version"] != null ? String(item["version"]) : undefined,
  };
};
