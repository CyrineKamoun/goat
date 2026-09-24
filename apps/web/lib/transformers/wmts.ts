/**
 * Converts a WMTS resource URL template to a Maplibre-compatible XYZ URL. Placeholders are
 * matched whatever their case, as GeoServer writes `{style}`.
 *
 * @param {string} resourceUrl - The WMTS resource URL template.
 * @param {string} style - The style to be used in the URL.
 * @param {string} tileMatrixSet - The tile matrix set to be used in the URL.
 * @param {string} matrixPrefix - What the set names its tile matrices before the zoom level,
 *   such as GeoWebCache's `EPSG:900913:`.
 * @returns {string} - The converted Maplibre-compatible URL.
 */
export const convertWmtsToXYZUrl = (
  resourceUrl: string,
  style?: string,
  tileMatrixSet?: string,
  matrixPrefix = ""
) => {
  const values: Record<string, string | undefined> = {
    tilematrix: `${matrixPrefix}{z}`,
    tilerow: "{y}",
    tilecol: "{x}",
    style,
    tilematrixset: tileMatrixSet,
  };
  return resourceUrl.replace(
    /\{(\w+)\}/g,
    (placeholder, name: string) => values[name.toLowerCase()] ?? placeholder
  );
};
