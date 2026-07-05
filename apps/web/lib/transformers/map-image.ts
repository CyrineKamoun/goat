import type { MapRef } from "react-map-gl/maplibre";

import type { PatternImage } from "@/lib/constants/pattern-images";
import type { FeatureLayerPointProperties } from "@/lib/validations/layer";

// Image prefix for marker images is needed to avoid
// name conflicts with other images from mapbox basemaps
export const PATTERN_IMAGE_PREFIX = "goat-pattern-";

// Track which image name→url is already loaded (and in-flight) per map, so that
// repeated loadImage() calls — panel re-renders, layer toggles, duplicate
// map-load handlers — don't refetch and re-decode an icon that's already on the
// map. Keyed by the underlying maplibre Map (survives MapRef identity changes,
// GC'd with the map). Presence is gated by hasImage() so a basemap style swap
// (which wipes all images) still triggers a reload; a changed url also reloads.
const loadedImageUrls = new WeakMap<object, Map<string, string>>();
const inFlightImageUrls = new WeakMap<object, Map<string, string>>();

function trackFor(
  store: WeakMap<object, Map<string, string>>,
  key: object
): Map<string, string> {
  let m = store.get(key);
  if (!m) {
    m = new Map();
    store.set(key, m);
  }
  return m;
}

/**
 * Load image from url and adds or updates the map with the image
 * @param map MapRef
 * @param url string
 * @param marker_name string
 * @param width number
 * @param height number
 * @param sdf boolean
 * @returns void
 */
export const loadImage = (
  map: MapRef | null | undefined,
  url: string,
  marker_name: string,
  sdf?: boolean,
  targetWidth = 200, // default
  targetHeight = 200 // default
) => {
  if (!map) return;

  // Idempotency guard: skip if this exact image (name→url) is already on the map
  // or currently loading. A style-swap wipe (hasImage false) or a url change
  // both fall through and reload.
  const glMap = (typeof map.getMap === "function" ? map.getMap() : map) as object;
  const loaded = trackFor(loadedImageUrls, glMap);
  const inFlight = trackFor(inFlightImageUrls, glMap);
  let present = false;
  try {
    present = !!map.hasImage(marker_name);
  } catch {
    present = false;
  }
  if (present && loaded.get(marker_name) === url) return;
  if (inFlight.get(marker_name) === url) return;
  inFlight.set(marker_name, url);

  const addOrUpdateImage = (
    image:
      | HTMLImageElement
      | { width: number; height: number; data: Uint8Array | Uint8ClampedArray }
      | ImageData
      | ImageBitmap,
    pixelRatio?: number
  ) => {
    // Check if map exists and is ready (style loaded)
    if (!map) return;
    try {
      // The map might not be ready yet or could have been unmounted
      // hasImage/removeImage/addImage require the style to be loaded
      if (map.hasImage(marker_name)) {
        map.removeImage(marker_name);
      }
      map.addImage(marker_name, image, { sdf: sdf ?? true, pixelRatio: pixelRatio ?? 1 });
      loaded.set(marker_name, url);
    } catch (error) {
      // Map may have been unmounted or style not loaded yet - ignore silently
      console.debug(`Failed to add/update map image "${marker_name}":`, error);
    } finally {
      inFlight.delete(marker_name);
    }
  };

  const rasterizeToCanvas = (img: HTMLImageElement, targetW: number, targetH: number) => {
    // Cap at 2 to prevent print contexts (DPI 300+ → devicePixelRatio 3.125+) from inflating icons
    const pixelRatio = Math.min(window.devicePixelRatio || 1, 2);
    const canvas = document.createElement("canvas");
    canvas.width = targetW * pixelRatio;
    canvas.height = targetH * pixelRatio;

    const context = canvas.getContext("2d");
    if (context) {
      context.scale(pixelRatio, pixelRatio);
      context.imageSmoothingEnabled = true;
      context.drawImage(img, 0, 0, targetW, targetH);

      const imageData = context.getImageData(0, 0, canvas.width, canvas.height);
      addOrUpdateImage(
        {
          width: canvas.width,
          height: canvas.height,
          data: new Uint8Array(imageData.data.buffer),
        },
        pixelRatio
      );
    }
  };

  // Create an Image object for all formats the browser supports
  const img = new Image();
  img.crossOrigin = "anonymous";
  img.src = url;
  img.onload = () => {
    rasterizeToCanvas(img, targetWidth, targetHeight);
  };
  img.onerror = (err) => {
    inFlight.delete(marker_name);
    console.error(`Failed to load image: ${url}`, err);
  };
};

/**
 * Add or update marker images on the map
 * @param properties FeatureLayerPointProperties
 * @param map MapRef
 * @returns void
 */
export function addOrUpdateMarkerImages(
  id: number,
  properties: FeatureLayerPointProperties,
  map: MapRef | null
) {
  if (map && properties.custom_marker) {
    const markers = [properties.marker];
    properties.marker_mapping?.forEach((markerMap) => {
      if (markerMap && markerMap[1]) markers.push(markerMap[1]);
    });
    markers.forEach((marker) => {
      if (marker && marker.url && marker.name) {
        const name = `${id}-${marker.name}`;
        const sdf = marker.source === "library" ? true : false;
        loadImage(map, marker.url, name, sdf);
      }
    });
  }
}

/**
 * Add pattern images on the map
 * @param patterns PatternImage[]
 * @param map MapRef
 */
export function addPatternImages(patterns: PatternImage[], map: MapRef | null) {
  if (map && patterns) {
    patterns.forEach((pattern) => {
      const name = `${PATTERN_IMAGE_PREFIX}${pattern.name}`;
      loadImage(map, pattern.url, name, false, pattern.width, pattern.height);
    });
  }
}
