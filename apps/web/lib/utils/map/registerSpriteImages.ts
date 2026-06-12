import type { Map as MapLibreMap } from "maplibre-gl";

interface SpriteSpec {
  name: string;
  url: string;
  sdf: boolean;
  /** Pixel size at which to rasterise (square). */
  size: number;
}

// Note on `size`: this is the rasterisation resolution, not the rendered size.
// MapLibre scales the registered raster by `icon-size`. Rendering at a higher
// source resolution keeps edges sharp when users dial `decoration_size` up
// (negligible memory cost: 128*128*4 ≈ 64KB per icon).
//
// Exported so the transformer can derive a pixel-accurate `icon-size`
// multiplier from a user-supplied target pixel size: icon-size = target / source.
export const ARROW_SDF_SOURCE_SIZE = 128;

const SPRITES: SpriteSpec[] = [
  { name: "arrow-sdf", url: "/sprites/arrow.svg", sdf: true, size: ARROW_SDF_SOURCE_SIZE },
  { name: "badge-sdf", url: "/sprites/badge.svg", sdf: true, size: 64 },
];

async function loadAndRegister(map: MapLibreMap, spec: SpriteSpec): Promise<void> {
  if (map.hasImage(spec.name)) return;

  const img = new Image(spec.size, spec.size);
  img.src = spec.url;
  await img.decode();

  const canvas = document.createElement("canvas");
  canvas.width = spec.size;
  canvas.height = spec.size;
  const ctx = canvas.getContext("2d");
  if (!ctx) return;
  ctx.drawImage(img, 0, 0, spec.size, spec.size);
  const imageData = ctx.getImageData(0, 0, spec.size, spec.size);

  // Re-check after the await — style reloads can race with our load.
  if (map.hasImage(spec.name)) return;
  map.addImage(spec.name, imageData, { sdf: spec.sdf });
}

/**
 * Idempotently registers all known decoration sprites on the map.
 * Safe to call from `onLoad` / `styledata` events; in-flight registrations
 * fall through quickly because hasImage() short-circuits.
 */
export async function registerSpriteImages(map: MapLibreMap): Promise<void> {
  await Promise.all(SPRITES.map((s) => loadAndRegister(map, s).catch(() => undefined)));
}
