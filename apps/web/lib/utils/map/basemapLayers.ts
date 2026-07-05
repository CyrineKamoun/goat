export type BasemapCategory =
  | "labels"
  | "roads"
  | "water"
  | "landuse"
  | "buildings"
  | "poi"
  | "other";

export interface BasemapLayerInfo {
  id: string;
  category: BasemapCategory;
  prettyName: string;
}

interface StyleLayerLike {
  id: string;
  type: string;
  "source-layer"?: string;
  layout?: Record<string, unknown>;
}

function classifyOne(layer: StyleLayerLike): BasemapCategory {
  const sourceLayer = (layer["source-layer"] ?? "").toLowerCase();
  const isSymbol = layer.type === "symbol";
  const hasText = !!(layer.layout && "text-field" in layer.layout);
  // Labels first so a road_label symbol is "labels", not "roads".
  if (isSymbol && hasText) return "labels";
  if (/transportation|road|street/.test(sourceLayer)) return "roads";
  if (/water|waterway|ocean/.test(sourceLayer)) return "water";
  if (/landuse|landcover|park|wood|grass/.test(sourceLayer)) return "landuse";
  if (/building/.test(sourceLayer)) return "buildings";
  if (/poi/.test(sourceLayer)) return "poi";
  return "other";
}

export function prettifyLayerId(id: string): string {
  const words = id
    .replace(/[_\-.]+/g, " ")
    .replace(/([a-z0-9])([A-Z])/g, "$1 $2")
    .trim()
    .split(/\s+/)
    .filter(Boolean);
  if (words.length === 0) return id;
  const first = words[0][0].toUpperCase() + words[0].slice(1).toLowerCase();
  return [first, ...words.slice(1).map((w) => w.toLowerCase())].join(" ");
}

export function classifyBasemapLayers(
  styleLayers: StyleLayerLike[]
): BasemapLayerInfo[] {
  return styleLayers.map((layer) => ({
    id: layer.id,
    category: classifyOne(layer),
    prettyName: prettifyLayerId(layer.id),
  }));
}

// A target that doesn't point at a current project layer (e.g. that layer was
// deleted) falls back to "all". Shared by the renderer and the dialog so both
// resolve an orphaned target the same way.
export function resolveTarget(target: string, validTargetIds: Set<string>): string {
  return target !== "all" && !validTargetIds.has(target) ? "all" : target;
}

export function computeStackOrder(
  userLayers: Array<{ id: string; sublayers: string[] }>,
  promoted: Array<{ id: string; relation: "above" | "below"; target: string }>
): string[] {
  const userIds = new Set(userLayers.map((u) => u.id));
  const norm = promoted.map((p) => ({
    ...p,
    target: resolveTarget(p.target, userIds),
  }));

  const out: string[] = [];
  // "above all" → very top, preserving input order.
  out.push(
    ...norm.filter((p) => p.relation === "above" && p.target === "all").map((p) => p.id)
  );
  for (const u of userLayers) {
    out.push(
      ...norm.filter((p) => p.relation === "above" && p.target === u.id).map((p) => p.id)
    );
    out.push(...u.sublayers);
    out.push(
      ...norm.filter((p) => p.relation === "below" && p.target === u.id).map((p) => p.id)
    );
  }
  // "below all" entries are intentionally excluded — they stay in their native
  // basemap position (below all user data).
  return out;
}
