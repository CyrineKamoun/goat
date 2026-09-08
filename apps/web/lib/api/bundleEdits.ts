import { apiRequestAuth } from "@/lib/api/fetcher";
import { COLLECTIONS_API_BASE_URL } from "@/lib/api/layers";
import { type Job, executeProcessAsync } from "@/lib/api/processes";
import type { PendingFeature } from "@/lib/store/featureEditor/types";

/** An edit batch for a bundle's editable member layer.
 *
 *  Sent as one request because the server writes both the edges layer and the
 *  nodes layer it derives, and half of that landing would leave edges pointing
 *  at nodes that were never written. */
export interface BundleEditPayload {
  base_revision: number;
  create: { geometry: GeoJSON.Geometry; properties: Record<string, unknown> }[];
  update: { id: string; geometry: GeoJSON.Geometry; properties: Record<string, unknown> }[];
  delete: string[];
}

export interface BundleEditResponse {
  revision: number;
  bundle_id: string;
  /** The bundle's nodes layer, whose tiles also need refreshing after a save. */
  nodes_layer_id: string;
  /** The artifact rebuild the server queued, for job tracking. Null when the
   *  dispatch failed — the layers are saved either way, and the bundle page's
   *  Update button is the recovery path. */
  rebuild_job_id: string | null;
  edges: {
    created: string[];
    updated: string[];
    deleted: string[];
    split: { original_id: string; halves: string[] }[];
  };
  nodes: { created: string[]; removed: string[] };
}

/** Raised when the network moved on while the user was editing. */
export class BundleEditConflictError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "BundleEditConflictError";
  }
}

/** Editor properties are for drawing, not for the layer. */
const cleanProperties = (properties: Record<string, unknown>) => {
  const clean: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(properties)) {
    if (key.startsWith("_")) continue;
    clean[key] = value === undefined || value === "" ? null : value;
  }
  return clean;
};

export function buildBundleEditPayload(
  pendingFeatures: Record<string, PendingFeature>,
  baseRevision: number
): BundleEditPayload {
  const committed = Object.values(pendingFeatures).filter((f) => f.committed);
  return {
    base_revision: baseRevision,
    create: committed
      .filter((f) => f.action === "create")
      .map((f) => ({
        geometry: f.geometry as GeoJSON.Geometry,
        properties: cleanProperties(f.properties),
      })),
    update: committed
      .filter((f) => f.action === "update")
      .map((f) => ({
        id: f.id,
        geometry: f.geometry as GeoJSON.Geometry,
        properties: cleanProperties(f.properties),
      })),
    delete: committed.filter((f) => f.action === "delete").map((f) => f.id),
  };
}

export const applyBundleEdits = async (
  layerId: string,
  payload: BundleEditPayload
): Promise<BundleEditResponse> => {
  const response = await apiRequestAuth(`${COLLECTIONS_API_BASE_URL}/${layerId}/edits`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (response.status === 409) {
    const body = await response.json();
    throw new BundleEditConflictError(body.detail ?? "This network changed while you were editing.");
  }
  if (!response.ok) {
    const body = await response.json();
    throw new Error(body.detail || "Failed to save the bundle's edits");
  }
  return response.json();
};

/** Queue a rebuild of the bundle's derived artifacts.
 *
 *  Dispatched from the client after a successful save, the way a dataset update
 *  dispatches layer_update. The artifact stays stale until this lands, so tools
 *  refuse to route on the bundle in the meantime. */
export const rebuildBundleArtifact = async (bundleId: string): Promise<Job> =>
  executeProcessAsync("bundle_artifact_rebuild", { bundle_id: bundleId });
