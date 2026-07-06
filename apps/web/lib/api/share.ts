import { API_BASE_URL } from "@/lib/constants";

import { apiRequestAuth } from "@/lib/api/fetcher";
import type { LayerSharedWith } from "@/lib/validations/layer";
import type { ProjectSharedWith } from "@/lib/validations/project";

export const SHARE_API_BASE_URL = new URL("api/v2/share", API_BASE_URL).href;

const shareItem = async (
  itemType: "project" | "layer",
  itemId: string,
  payload: ProjectSharedWith | LayerSharedWith
) => {
  const params = new URLSearchParams();
  payload.organizations?.forEach((o) => params.append("organization_ids", o.id));
  payload.teams?.forEach((t) => params.append("team_ids", t.id));

  const qs = params.toString();
  const url = `${SHARE_API_BASE_URL}/${itemType}/${itemId}${qs ? `?${qs}` : ""}`;

  const response = await apiRequestAuth(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });

  if (!response.ok) {
    try {
      throw await response.json();
    } catch {
      throw new Error(`Failed to share ${itemType}`);
    }
  }
  return await response.json();
};

export const shareProject = async (projectId: string, payload: ProjectSharedWith) => {
  return shareItem("project", projectId, payload);
};

export const shareLayer = async (layerId: string, payload: LayerSharedWith) => {
  return shareItem("layer", layerId, payload);
};
