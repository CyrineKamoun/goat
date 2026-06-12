import useSWR from "swr";

import { fetcher } from "@/lib/api/fetcher";
import { GEOAPI_BASE_URL } from "@/lib/constants";

interface SampleFeature {
  properties: Record<string, unknown>;
  geometry: { type: string; coordinates: number[] | number[][] | number[][][] };
}

interface SampleFeatureResponse {
  features?: SampleFeature[];
}

export function useSampleFeature(layerId: string | null | undefined, enabled: boolean) {
  const url =
    enabled && layerId ? `${GEOAPI_BASE_URL}/collections/${layerId}/items?limit=1` : null;
  const { data, error, isLoading } = useSWR<SampleFeatureResponse>(url, fetcher);
  return {
    feature: data?.features?.[0],
    error,
    isLoading,
  };
}

export type { SampleFeature };
