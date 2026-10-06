import { type APIRequestContext, expect } from "@playwright/test";

import { API_URL, castOf } from "./users";

/**
 * Datasets as the API sees them: the suite's checks that an upload became a
 * real layer, with its rows in DuckLake and served by geoapi.
 */

export const GEOAPI_URL = process.env.NEXT_PUBLIC_GEOAPI_URL ?? "http://127.0.0.1:8100";

/** The id of the caller's dataset with exactly this name, or undefined. */
export const datasetIdByName = async (api: APIRequestContext, name: string): Promise<string | undefined> => {
  const response = await api.post(`${API_URL}/api/v2/layer`, { data: { search: name } });
  expect(response.status()).toBe(200);
  const { items } = (await response.json()) as { items: { id: string; name: string }[] };
  return items.find((item) => item.name === name)?.id;
};

/** How many rows geoapi serves for a dataset. */
export const rowCount = async (api: APIRequestContext, layerId: string): Promise<number> => {
  const response = await api.get(`${GEOAPI_URL}/collections/${layerId}/items?limit=1`);
  expect(response.status(), await response.text()).toBe(200);
  return ((await response.json()) as { numberMatched: number }).numberMatched;
};

/** The id of one of the owner's provisioned datasets (auth on only). */
export const datasetOf = (key: "points" | "table" | "editable"): string => castOf().datasets[key];
