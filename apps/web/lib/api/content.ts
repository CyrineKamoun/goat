import { mutate } from "swr";

import { apiRequestAuth, fetcher } from "@/lib/api/fetcher";
import { useAuthedSWR } from "@/lib/api/useAuthedSWR";
import {
  type ContentPage,
  type ContentQueryParams,
  type ContentRef,
  type ContentType,
  type Space,
  type SpaceDefaultRole,
  type SpaceUsage,
  type TransferPreview,
  type TransferPreviewRequest,
  type TransferRequest,
  type TransferResult,
  type TrashItem,
  transferPreviewSchema,
  transferResultSchema,
} from "@/lib/validations/content";

export const SPACE_API_BASE_URL = new URL("api/v2/space", process.env.NEXT_PUBLIC_API_URL).href;
export const CONTENT_API_BASE_URL = new URL("api/v2/content", process.env.NEXT_PUBLIC_API_URL).href;

/** SWR key matcher for everything the Content page lists (feed, trash). */
export const matchesContentFeedKey = (key: unknown): boolean => {
  const url = Array.isArray(key) ? key[0] : key;
  return typeof url === "string" && url.startsWith(CONTENT_API_BASE_URL);
};

/** Refresh every content list after a mutation (create, move, share, delete, transfer, restore). */
export const refreshContentFeed = () => mutate(matchesContentFeedKey);

export const useSpaces = () => {
  const { data, isLoading, error, mutate } = useAuthedSWR<Space[]>(SPACE_API_BASE_URL, fetcher);
  return { spaces: data ?? [], isLoading, isError: error, mutate };
};

/** Live storage/content usage of a space (D13) — members only; the space
 * settings dialog and the Content details location card show it read-only.
 * `spaceId` null (no active space yet, or not applicable) skips the fetch. */
export const useSpaceUsage = (spaceId: string | null) => {
  const { data, isLoading, error, mutate } = useAuthedSWR<SpaceUsage>(
    spaceId ? `${SPACE_API_BASE_URL}/${spaceId}/usage` : null,
    fetcher
  );
  return { usage: data, isLoading, isError: error, mutate };
};

export const useContent = (params: ContentQueryParams | null) => {
  const { data, isLoading, error, mutate, isValidating } = useAuthedSWR<ContentPage>(
    params ? [CONTENT_API_BASE_URL, params] : null,
    fetcher
  );
  return { page: data, isLoading, isError: error, mutate, isValidating };
};

export const useSharedWithSpace = (spaceId: string | null) =>
  useContent(spaceId ? { view: "shared_with_space", space_id: spaceId, size: 100 } : null);

export const useTrash = (spaceId: string | null) => {
  const { data, isLoading, error, mutate } = useAuthedSWR<TrashItem[]>(
    spaceId ? [`${CONTENT_API_BASE_URL}/trash`, { space_id: spaceId }] : null,
    fetcher
  );
  return { items: data ?? [], isLoading, isError: error, mutate };
};

const readError = async (response: Response, fallback: string): Promise<never> => {
  let detail = fallback;
  try {
    const body = await response.json();
    if (typeof body?.detail === "string") detail = body.detail;
  } catch {
    // no JSON body
  }
  throw new Error(detail);
};

const postJson = async (url: string, body: unknown, method: "POST" | "PATCH" = "POST") =>
  apiRequestAuth(url, {
    method,
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });

export const restoreContent = async (items: ContentRef[]): Promise<void> => {
  const response = await postJson(`${CONTENT_API_BASE_URL}/restore`, { items });
  if (!response.ok) await readError(response, "Failed to restore");
};

export const previewTransfer = async (body: TransferPreviewRequest): Promise<TransferPreview> => {
  const response = await postJson(`${CONTENT_API_BASE_URL}/transfer/preview`, body);
  if (!response.ok) await readError(response, "Failed to preview transfer");
  return transferPreviewSchema.parse(await response.json());
};

export const transferContent = async (body: TransferRequest): Promise<TransferResult> => {
  const response = await postJson(`${CONTENT_API_BASE_URL}/transfer`, body);
  if (!response.ok) await readError(response, "Failed to transfer");
  return transferResultSchema.parse(await response.json());
};

export const updateSpaceDefaultRole = async (spaceId: string, role: SpaceDefaultRole): Promise<Space> => {
  const response = await postJson(`${SPACE_API_BASE_URL}/${spaceId}`, { default_role: role }, "PATCH");
  if (!response.ok) await readError(response, "Failed to update space");
  return (await response.json()) as Space;
};

export const setRestricted = async (type: ContentType, id: string, restricted: boolean): Promise<void> => {
  const response = await postJson(`${CONTENT_API_BASE_URL}/${type}/${id}/restricted`, { restricted }, "PATCH");
  if (!response.ok) await readError(response, "Failed to update access");
};
