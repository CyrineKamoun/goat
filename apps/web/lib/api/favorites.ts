import { useCallback, useMemo } from "react";

import { apiRequestAuth, fetcher } from "@/lib/api/fetcher";
import { useAuthedSWR } from "@/lib/api/useAuthedSWR";

export const FAVORITES_API_BASE_URL = new URL("api/v2/favorite", process.env.NEXT_PUBLIC_API_URL).href;

/** What can be favourited — mirrors the backend's FavoriteItemType. */
export type FavoriteItemType = "catalog_item" | "workflow_template" | "project" | "dataset" | "template";

export const setFavorite = async (
  itemType: FavoriteItemType,
  itemId: string,
  favorite: boolean
): Promise<void> => {
  const response = await apiRequestAuth(
    `${FAVORITES_API_BASE_URL}/${itemType}/${encodeURIComponent(itemId)}`,
    { method: favorite ? "PUT" : "DELETE" }
  );
  if (!response.ok) {
    throw new Error(`Failed to update favourite ${itemId}`);
  }
};

/**
 * The caller's favourites of one kind, as the `starred` map + toggle the UI
 * consumes. Persistent — one hook shared by every star control, so the
 * catalog page and the add-layer picker always agree.
 *
 * The toggle is optimistic: the star flips immediately, the write follows,
 * and a failed write rolls back and then revalidates to the server's truth.
 */
export const useFavoriteStars = (itemType: FavoriteItemType) => {
  const { data, mutate } = useAuthedSWR<string[]>(
    () => [`${FAVORITES_API_BASE_URL}?item_type=${itemType}`],
    fetcher
  );

  const starred = useMemo<Record<string, boolean>>(
    () => Object.fromEntries((data ?? []).map((id) => [id, true])),
    [data]
  );

  const toggleStar = useCallback(
    (id: string) => {
      const next = !starred[id];
      mutate(
        async (current) => {
          await setFavorite(itemType, id, next);
          const ids = current ?? [];
          return next ? [id, ...ids.filter((value) => value !== id)] : ids.filter((value) => value !== id);
        },
        {
          optimisticData: (current) => {
            const ids = current ?? [];
            return next ? [id, ...ids.filter((value) => value !== id)] : ids.filter((value) => value !== id);
          },
          rollbackOnError: true,
          revalidate: false,
        }
      ).catch(() => {
        // Rolled back already; the server is the truth now, so ask it — and
        // do not let the rejection escape as an unhandled promise.
        void mutate();
      });
    },
    [starred, mutate, itemType]
  );

  return { starred, toggleStar };
};
