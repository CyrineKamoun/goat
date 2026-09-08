import type { Layer } from "@/lib/validations/layer";
import type { ProjectLayer } from "@/lib/validations/project";

import { isCatalogLayer } from "@/lib/utils/catalog-layer";

/**
 * When the DATA behind a layer was last updated — the one date a metadata tab
 * should show, and never the same thing as the layer's `updated_at`.
 *
 * Three timestamps exist and only one of them answers "how current is this":
 *
 * - `updated_at` on a project layer is the later of the dataset's and this
 *   project's link, because the map keys its tile source on it. Restyling moves
 *   it, so it says nothing about the data.
 * - `dataset_updated_at` is the dataset row's own: for a layer you hold, when
 *   its data or fields last changed. That is the answer for your own data.
 * - A catalog layer is a copy of someone else's dataset. Its row changes when
 *   GOAT promotes or re-materializes the copy — minutes ago for a dataset
 *   published years back — so the only honest date is the provider's own,
 *   snapshotted as `catalog_item.updated`.
 *
 * A catalog layer whose snapshot predates that field returns **undefined**
 * rather than falling back: "2 minutes ago" for a 2022 dataset is worse than
 * saying nothing.
 */
export function datasetUpdatedAt(dataset: Layer | ProjectLayer): string | undefined {
  const other = dataset.other_properties as
    | { catalog_item?: { updated?: string | null } }
    | undefined;

  if (isCatalogLayer(dataset)) {
    return other?.catalog_item?.updated ?? undefined;
  }

  const own = (dataset as ProjectLayer).dataset_updated_at;
  return own ?? dataset.updated_at ?? undefined;
}
