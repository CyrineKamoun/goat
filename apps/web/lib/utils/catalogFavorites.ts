/**
 * The id a catalog favourite is stored under: always the dataset.
 *
 * A favourite has to be readable by the views that show favourites, and both
 * of them work in datasets — the catalog list stars `dataset.id` and filters
 * with `ids` against the *collections* search, and the Add Layer picker stars
 * `collection.id`. A layer's own id is never a collection id (0 of the 19,356
 * single-layer datasets in the catalog have them equal), so storing one is
 * write-only: the star reads back as saved on the layer's own page and appears
 * nowhere else.
 *
 * So a layer detail page saves the dataset it belongs to: the collection it
 * resolved from for a single-layer dataset, the parent bundle for a member.
 * Only a layer with no dataset at all falls back to its own id.
 */
export function favoriteDatasetId(entry: {
  collection?: { id: string } | null;
  parent?: { id: string } | null;
  item?: { id: string } | null;
}): string | undefined {
  return entry.collection?.id ?? entry.parent?.id ?? entry.item?.id;
}
