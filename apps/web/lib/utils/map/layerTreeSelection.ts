/** A row of the project layer tree, as far as selection is concerned. */
export type SelectableTreeItem = {
  /** The tree's own composite id, which is what the tree highlights. */
  id: string;
  data?: {
    id: number;
    type: string;
    /** Set on a group that stands for a bundle. */
    bundle_id?: string | null;
  };
};

/**
 * Which rows the tree highlights: the ones whose settings panel is open.
 *
 * A layer is addressed by its `layer_project` id and a bundle by its own id —
 * a bundle is a *group* in the tree, so it can never appear in
 * `selectedLayerIds`, and selecting one clears them. Both id spaces count
 * from 1, so a layer id is only ever matched against a layer row.
 */
export function selectedTreeItemIds(
  items: SelectableTreeItem[],
  selection: { selectedLayerIds: number[]; selectedBundleId?: string | null }
): string[] {
  const { selectedLayerIds, selectedBundleId } = selection;
  if (selectedLayerIds.length === 0 && !selectedBundleId) return [];
  return items
    .filter((item) => {
      const node = item.data;
      if (!node) return false;
      if (node.type === "group") return !!selectedBundleId && node.bundle_id === selectedBundleId;
      return selectedLayerIds.includes(node.id);
    })
    .map((item) => item.id);
}
