import type { ProjectLayer, ProjectLayerGroup } from "@/lib/validations/project";

/**
 * Orders project layers to match the visual layer panel hierarchy and filters
 * out layers that belong to invisible groups.
 *
 * `layer.order` is a sibling position within its parent group — not a global
 * position — so a flat sort interleaves layers from different groups. This
 * performs a tree-aware DFS traversal (layers inside a group inherit the
 * group's position) so the returned array matches the panel top-to-bottom,
 * which is the stacking order expected by the map `Layers` component.
 */
export function orderLayersByTree<T extends ProjectLayer>(
  layers: T[],
  layerGroups: ProjectLayerGroup[] | null | undefined
): T[] {
  if (!layerGroups || layerGroups.length === 0) {
    return [...layers].sort((a, b) => (a.order ?? 0) - (b.order ?? 0));
  }

  // Collect invisible group IDs, including groups nested inside invisible parents
  const invisibleGroupIds = new Set<number>();
  const findInvisibleGroups = () => {
    layerGroups.forEach((group) => {
      const groupVisibility = group.properties?.visibility ?? true;
      if (!groupVisibility) {
        invisibleGroupIds.add(group.id);
      }
      if (group.parent_id && invisibleGroupIds.has(group.parent_id)) {
        invisibleGroupIds.add(group.id);
      }
    });
  };
  let previousSize = -1;
  while (invisibleGroupIds.size !== previousSize) {
    previousSize = invisibleGroupIds.size;
    findInvisibleGroups();
  }

  const visibleLayers = layers.filter(
    (layer) =>
      !layer.layer_project_group_id || !invisibleGroupIds.has(layer.layer_project_group_id)
  );

  // Build children lookup per parent for DFS traversal
  type TreeNode = { type: "group" | "layer"; id: number; order: number; layer?: T };
  const childrenByParent = new Map<number | null, TreeNode[]>();

  for (const group of layerGroups) {
    if (invisibleGroupIds.has(group.id)) continue;
    const parentKey = group.parent_id ?? null;
    if (!childrenByParent.has(parentKey)) childrenByParent.set(parentKey, []);
    childrenByParent.get(parentKey)!.push({ type: "group", id: group.id, order: group.order ?? 0 });
  }

  for (const layer of visibleLayers) {
    const parentKey = layer.layer_project_group_id ?? null;
    if (!childrenByParent.has(parentKey)) childrenByParent.set(parentKey, []);
    childrenByParent.get(parentKey)!.push({ type: "layer", id: layer.id, order: layer.order ?? 0, layer });
  }

  for (const children of childrenByParent.values()) {
    children.sort((a, b) => a.order - b.order);
  }

  // DFS collects layers in visual tree order
  const orderedLayers: T[] = [];
  const collectLayers = (parentId: number | null) => {
    const children = childrenByParent.get(parentId);
    if (!children) return;
    for (const child of children) {
      if (child.type === "layer" && child.layer) {
        orderedLayers.push(child.layer);
      } else if (child.type === "group") {
        collectLayers(child.id);
      }
    }
  };
  collectLayers(null);

  return orderedLayers;
}
