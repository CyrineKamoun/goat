import { describe, expect, it } from "vitest";

import { orderLayersByTree } from "@/lib/utils/map/layerTreeOrder";
import type { ProjectLayer, ProjectLayerGroup } from "@/lib/validations/project";

const makeLayer = (
  id: number,
  order: number,
  layer_project_group_id: number | null = null
): ProjectLayer =>
  ({
    id,
    order,
    layer_project_group_id,
  }) as unknown as ProjectLayer;

const makeGroup = (
  id: number,
  order: number,
  parent_id: number | null = null,
  properties: Record<string, unknown> | null = null
): ProjectLayerGroup =>
  ({
    id,
    order,
    parent_id,
    properties,
    name: `group-${id}`,
    project_id: "00000000-0000-0000-0000-000000000000",
  }) as unknown as ProjectLayerGroup;

const ids = (layers: ProjectLayer[]) => layers.map((l) => l.id);

describe("orderLayersByTree", () => {
  it("sorts flat by order when there are no groups", () => {
    const layers = [makeLayer(3, 2), makeLayer(1, 0), makeLayer(2, 1)];
    expect(ids(orderLayersByTree(layers, []))).toEqual([1, 2, 3]);
    expect(ids(orderLayersByTree(layers, undefined))).toEqual([1, 2, 3]);
  });

  it("places grouped layers at their group's position, not by their raw order", () => {
    // Panel:
    //   group "Points" (order 0)
    //     layer 10 (order 0)
    //     layer 11 (order 1)
    //   layer 20 "Heatmap" (order 1, root)
    // A flat sort by order would interleave: [10, 11+20 tie] — with the
    // heatmap above layer 11. Tree order keeps both points above the heatmap.
    const groups = [makeGroup(1, 0)];
    const layers = [makeLayer(20, 1), makeLayer(10, 0, 1), makeLayer(11, 1, 1)];
    expect(ids(orderLayersByTree(layers, groups))).toEqual([10, 11, 20]);
  });

  it("orders sibling groups and root layers together by order", () => {
    // root: layer 30 (order 0), group A (order 1) [40, 41], group B (order 2) [50]
    const groups = [makeGroup(1, 1), makeGroup(2, 2)];
    const layers = [
      makeLayer(50, 0, 2),
      makeLayer(41, 1, 1),
      makeLayer(30, 0),
      makeLayer(40, 0, 1),
    ];
    expect(ids(orderLayersByTree(layers, groups))).toEqual([30, 40, 41, 50]);
  });

  it("traverses nested groups depth-first", () => {
    // group A (order 0): layer 10 (order 0), subgroup B (order 1): layer 20; root layer 30 (order 1)
    const groups = [makeGroup(1, 0), makeGroup(2, 1, 1)];
    const layers = [makeLayer(30, 1), makeLayer(20, 0, 2), makeLayer(10, 0, 1)];
    expect(ids(orderLayersByTree(layers, groups))).toEqual([10, 20, 30]);
  });

  it("excludes layers in invisible groups, including nested ones", () => {
    // group A (visible): layer 10; group B (invisible): layer 20;
    // subgroup C of B (visible itself, but parent invisible): layer 30
    const groups = [
      makeGroup(1, 0),
      makeGroup(2, 1, null, { visibility: false }),
      makeGroup(3, 0, 2),
    ];
    const layers = [makeLayer(10, 0, 1), makeLayer(20, 0, 2), makeLayer(30, 0, 3), makeLayer(40, 2)];
    expect(ids(orderLayersByTree(layers, groups))).toEqual([10, 40]);
  });

  it("keeps ungrouped layers when groups exist", () => {
    const groups = [makeGroup(1, 1)];
    const layers = [makeLayer(10, 0), makeLayer(20, 0, 1), makeLayer(30, 2)];
    expect(ids(orderLayersByTree(layers, groups))).toEqual([10, 20, 30]);
  });
});
