/**
 * The tree highlights what is open. A bundle-backed group opens its own
 * settings panel, and selecting it clears `selectedLayerIds` — so if the
 * highlight came from those alone, the one row with a panel on screen would be
 * the one row not highlighted.
 */
import { describe, expect, it } from "vitest";

import { selectedTreeItemIds } from "@/lib/utils/map/layerTreeSelection";

const items = [
  { id: "group-1", data: { id: 1, type: "group", bundle_id: "bundle-a" } },
  { id: "group-2", data: { id: 2, type: "group" } },
  { id: "layer-1", data: { id: 1, type: "layer" } },
  { id: "layer-7", data: { id: 7, type: "layer" } },
];

describe("selectedTreeItemIds", () => {
  it("highlights nothing when nothing is selected", () => {
    expect(selectedTreeItemIds(items, { selectedLayerIds: [], selectedBundleId: null })).toEqual([]);
  });

  it("highlights the selected layers", () => {
    expect(selectedTreeItemIds(items, { selectedLayerIds: [7] })).toEqual(["layer-7"]);
  });

  it("highlights the bundle group whose panel is open", () => {
    expect(selectedTreeItemIds(items, { selectedLayerIds: [], selectedBundleId: "bundle-a" })).toEqual([
      "group-1",
    ]);
  });

  it("never matches a layer id against a group row", () => {
    // Group ids and layer_project ids are separate sequences that both count
    // from 1, so selecting layer 1 must not light up group 1 as well.
    expect(selectedTreeItemIds(items, { selectedLayerIds: [1] })).toEqual(["layer-1"]);
  });

  it("leaves a bundle group alone while another bundle is open", () => {
    expect(selectedTreeItemIds(items, { selectedLayerIds: [], selectedBundleId: "bundle-b" })).toEqual([]);
  });
});
