import { describe, expect, it } from "vitest";

import { type BundleMemberGate, isBundleMemberLayer, isEditableBundleMember } from "@/lib/utils/bundleEditable";

const members = new Map<string, BundleMemberGate>([
  ["layer-edges", { role: "edges", editable: true }],
  ["layer-nodes", { role: "nodes", editable: false }],
  ["layer-legacy", { role: "edges" }],
]);

describe("bundle member gating", () => {
  it.each([
    ["an editable member", "layer-edges", true, true],
    ["a member whose role is not editable", "layer-nodes", true, false],
    ["a member with no flag at all", "layer-legacy", true, false],
    ["a layer belonging to no bundle", "plain-layer", false, false],
  ])("%s", (_case, layerId, isMember, isEditable) => {
    expect(isBundleMemberLayer(layerId, members)).toBe(isMember);
    expect(isEditableBundleMember(layerId, members)).toBe(isEditable);
  });

  it("claims nothing while the members are still loading", () => {
    expect(isBundleMemberLayer("layer-edges", undefined)).toBe(false);
    expect(isEditableBundleMember("layer-edges", undefined)).toBe(false);
  });
});
