import { describe, expect, it } from "vitest";

import { edgeDerivedInputHandles } from "@/lib/utils/workflowHandles";

const edge = (target: string, targetHandle: string | null, id = `${target}-${targetHandle}`) =>
  ({ id, source: "src", target, targetHandle }) as never;

describe("edgeDerivedInputHandles", () => {
  it("returns the named handles pointing at the node", () => {
    const edges = [edge("tool-a", "input_layer_id"), edge("tool-a", "overlay_layer_id")];

    expect(edgeDerivedInputHandles(edges, "tool-a")).toEqual([
      "input_layer_id",
      "overlay_layer_id",
    ]);
  });

  it("deduplicates a handle two edges point at", () => {
    // Real case: three tool nodes in a saved workflow each had two edges into
    // `input_layer_id`, which rendered two handles with the same React key.
    const edges = [
      edge("tool-a", "input_layer_id", "e1"),
      edge("tool-a", "overlay_layer_id", "e2"),
      edge("tool-a", "input_layer_id", "e3"),
    ];

    expect(edgeDerivedInputHandles(edges, "tool-a")).toEqual([
      "input_layer_id",
      "overlay_layer_id",
    ]);
  });

  it("ignores edges into other nodes", () => {
    const edges = [edge("tool-a", "input_layer_id"), edge("tool-b", "join_layer_id")];

    expect(edgeDerivedInputHandles(edges, "tool-a")).toEqual(["input_layer_id"]);
  });

  it("ignores the generic handle and edges with none", () => {
    const edges = [edge("tool-a", "input"), edge("tool-a", null)];

    expect(edgeDerivedInputHandles(edges, "tool-a")).toEqual([]);
  });
});
