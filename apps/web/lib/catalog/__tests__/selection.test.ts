import { describe, expect, it } from "vitest";

import { cardSelectionState } from "@/lib/catalog/selection";

/**
 * The tri-state a bundle card shows. Worth pinning precisely: it is the one piece of
 * arithmetic in the picker, and getting `nextSelected` wrong makes a half-selected
 * bundle impossible to finish with one click.
 */
describe("cardSelectionState", () => {
  const bundle = ["a", "b", "c"];

  it("is unselected when nothing in it is picked", () => {
    expect(cardSelectionState(bundle, [])).toEqual({
      selected: false,
      indeterminate: false,
      nextSelected: true,
    });
  });

  it("is partial when some of its layers are picked", () => {
    expect(cardSelectionState(bundle, ["b"])).toEqual({
      selected: false,
      indeterminate: true,
      // Clicking a partial card completes it rather than clearing it.
      nextSelected: true,
    });
  });

  it("is selected only when every layer is picked", () => {
    expect(cardSelectionState(bundle, ["a", "b", "c"])).toEqual({
      selected: true,
      indeterminate: false,
      nextSelected: false,
    });
  });

  it("ignores selections belonging to other cards", () => {
    expect(cardSelectionState(bundle, ["x", "y", "a"])).toMatchObject({
      selected: false,
      indeterminate: true,
    });
  });

  it("treats a single-layer dataset as in or out", () => {
    expect(cardSelectionState(["only"], [])).toMatchObject({ selected: false, indeterminate: false });
    expect(cardSelectionState(["only"], ["only"])).toMatchObject({
      selected: true,
      indeterminate: false,
      nextSelected: false,
    });
  });

  it("stays safe while a bundle's layers are still loading", () => {
    // Members arrive from the API, so the card renders once with none of them.
    expect(cardSelectionState([], ["a"])).toEqual({
      selected: false,
      indeterminate: false,
      nextSelected: true,
    });
  });
});
