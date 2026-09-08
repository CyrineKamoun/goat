/**
 * `fieldEditability` is the one derivation every field editor uses — the
 * attribute panel, the map's popover editor and the data table — so what it
 * says about a column is what all three do.
 */
import { describe, expect, it } from "vitest";

import { fieldEditability, vocabularyItems } from "@/lib/utils/allowedValues";

describe("fieldEditability", () => {
  it("makes a plain column freely editable", () => {
    const editability = fieldEditability({});

    expect(editability.readOnly).toBe(false);
    expect(editability.vocabulary).toBe(false);
    expect(editability.suggestions).toBe(false);
    expect(editability.items).toEqual([]);
  });

  it("picks from the vocabulary when only the listed values are accepted", () => {
    const editability = fieldEditability({ allowed_values: ["primary", "secondary"] }, "primary");

    expect(editability.vocabulary).toBe(true);
    expect(editability.suggestions).toBe(false);
    expect(editability.items.map((item) => item.value)).toEqual(["", "primary", "secondary"]);
  });

  it("turns the vocabulary into suggestions when other values are allowed", () => {
    // "Allow other values" has to reach the editors: the backend accepts a
    // value outside the list, so a pick-only control would be the only thing
    // refusing it.
    const editability = fieldEditability(
      { allowed_values: ["primary", "secondary"], allow_other: true },
      "tertiary"
    );

    expect(editability.suggestions).toBe(true);
    expect(editability.vocabulary).toBe(false);
    // Still offered, and the value held now among them.
    expect(editability.items.map((item) => item.value)).toContain("primary");
    expect(editability.items.map((item) => item.value)).toContain("tertiary");
  });

  it("keeps locked apart from computed: a locked column has no formula", () => {
    const locked = fieldEditability({ is_locked: true });
    const computed = fieldEditability({ is_computed: true });

    expect(locked.locked).toBe(true);
    expect(locked.computed).toBe(false);
    expect(computed.computed).toBe(true);
    expect(computed.locked).toBe(false);
    expect(locked.readOnly && computed.readOnly).toBe(true);
  });

  it("offers nothing on a read-only column, vocabulary or not", () => {
    const editability = fieldEditability({
      allowed_values: ["a", "b"],
      allow_other: true,
      is_locked: true,
    });

    expect(editability.vocabulary).toBe(false);
    expect(editability.suggestions).toBe(false);
    expect(editability.items).toEqual([]);
  });
});

describe("vocabularyItems", () => {
  it("leads with a blank option only when the column has no default", () => {
    expect(vocabularyItems({ allowed_values: ["a"] }, null)[0].value).toBe("");
    expect(vocabularyItems({ allowed_values: ["a"], default_value: "a" }, null)[0].value).toBe("a");
  });
});
