/**
 * The vocabulary input has two commit paths — Enter and blur — and they must
 * mean the same thing. Entering a value that is already a chip is a no-op:
 * nothing is added, nothing is removed, and the box is cleared so the Enter
 * does not read as having been ignored.
 */
import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type { FieldDefinition } from "@/lib/validations/layer";

import FieldEditor from "@/components/common/FieldEditor";

vi.mock("react-i18next", () => ({
  useTranslation: () => ({ t: (key: string) => key, i18n: { language: "en" } }),
}));

const renderEditor = (field: Partial<FieldDefinition>) => {
  const onChange = vi.fn();
  render(
    <FieldEditor
      fields={[{ id: "f1", name: "class", kind: "string", ...field } as FieldDefinition]}
      onChange={onChange}
      selectedFieldId="f1"
      onSelectField={() => {}}
    />
  );
  return { input: screen.getByPlaceholderText("type_value_enter") as HTMLInputElement, onChange };
};

const chips = () => Array.from(document.querySelectorAll(".MuiChip-label")).map((node) => node.textContent);

describe("AllowedValuesInput", () => {
  it("adds a value that is not in the vocabulary yet", () => {
    const { input, onChange } = renderEditor({ allowed_values: ["a", "b"] });

    fireEvent.change(input, { target: { value: "c" } });
    fireEvent.keyDown(input, { key: "Enter" });

    expect(onChange).toHaveBeenCalledTimes(1);
    expect(onChange.mock.calls[0][0][0].allowed_values).toEqual(["a", "b", "c"]);
    expect(input.value).toBe("");
  });

  it("treats entering a value that is already a chip as a no-op", () => {
    const { input, onChange } = renderEditor({ allowed_values: ["a", "b"] });

    fireEvent.change(input, { target: { value: "a" } });
    fireEvent.keyDown(input, { key: "Enter" });

    expect(onChange).not.toHaveBeenCalled();
    expect(chips()).toEqual(["a", "b"]);
    // Committed, so the box empties — not left holding text that never landed.
    expect(input.value).toBe("");
  });

  it("treats a duplicate committed on blur the same way", () => {
    const { input, onChange } = renderEditor({ kind: "number", allowed_values: [30, 50] });

    fireEvent.change(input, { target: { value: "30" } });
    fireEvent.blur(input);

    expect(onChange).not.toHaveBeenCalled();
    expect(chips()).toEqual(["30", "50"]);
    expect(input.value).toBe("");
  });

  it("still commits a typed value that is only blurred away from", () => {
    const { input, onChange } = renderEditor({ allowed_values: ["a"] });

    fireEvent.change(input, { target: { value: "b" } });
    fireEvent.blur(input);

    expect(onChange.mock.calls[0][0][0].allowed_values).toEqual(["a", "b"]);
  });
});
