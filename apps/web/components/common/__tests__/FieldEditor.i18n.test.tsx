/**
 * Every visible string in the field editor must come from i18n so the panel
 * follows the profile language. The type selector and the name placeholder
 * used to be hardcoded English.
 */
import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type { FieldDefinition } from "@/lib/validations/layer";

import FieldEditor from "@/components/common/FieldEditor";

vi.mock("react-i18next", () => ({
  useTranslation: () => ({ t: (key: string) => `i18n:${key}`, i18n: { language: "de" } }),
}));

const renderEditor = () =>
  render(
    <FieldEditor
      fields={[{ id: "f1", name: "class", kind: "string" } as FieldDefinition]}
      onChange={vi.fn()}
      selectedFieldId="f1"
      onSelectField={() => {}}
    />
  );

describe("FieldEditor i18n", () => {
  it("labels the selected field type through i18n", () => {
    renderEditor();
    expect(screen.queryByText("Text")).toBeNull();
    expect(screen.getByText("i18n:field_kind_text")).toBeTruthy();
  });

  it("translates the field name placeholder", () => {
    renderEditor();
    expect(screen.queryByPlaceholderText("Field name")).toBeNull();
    expect(screen.getByPlaceholderText("i18n:field_name")).toBeTruthy();
  });
});
