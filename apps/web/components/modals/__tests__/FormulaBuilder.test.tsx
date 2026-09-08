import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import FormulaBuilder from "@/components/modals/FormulaBuilder";

vi.mock("react-i18next", () => ({ useTranslation: () => ({ t: (key: string) => key }) }));
vi.mock("@/lib/api/expressions", () => ({
  FUNCTION_CATEGORIES: {
    math: { labelKey: "category_math", icon: "calculator" },
  },
  previewExpressionAsAggregation: vi.fn(),
  previewSql: vi.fn(),
  useExpressionFunctions: () => ({ functions: [], total: 0, isLoading: false, isError: undefined }),
  validateExpression: vi.fn(),
  validateSql: vi.fn(),
}));

const fields = [{ name: "population", type: "number" }];

describe("FormulaBuilder", () => {
  it("shows the builder's title and its apply action", () => {
    render(<FormulaBuilder open fields={fields} onApply={() => {}} onClose={() => {}} />);

    expect(screen.getByText("formula_builder")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "apply" })).toBeInTheDocument();
  });

  it("hands the expression back and closes when applied", () => {
    const onApply = vi.fn();
    const onClose = vi.fn();
    render(
      <FormulaBuilder
        open
        fields={fields}
        initialExpression="population * 2"
        onApply={onApply}
        onClose={onClose}
      />
    );

    fireEvent.click(screen.getByRole("button", { name: "apply" }));

    expect(onApply).toHaveBeenCalledWith("population * 2", undefined);
    expect(onClose).toHaveBeenCalled();
  });
});
