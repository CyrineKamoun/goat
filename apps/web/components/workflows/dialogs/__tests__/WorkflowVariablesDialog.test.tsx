import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import WorkflowVariablesDialog from "@/components/workflows/dialogs/WorkflowVariablesDialog";

vi.mock("react-i18next", () => ({ useTranslation: () => ({ t: (k: string) => k }) }));

const dispatch = vi.fn();
// One frozen array, not a fresh one per call: the dialog re-seeds its local copy
// whenever the selected variables change identity, so a new array each render
// would loop forever.
const storedVariables: unknown[] = [];
vi.mock("react-redux", () => ({
  useDispatch: () => dispatch,
  useSelector: () => storedVariables,
}));

describe("WorkflowVariablesDialog", () => {
  it("renders the title, the empty state and the Done action", () => {
    render(<WorkflowVariablesDialog open onClose={vi.fn()} />);

    expect(screen.getByText("workflow_variables")).toBeInTheDocument();
    expect(screen.getByText("workflow_variable_none_defined")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "done" })).toBeInTheDocument();
  });

  it("keeps the dialog open when a new variable has no name yet", () => {
    const onClose = vi.fn();
    render(<WorkflowVariablesDialog open onClose={onClose} />);

    fireEvent.click(screen.getByRole("button", { name: "workflow_variable_add" }));
    fireEvent.click(screen.getByRole("button", { name: "done" }));
    expect(onClose).not.toHaveBeenCalled();
    expect(screen.getByText("workflow_variable_name_required")).toBeInTheDocument();
  });
});
