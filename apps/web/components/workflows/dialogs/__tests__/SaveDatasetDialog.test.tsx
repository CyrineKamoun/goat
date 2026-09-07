import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import SaveDatasetDialog from "@/components/workflows/dialogs/SaveDatasetDialog";

vi.mock("react-i18next", () => ({ useTranslation: () => ({ t: (k: string) => k }) }));

describe("SaveDatasetDialog", () => {
  it("renders the title and refuses to save an empty name", () => {
    render(<SaveDatasetDialog open onClose={vi.fn()} onSave={vi.fn()} />);

    expect(screen.getByText("save_dataset")).toBeInTheDocument();
    // Read off the DOM property rather than a matcher: the repo's ESLint setup
    // reads jest-dom's `toBeDisabled` as Playwright's (async) matcher.
    const save = screen.getByRole("button", { name: "save" }) as HTMLButtonElement;
    expect(save.disabled).toBe(true);
  });

  it("saves the trimmed name, from the button and from Enter in the field", async () => {
    const onSave = vi.fn().mockResolvedValue(undefined);
    render(<SaveDatasetDialog open onClose={vi.fn()} onSave={onSave} defaultName="  roads  " />);

    fireEvent.click(screen.getByRole("button", { name: "save" }));
    await waitFor(() => expect(onSave).toHaveBeenCalledWith("roads"));

    onSave.mockClear();
    fireEvent.keyDown(screen.getByPlaceholderText("dataset_name"), { key: "Enter" });
    await waitFor(() => expect(onSave).toHaveBeenCalledWith("roads"));
  });
});
