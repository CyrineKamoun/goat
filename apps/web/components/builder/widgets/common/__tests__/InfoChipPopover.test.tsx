import { fireEvent, render, screen } from "@testing-library/react";
import type { Editor } from "@tiptap/react";
import { describe, expect, it, vi } from "vitest";

import { InfoChipEditDialog } from "@/components/builder/widgets/common/InfoChipPopover";

vi.mock("react-i18next", () => ({ useTranslation: () => ({ t: (k: string) => k }) }));
vi.mock("@/components/builder/widgets/common/MarkdownContentEditor", () => ({
  default: () => <div>markdown editor</div>,
}));
vi.mock("@/components/builder/widgets/common/PopupSettingsControls", () => ({
  default: () => <div>popup settings</div>,
}));

// The chip the dialog edits: the dialog snapshots its position and attrs when it
// opens, and writes them back through the editor's command chain on close.
const chipEditor = {
  state: {
    selection: { from: 3 },
    doc: { nodeAt: () => ({ type: { name: "infoChip" }, attrs: { text: "note", title: "Roads" } }) },
  },
  chain: () => ({ command: () => ({ run: () => undefined }) }),
} as unknown as Editor;

describe("InfoChipEditDialog", () => {
  it("renders the title, the chip's fields and the Done action", () => {
    render(<InfoChipEditDialog editor={chipEditor} open onClose={vi.fn()} />);

    expect(screen.getByText("edit_popup_content")).toBeInTheDocument();
    // Read off the DOM property rather than a matcher: the repo's ESLint setup
    // reads jest-dom's `toHaveValue` as Playwright's (async) matcher.
    expect((screen.getByTestId("infochip-title-input") as HTMLInputElement).value).toBe("Roads");
    expect(screen.getByRole("button", { name: "done" })).toBeInTheDocument();
  });

  it("closes on Done", () => {
    const onClose = vi.fn();
    render(<InfoChipEditDialog editor={chipEditor} open onClose={onClose} />);

    fireEvent.click(screen.getByRole("button", { name: "done" }));
    expect(onClose).toHaveBeenCalledTimes(1);
  });
});
