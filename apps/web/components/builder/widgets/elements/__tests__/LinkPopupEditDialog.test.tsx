import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import LinkPopupEditDialog from "@/components/builder/widgets/elements/LinkPopupEditDialog";

vi.mock("react-i18next", () => ({ useTranslation: () => ({ t: (k: string) => k }) }));
vi.mock("@/components/builder/widgets/common/MarkdownContentEditor", () => ({
  default: () => <div>markdown editor</div>,
}));
vi.mock("@/components/builder/widgets/common/PopupSettingsControls", () => ({
  default: () => <div>popup settings</div>,
}));

describe("LinkPopupEditDialog", () => {
  it("renders the title, the content editor and the Done action", () => {
    render(<LinkPopupEditDialog open onClose={vi.fn()} onSave={vi.fn()} initial={{}} />);

    expect(screen.getByText("edit_popup_content")).toBeInTheDocument();
    expect(screen.getByText("markdown editor")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "done" })).toBeInTheDocument();
  });

  it("commits the edited values on Done", () => {
    const onSave = vi.fn();
    const onClose = vi.fn();
    render(
      <LinkPopupEditDialog
        open
        onClose={onClose}
        onSave={onSave}
        initial={{ popup_content: "hello", popup_type: "dialog" }}
      />
    );

    fireEvent.click(screen.getByRole("button", { name: "done" }));
    expect(onSave).toHaveBeenCalledWith(
      expect.objectContaining({ popup_content: "hello", popup_type: "dialog" })
    );
    expect(onClose).toHaveBeenCalledTimes(1);
  });
});
