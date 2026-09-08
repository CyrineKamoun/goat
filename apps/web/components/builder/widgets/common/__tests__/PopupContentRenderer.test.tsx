import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import PopupContentRenderer from "@/components/builder/widgets/common/PopupContentRenderer";

vi.mock("react-i18next", () => ({ useTranslation: () => ({ t: (k: string) => k }) }));

describe("PopupContentRenderer (dialog type)", () => {
  it("renders the title and the markdown content", () => {
    render(
      <PopupContentRenderer
        open
        onClose={vi.fn()}
        popup_type="dialog"
        placement="auto"
        anchorEl={null}
        title="About this map"
        content="**bold** body"
      />
    );

    expect(screen.getByText("About this map")).toBeInTheDocument();
    expect(screen.getByText("bold")).toBeInTheDocument();
  });

  it("closes from the header close button", () => {
    const onClose = vi.fn();
    render(
      <PopupContentRenderer
        open
        onClose={onClose}
        popup_type="dialog"
        placement="auto"
        anchorEl={null}
        title="About this map"
        content="body"
      />
    );

    fireEvent.click(screen.getByRole("button", { name: "close" }));
    expect(onClose).toHaveBeenCalledTimes(1);
  });
});
