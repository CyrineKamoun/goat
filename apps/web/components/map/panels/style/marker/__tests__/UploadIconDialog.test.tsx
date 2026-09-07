import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { UploadIconDialog } from "@/components/map/panels/style/marker/UploadIconDialog";

vi.mock("react-i18next", () => ({
  useTranslation: () => ({ t: (k: string) => k }),
  Trans: ({ i18nKey }: { i18nKey: string }) => <span>{i18nKey}</span>,
}));

describe("UploadIconDialog", () => {
  it("renders the title and refuses the upload until a file is picked", () => {
    render(<UploadIconDialog open onClose={vi.fn()} />);

    expect(screen.getByText("upload_icon", { selector: "div" })).toBeInTheDocument();
    // Read off the DOM property rather than a matcher: the repo's ESLint setup
    // reads jest-dom's `toBeDisabled` as Playwright's (async) matcher.
    const upload = screen.getByRole("button", { name: "upload" }) as HTMLButtonElement;
    expect(upload.disabled).toBe(true);
  });

  it("closes from cancel", () => {
    const onClose = vi.fn();
    render(<UploadIconDialog open onClose={onClose} />);

    fireEvent.click(screen.getByRole("button", { name: "cancel" }));
    expect(onClose).toHaveBeenCalledTimes(1);
  });
});
