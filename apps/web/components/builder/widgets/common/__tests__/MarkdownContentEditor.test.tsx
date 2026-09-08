import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import MarkdownContentEditor from "@/components/builder/widgets/common/MarkdownContentEditor";

vi.mock("react-i18next", () => ({
  useTranslation: () => ({ t: (key: string) => key }),
}));

describe("MarkdownContentEditor", () => {
  it("writes and previews through its own tabs by default", () => {
    render(<MarkdownContentEditor value="**bold**" onChange={vi.fn()} ariaLabel="content" />);

    expect(screen.getByRole("tab", { name: "write" })).toBeInTheDocument();
    fireEvent.click(screen.getByRole("tab", { name: "preview" }));
    expect(screen.getByText("bold").tagName).toBe("STRONG");
  });

  it("drops the tabs with showPreview off, keeping the field and its hint", () => {
    const onChange = vi.fn();
    render(
      <MarkdownContentEditor
        value=""
        onChange={onChange}
        ariaLabel="description"
        videoHint={false}
        showPreview={false}
      />
    );

    expect(screen.queryAllByRole("tab")).toHaveLength(0);
    expect(screen.getByText("markdown_syntax_hint")).toBeInTheDocument();
    expect(screen.queryByText("markdown_video_hint")).not.toBeInTheDocument();

    const field = screen.getByLabelText("description");
    fireEvent.change(field, { target: { value: "## Why" } });
    expect(onChange).toHaveBeenCalledWith("## Why");
    // The house form metric, not the tabbed editor's monospace source view.
    expect(getComputedStyle(field.closest(".MuiInputBase-root") as Element).fontSize).toBe("0.875rem");
  });

  it("keeps plainText a bare textarea with its own hint", () => {
    render(<MarkdownContentEditor value="" onChange={vi.fn()} plainText ariaLabel="tooltip" />);

    expect(screen.queryAllByRole("tab")).toHaveLength(0);
    expect(screen.getByText("tooltip_text_only_hint")).toBeInTheDocument();
  });
});
