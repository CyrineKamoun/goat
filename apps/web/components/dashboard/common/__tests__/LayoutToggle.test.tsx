import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

vi.mock("react-i18next", () => ({
  useTranslation: () => ({ t: (key: string) => key }),
}));

import LayoutToggle from "@/components/dashboard/common/LayoutToggle";

describe("LayoutToggle", () => {
  it("labels both segments and marks neither with the other's name", () => {
    render(<LayoutToggle value="grid" onChange={() => {}} />);

    expect(screen.getByRole("button", { name: "grid_view" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "list_view" })).toBeInTheDocument();
    expect(screen.getByText("grid")).toBeInTheDocument();
    expect(screen.getByText("list")).toBeInTheDocument();
  });

  it("drops the visible labels when compact, keeping the accessible names", () => {
    render(<LayoutToggle value="grid" onChange={() => {}} compact />);

    expect(screen.getByRole("button", { name: "grid_view" })).toBeInTheDocument();
    expect(screen.queryByText("grid")).not.toBeInTheDocument();
    expect(screen.queryByText("list")).not.toBeInTheDocument();
  });

  it("reports the segment that was clicked", async () => {
    const onChange = vi.fn();
    render(<LayoutToggle value="grid" onChange={onChange} />);

    await userEvent.click(screen.getByRole("button", { name: "list_view" }));

    expect(onChange).toHaveBeenCalledWith("list");
  });
});
