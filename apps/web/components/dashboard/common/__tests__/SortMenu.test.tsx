import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

vi.mock("react-i18next", () => ({
  useTranslation: () => ({ t: (key: string) => key }),
}));

import SortMenu from "@/components/dashboard/common/SortMenu";

const options = [
  { value: "updated", label: "Last updated" },
  { value: "name", label: "Name" },
];

describe("SortMenu", () => {
  it("names the pill after the option in force", () => {
    render(<SortMenu value="name" options={options} onChange={() => {}} label="Sort" />);

    expect(screen.getByRole("button", { name: "Name" })).toBeInTheDocument();
  });

  it("falls back to the first option when the value matches none", () => {
    render(<SortMenu value="gone" options={options} onChange={() => {}} label="Sort" />);

    expect(screen.getByRole("button", { name: "Last updated" })).toBeInTheDocument();
  });

  it("keeps only the sort label when compact", () => {
    render(<SortMenu value="name" options={options} onChange={() => {}} label="Sort" compact />);

    expect(screen.getByRole("button", { name: "Sort" })).toBeInTheDocument();
    expect(screen.queryByText("Name")).not.toBeInTheDocument();
  });

  it("reports the option picked from the menu", async () => {
    const onChange = vi.fn();
    render(<SortMenu value="updated" options={options} onChange={onChange} label="Sort" />);

    await userEvent.click(screen.getByRole("button", { name: "Last updated" }));
    await userEvent.click(screen.getByRole("menuitem", { name: "Name" }));

    expect(onChange).toHaveBeenCalledWith("name");
  });
});
