import { fireEvent, render, screen } from "@testing-library/react";
import { createRef } from "react";
import { describe, expect, it, vi } from "vitest";

import SearchInput from "@/components/dashboard/common/SearchInput";

vi.mock("react-i18next", () => ({
  useTranslation: () => ({ t: (key: string) => key }),
}));

describe("SearchInput", () => {
  it("renders the placeholder and reports every keystroke", () => {
    const onChange = vi.fn();
    render(<SearchInput value="" onChange={onChange} placeholder="search_items" />);

    const input = screen.getByPlaceholderText("search_items");
    fireEvent.change(input, { target: { value: "bus" } });

    expect(onChange).toHaveBeenCalledWith("bus");
  });

  it("names the field for assistive tech, placeholder by default", () => {
    const { rerender } = render(<SearchInput value="" onChange={vi.fn()} placeholder="search_items" />);
    expect(screen.getByLabelText("search_items")).toBeInTheDocument();

    rerender(
      <SearchInput value="" onChange={vi.fn()} placeholder="search_items" ariaLabel="search_datasets" />
    );
    expect(screen.getByLabelText("search_datasets")).toBeInTheDocument();
  });

  it("shows the clear button only once something is typed, and calls onClear", () => {
    const onClear = vi.fn();
    const { rerender } = render(
      <SearchInput value="" onChange={vi.fn()} onClear={onClear} placeholder="search_items" />
    );

    expect(screen.queryByRole("button", { name: "clear" })).not.toBeInTheDocument();

    rerender(<SearchInput value="bus" onChange={vi.fn()} onClear={onClear} placeholder="search_items" />);
    fireEvent.click(screen.getByRole("button", { name: "clear" }));

    expect(onClear).toHaveBeenCalledTimes(1);
  });

  it("empties the field through onChange when no onClear is given", () => {
    const onChange = vi.fn();
    render(<SearchInput value="bus" onChange={onChange} placeholder="search_items" />);

    fireEvent.click(screen.getByRole("button", { name: "clear" }));

    expect(onChange).toHaveBeenCalledWith("");
  });

  it("forwards keydown to the caller", () => {
    const onKeyDown = vi.fn();
    render(<SearchInput value="" onChange={vi.fn()} placeholder="search_items" onKeyDown={onKeyDown} />);

    fireEvent.keyDown(screen.getByPlaceholderText("search_items"), { key: "ArrowDown" });

    expect(onKeyDown).toHaveBeenCalledTimes(1);
    expect(onKeyDown.mock.calls[0][0]).toMatchObject({ key: "ArrowDown" });
  });

  it("forwards its ref to the input element", () => {
    const ref = createRef<HTMLInputElement>();
    render(<SearchInput ref={ref} value="" onChange={vi.fn()} placeholder="search_items" />);

    expect(ref.current).toBe(screen.getByPlaceholderText("search_items"));
  });

  it("renders the end adornment only while the field is empty", () => {
    const { rerender } = render(
      <SearchInput value="" onChange={vi.fn()} placeholder="search_items" endAdornment={<span>⌘K</span>} />
    );
    expect(screen.getByText("⌘K")).toBeInTheDocument();

    rerender(
      <SearchInput value="bus" onChange={vi.fn()} placeholder="search_items" endAdornment={<span>⌘K</span>} />
    );
    expect(screen.queryByText("⌘K")).not.toBeInTheDocument();
  });

  it("collapsed: starts as an icon button and expands into the field on click", () => {
    render(<SearchInput value="" onChange={vi.fn()} placeholder="search_datasets" collapsible />);

    expect(screen.queryByPlaceholderText("search_datasets")).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "search_datasets" }));

    expect(screen.getByPlaceholderText("search_datasets")).toBeInTheDocument();
  });
});
