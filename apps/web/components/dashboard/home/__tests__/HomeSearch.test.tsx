import { fireEvent, render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { ICON_NAME } from "@p4b/ui/components/Icon";

import HomeSearch from "@/components/dashboard/home/HomeSearch";

vi.mock("react-i18next", () => ({
  useTranslation: () => ({ t: (key: string) => key }),
}));

const goMock = vi.fn();
const closePreviewMock = vi.fn();
const useHomeSearchMock = vi.fn();
vi.mock("@/components/dashboard/content/ContentPreviewDialog", () => ({
  default: ({ layerId, onClose }: { layerId: string; onClose: () => void }) => (
    <button type="button" data-testid="preview-dialog" onClick={onClose}>
      preview {layerId}
    </button>
  ),
}));
vi.mock("@/hooks/dashboard/home/useHomeSearch", async () => {
  const actual = await vi.importActual<typeof import("@/hooks/dashboard/home/useHomeSearch")>(
    "@/hooks/dashboard/home/useHomeSearch"
  );
  return { ...actual, useHomeSearch: (raw: string) => useHomeSearchMock(raw) };
});

const placeholder = "search_placeholder_short";

describe("HomeSearch", () => {
  beforeEach(() => {
    goMock.mockReset();
    useHomeSearchMock.mockReset();
    useHomeSearchMock.mockReturnValue({
      rows: [
        { kind: "head", label: "projects" },
        { kind: "item", id: "p1", icon: ICON_NAME.MAP, label: "Bus network", meta: "project", go: goMock },
        {
          kind: "item",
          id: "l1",
          icon: ICON_NAME.MAP,
          label: "Bus stops",
          meta: "feature_layer",
          go: goMock,
        },
      ],
      isLoading: false,
    });
  });

  it("focuses the input on Ctrl+K from anywhere on the page", () => {
    render(<HomeSearch />);

    fireEvent.keyDown(window, { key: "k", ctrlKey: true });

    expect(document.activeElement).toBe(screen.getByPlaceholderText(placeholder));
  });

  it("ArrowDown selects the first item row, Enter runs its go", () => {
    render(<HomeSearch />);
    const input = screen.getByPlaceholderText(placeholder);
    fireEvent.focus(input);
    fireEvent.change(input, { target: { value: "bus" } });

    fireEvent.keyDown(input, { key: "ArrowDown" });
    fireEvent.keyDown(input, { key: "Enter" });

    expect(goMock).toHaveBeenCalledTimes(1);
  });

  it("ArrowDown wraps from the last item row back to the first", () => {
    render(<HomeSearch />);
    const input = screen.getByPlaceholderText(placeholder);
    fireEvent.focus(input);
    fireEvent.change(input, { target: { value: "bus" } });

    fireEvent.keyDown(input, { key: "ArrowUp" });
    fireEvent.keyDown(input, { key: "Enter" });

    expect(goMock).toHaveBeenCalledTimes(1);
  });

  it("Escape clears the query and blurs the input", () => {
    render(<HomeSearch />);
    const input = screen.getByPlaceholderText(placeholder) as HTMLInputElement;
    fireEvent.focus(input);
    fireEvent.change(input, { target: { value: "bus" } });

    fireEvent.keyDown(input, { key: "Escape" });

    expect(input.value).toBe("");
    expect(document.activeElement).not.toBe(input);
  });

  it("clicking a result row runs its go", () => {
    render(<HomeSearch />);
    const input = screen.getByPlaceholderText(placeholder);
    fireEvent.focus(input);
    fireEvent.change(input, { target: { value: "bus" } });

    // The label is split around the highlight mark, so the row is the target.
    fireEvent.click(screen.getAllByRole("option")[0]);

    expect(goMock).toHaveBeenCalledTimes(1);
  });

  it("renders one group header per section and no tips while a query is typed", () => {
    useHomeSearchMock.mockReturnValue({
      rows: [
        { kind: "head", label: "projects" },
        { kind: "item", id: "p1", icon: ICON_NAME.MAP, label: "Bus network", meta: "project", go: goMock },
        { kind: "head", label: "catalog" },
        { kind: "item", id: "c1", icon: ICON_NAME.GLOBE, label: "Bus routes", meta: "", go: goMock },
      ],
      isLoading: false,
    });
    render(<HomeSearch />);
    const input = screen.getByPlaceholderText(placeholder);
    fireEvent.focus(input);
    fireEvent.change(input, { target: { value: "bus" } });

    expect(screen.getByText("projects")).toBeInTheDocument();
    expect(screen.getByText("catalog")).toBeInTheDocument();
    expect(screen.queryByText("search_tips")).not.toBeInTheDocument();
    expect(screen.queryByText("project:")).not.toBeInTheDocument();
  });

  it("never shows a group header the hook left empty", () => {
    useHomeSearchMock.mockReturnValue({ rows: [], isLoading: false });
    render(<HomeSearch />);
    fireEvent.focus(screen.getByPlaceholderText(placeholder));

    expect(screen.queryByText("recent")).not.toBeInTheDocument();
  });

  it("lists the scope prefixes as tips for the empty query", () => {
    useHomeSearchMock.mockReturnValue({ rows: [], isLoading: false });
    render(<HomeSearch />);
    fireEvent.focus(screen.getByPlaceholderText(placeholder));

    expect(screen.getByText("search_tips")).toBeInTheDocument();
    expect(screen.getByText("project:")).toBeInTheDocument();
    expect(screen.getByText("dataset:")).toBeInTheDocument();
    expect(screen.getByText("catalog:")).toBeInTheDocument();
    expect(screen.getByText("— search_scope_projects")).toBeInTheDocument();
  });

  it("clicking a tip types its prefix into the field instead of navigating", () => {
    useHomeSearchMock.mockReturnValue({ rows: [], isLoading: false });
    render(<HomeSearch />);
    const input = screen.getByPlaceholderText(placeholder) as HTMLInputElement;
    fireEvent.focus(input);

    fireEvent.click(screen.getByText("catalog:"));

    expect(input.value).toBe("catalog: ");
    expect(document.activeElement).toBe(input);
    expect(goMock).not.toHaveBeenCalled();
  });

  it("Enter on a tip types its prefix too", () => {
    useHomeSearchMock.mockReturnValue({ rows: [], isLoading: false });
    render(<HomeSearch />);
    const input = screen.getByPlaceholderText(placeholder) as HTMLInputElement;
    fireEvent.focus(input);

    fireEvent.keyDown(input, { key: "ArrowDown" });
    fireEvent.keyDown(input, { key: "Enter" });

    expect(input.value).toBe("dataset: ");
  });

  describe("with only a scope prefix in the field", () => {
    /** The rows the hook answers a bare `catalog:` with. */
    const scopedRows = [
      { kind: "head", label: "catalog" },
      { kind: "note", label: "search_type_to_search_catalog" },
    ];

    /** What the tip row for a prefix reports as its selected state. */
    const tipSelected = (prefix: string) =>
      screen.getByText(prefix).closest('[role="option"]')?.getAttribute("aria-selected");

    const typePrefix = (prefix: string) => {
      useHomeSearchMock.mockReturnValue({ rows: scopedRows, isLoading: false });
      render(<HomeSearch />);
      const input = screen.getByPlaceholderText(placeholder) as HTMLInputElement;
      fireEvent.focus(input);
      fireEvent.change(input, { target: { value: prefix } });
      return input;
    };

    it("shows the scoped group header and its caption instead of an empty list", () => {
      typePrefix("catalog:");

      expect(screen.getByText("catalog")).toBeInTheDocument();
      expect(screen.getByText("search_type_to_search_catalog")).toBeInTheDocument();
      expect(screen.queryByText("no_results_for")).not.toBeInTheDocument();
    });

    it("leaves the caption unselectable", () => {
      typePrefix("catalog:");

      expect(screen.getByText("search_type_to_search_catalog").closest('[role="option"]')).toBeNull();
      // The three tips are the only rows Enter can land on.
      expect(screen.getAllByRole("option")).toHaveLength(3);
    });

    it("keeps the tips up with the active prefix's row marked and the others normal", () => {
      typePrefix("catalog:");

      expect(screen.getByText("search_tips")).toBeInTheDocument();
      expect(tipSelected("catalog:")).toBe("true");
      expect(tipSelected("project:")).toBe("false");
      expect(tipSelected("dataset:")).toBe("false");
    });

    it("marks the tip that matches the prefix, whichever it is", () => {
      typePrefix("project: ");

      expect(tipSelected("project:")).toBe("true");
      expect(tipSelected("catalog:")).toBe("false");
    });

    it("picking another tip replaces the prefix in the field", () => {
      const input = typePrefix("catalog:");

      fireEvent.click(screen.getByText("project:"));

      expect(input.value).toBe("project: ");
      expect(document.activeElement).toBe(input);
    });

    it("keeps the keyboard footer", () => {
      typePrefix("catalog:");

      expect(screen.getByText("kbd_to_navigate")).toBeInTheDocument();
    });

    it("still shows the no-results row once a scoped query has actually run", () => {
      useHomeSearchMock.mockReturnValue({ rows: [], isLoading: false });
      render(<HomeSearch />);
      const input = screen.getByPlaceholderText(placeholder);
      fireEvent.focus(input);
      fireEvent.change(input, { target: { value: "catalog: bus" } });

      expect(screen.getByText("no_results_for")).toBeInTheDocument();
      expect(screen.queryByText("search_tips")).not.toBeInTheDocument();
    });
  });

  it("marks the matched part of a result label", () => {
    render(<HomeSearch />);
    const input = screen.getByPlaceholderText(placeholder);
    fireEvent.focus(input);
    fireEvent.change(input, { target: { value: "bus" } });

    const marks = Array.from(document.querySelectorAll("mark")).map((node) => node.textContent);
    expect(marks).toContain("Bus");
  });

  it("does not mark anything for the scope prefix itself", () => {
    render(<HomeSearch />);
    const input = screen.getByPlaceholderText(placeholder);
    fireEvent.focus(input);
    fireEvent.change(input, { target: { value: "project: bus" } });

    const marks = Array.from(document.querySelectorAll("mark")).map((node) => node.textContent);
    expect(marks).toEqual(["Bus", "Bus"]);
  });

  it("shows the keyboard hint footer whenever the dropdown is open", () => {
    render(<HomeSearch />);
    fireEvent.focus(screen.getByPlaceholderText(placeholder));

    expect(screen.getByText("kbd_to_navigate")).toBeInTheDocument();
    expect(screen.getByText("kbd_to_select")).toBeInTheDocument();
    expect(screen.getByText("kbd_to_close")).toBeInTheDocument();
  });

  it("passes the raw query through untouched — the hook does the parsing", () => {
    render(<HomeSearch />);
    const input = screen.getByPlaceholderText(placeholder);

    fireEvent.change(input, { target: { value: "project: bus" } });

    expect(useHomeSearchMock).toHaveBeenLastCalledWith("project: bus");
  });

  it("shows the ⌘K hint with no query and swaps it for a clear button once typing", () => {
    render(<HomeSearch />);
    const input = screen.getByPlaceholderText(placeholder);

    expect(screen.getByText("⌘K")).toBeInTheDocument();

    fireEvent.change(input, { target: { value: "bus" } });

    expect(screen.queryByText("⌘K")).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "clear" })).toBeInTheDocument();
  });

  it("opens the layer preview the hook asked for and closes it through the hook", async () => {
    useHomeSearchMock.mockReturnValue({
      rows: [],
      isLoading: false,
      previewLayerId: "l1",
      closePreview: closePreviewMock,
    });
    render(<HomeSearch />);

    // The dialog is a lazy `next/dynamic` chunk, so it lands a tick later.
    fireEvent.click(await screen.findByTestId("preview-dialog"));

    expect(screen.getByText("preview l1")).toBeInTheDocument();
    expect(closePreviewMock).toHaveBeenCalled();
  });

  it("renders no preview while the hook has no layer to show", () => {
    useHomeSearchMock.mockReturnValue({
      rows: [],
      isLoading: false,
      previewLayerId: null,
      closePreview: vi.fn(),
    });
    render(<HomeSearch />);

    expect(screen.queryByTestId("preview-dialog")).not.toBeInTheDocument();
  });
});
