import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type { TemplateRead } from "@/lib/validations/template";

import TemplateRow from "@/components/templates/TemplateRow";

vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    t: (key: string, opts?: Record<string, unknown>) => (opts ? `${key}:${JSON.stringify(opts)}` : key),
  }),
}));

const template = (overrides: Partial<TemplateRead>): TemplateRead => ({
  id: "t1",
  name: "Bus network analysis",
  description: null,
  categories: [],
  thumbnail_url: null,
  space_id: "s1",
  folder_id: "f1",
  created_by: null,
  payload_kind: "workflow",
  kinds: ["workflow"],
  inputs: [],
  ships_sample_data: false,
  catalog_status: "published",
  source_ref: {},
  datasets_needing_share: [],
  my_role: "viewer",
  created_at: new Date().toISOString(),
  updated_at: new Date().toISOString(),
  ...overrides,
});

const renderRow = (
  overrides: Partial<TemplateRead>,
  props: { selected?: boolean; sourceLabel?: string } = {}
) =>
  render(
    <TemplateRow
      template={template(overrides)}
      selected={props.selected ?? false}
      onSelect={vi.fn()}
      sourceLabel={props.sourceLabel}
    />
  );

describe("TemplateRow", () => {
  it("shows the template's own thumbnail when it has one", () => {
    const { container } = renderRow({ thumbnail_url: "https://example.test/thumb.png" });

    expect(container.querySelector("img")?.getAttribute("src")).toBe("https://example.test/thumb.png");
    // The picture is the image, so nothing is drawn behind it.
    expect(screen.queryByTestId("template-default-workflow")).not.toBeInTheDocument();
    expect(screen.queryByTestId("template-preview-page")).not.toBeInTheDocument();
    expect(screen.queryByTestId("content-thumbnail-mark")).not.toBeInTheDocument();
  });

  it("draws the payload's own default at the row's size when there is no picture", () => {
    const { container, unmount } = renderRow({ thumbnail_url: null, payload_kind: "workflow" });

    // A workflow reads as a chain of steps, the same drawing the card shows.
    expect(screen.getByTestId("template-default-workflow")).toBeInTheDocument();
    expect(container.querySelector("img")).toBeNull();
    unmount();

    // A layout reads as the page it prints on, in that page's orientation.
    renderRow({
      thumbnail_url: null,
      payload_kind: "layout",
      kinds: ["layout"],
      page_size: "A4",
      page_orientation: "landscape",
    });

    expect(screen.getByTestId("template-preview-page").getAttribute("data-orientation")).toBe("landscape");
  });

  it("keeps the template mark for a project payload, which draws no default of its own", () => {
    renderRow({ thumbnail_url: null, payload_kind: "project", kinds: [] });

    expect(screen.queryByTestId("template-default-workflow")).not.toBeInTheDocument();
    expect(screen.queryByTestId("template-preview-page")).not.toBeInTheDocument();
    expect(screen.getByText("Bus network analysis")).toBeInTheDocument();
  });

  it("keeps the name, the page tag, the shelf tag and the selected state around the tile", () => {
    renderRow(
      {
        thumbnail_url: null,
        payload_kind: "layout",
        kinds: ["layout"],
        page_size: "A3",
        page_orientation: "portrait",
        description: "**Counts** the buses",
      },
      { selected: true, sourceLabel: "source_goat" }
    );

    const option = screen.getByRole("option");
    expect(option.getAttribute("aria-selected")).toBe("true");
    expect(screen.getByText(/^A3 · portrait/)).toBeInTheDocument();
    expect(screen.getByText("source_goat")).toBeInTheDocument();
    // The description reads as text, with its markdown marks removed.
    expect(screen.getByText("Counts the buses")).toBeInTheDocument();
  });

  it("selects the template on click", () => {
    const onSelect = vi.fn();
    render(<TemplateRow template={template({})} selected={false} onSelect={onSelect} />);

    fireEvent.click(screen.getByRole("option"));

    expect(onSelect).toHaveBeenCalled();
  });
});
