import type { ComponentProps } from "react";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    t: (key: string, vars?: Record<string, unknown>) => (vars ? `${key}:${JSON.stringify(vars)}` : key),
  }),
}));

import ContentToolbar from "@/components/dashboard/content/ContentToolbar";

const noop = () => {};

const renderToolbar = (overrides: Partial<ComponentProps<typeof ContentToolbar>> = {}) =>
  render(
    <ContentToolbar
      search=""
      onSearch={noop}
      layout="grid"
      onLayout={noop}
      types={[]}
      onTypes={noop}
      orderBy="updated_at"
      order="descendent"
      onSort={noop}
      detailsOpen={false}
      onToggleDetails={noop}
      addMenu={<button aria-label="add_new">+</button>}
      canAdd
      {...overrides}
    />
  );

describe("ContentToolbar mobile layout", () => {
  it("exposes an aria-label for every icon-only control", () => {
    renderToolbar({ mobile: true });

    expect(screen.getByRole("button", { name: "grid_view" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "list_view" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "filter" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "sort" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "details" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "add_new" })).toBeInTheDocument();
  });

  it("renders no visible text label for the icon-only controls", () => {
    renderToolbar({ mobile: true });

    expect(screen.queryByText("grid_view")).not.toBeInTheDocument();
    expect(screen.queryByText("list_view")).not.toBeInTheDocument();
    expect(screen.queryByText("filter")).not.toBeInTheDocument();
    expect(screen.queryByText("sort")).not.toBeInTheDocument();
    expect(screen.queryByText("last_updated")).not.toBeInTheDocument();
    expect(screen.queryByText("details")).not.toBeInTheDocument();
    expect(screen.queryByText("add_new")).not.toBeInTheDocument();
  });

  it("still exposes the same aria-labelled controls on desktop, unchanged", () => {
    renderToolbar({ mobile: false });

    expect(screen.getByRole("button", { name: "grid_view" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "filter" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "details" })).toBeInTheDocument();
  });
});

// Sort is a pill of its own again, opening a menu — the same control the
// catalog's toolbar carries. Desktop names the pill after the option in force,
// so that is what opens it.
describe("ContentToolbar sort", () => {
  it("offers every sort option in the sort menu", async () => {
    renderToolbar();

    await userEvent.click(screen.getByRole("button", { name: "last_updated" }));

    expect(screen.getByRole("menuitem", { name: "last_updated" })).toBeInTheDocument();
    expect(screen.getByRole("menuitem", { name: "last_created" })).toBeInTheDocument();
    expect(screen.getByRole("menuitem", { name: "name" })).toBeInTheDocument();
  });

  it("reports the picked option through onSort", async () => {
    const onSort = vi.fn();
    renderToolbar({ onSort });

    await userEvent.click(screen.getByRole("button", { name: "last_updated" }));
    await userEvent.click(screen.getByRole("menuitem", { name: "name" }));

    expect(onSort).toHaveBeenCalledWith("name", "ascendent");
  });

  it("keeps the filter popover to the type checkboxes", async () => {
    renderToolbar();

    await userEvent.click(screen.getByRole("button", { name: "filter" }));

    expect(screen.getByRole("checkbox", { name: "projects" })).toBeInTheDocument();
    expect(screen.queryByRole("radio", { name: "last_updated" })).not.toBeInTheDocument();
  });
});
