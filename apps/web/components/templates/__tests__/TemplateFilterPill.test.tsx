import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type { Space } from "@/lib/validations/content";

import TemplateFilterPill from "@/components/templates/TemplateFilterPill";

vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    t: (key: string, opts?: Record<string, unknown>) => (opts ? `${key}:${JSON.stringify(opts)}` : key),
  }),
}));

const personalSpace: Space = {
  id: "s1",
  kind: "personal",
  name: "My Content",
  default_role: "viewer",
  my_role: "owner",
  team_id: null,
  organization_id: null,
};

const openPill = (props: Partial<React.ComponentProps<typeof TemplateFilterPill>> = {}) => {
  render(
    <TemplateFilterPill
      source="all"
      onSource={vi.fn()}
      spaces={[personalSpace]}
      tags={[]}
      availableTags={[{ tag: "Transport", count: 3 }]}
      onToggleTag={vi.fn()}
      activeFilterCount={0}
      onClear={vi.fn()}
      {...props}
    />
  );
  fireEvent.click(screen.getByRole("button", { name: "filter" }));
};

describe("TemplateFilterPill", () => {
  it("offers the source and the categories", () => {
    openPill();

    expect(screen.getByText("source")).toBeInTheDocument();
    expect(screen.getByRole("radio", { name: "source_goat" })).toBeInTheDocument();
    expect(screen.getByRole("checkbox", { name: "Transport" })).toBeInTheDocument();
  });

  it("drops the source section where the shelf is fixed, keeping the categories", () => {
    openPill({ hideSource: true });

    expect(screen.queryByText("source")).not.toBeInTheDocument();
    for (const name of ["source_everyone", "source_goat", "source_mine"]) {
      expect(screen.queryByRole("radio", { name })).not.toBeInTheDocument();
    }
    expect(screen.getByText("categories")).toBeInTheDocument();
    expect(screen.getByRole("checkbox", { name: "Transport" })).toBeInTheDocument();
  });

  it("keeps the clear link reachable with the source section gone", () => {
    const onClear = vi.fn();

    openPill({ hideSource: true, activeFilterCount: 1, tags: ["Transport"], onClear });
    fireEvent.click(screen.getByText("clear_all"));

    expect(onClear).toHaveBeenCalled();
  });
});
