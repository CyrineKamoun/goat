import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type { Space } from "@/lib/validations/content";

import ContentSpacesPanel from "@/components/dashboard/content/ContentSpacesPanel";

vi.mock("react-i18next", () => ({
  useTranslation: () => ({ t: (key: string) => key }),
}));

const space = (overrides: Partial<Space>): Space =>
  ({
    id: "00000000-0000-0000-0000-000000000001",
    kind: "team",
    name: "Planning",
    default_role: "viewer",
    my_role: "editor",
    ...overrides,
  }) as Space;

const spaces = [
  space({ id: "00000000-0000-0000-0000-00000000000a", kind: "personal", name: "Personal" }),
  space({ id: "00000000-0000-0000-0000-00000000000b", name: "Planning" }),
  space({ id: "00000000-0000-0000-0000-00000000000c", name: "Mobility" }),
];

const panel = (props: Partial<React.ComponentProps<typeof ContentSpacesPanel>> = {}) => (
  <ContentSpacesPanel
    spaces={spaces}
    active={{ kind: "view", view: "recent" }}
    onSelectSpace={() => {}}
    onSelectView={() => {}}
    {...props}
  />
);

describe("ContentSpacesPanel", () => {
  it("claims no team count while the spaces are still being fetched", () => {
    const { container } = render(panel({ spaces: [], loading: true, sharedCount: 3 }));

    expect(screen.getByText("team_spaces")).toBeInTheDocument();
    expect(screen.queryByText("0")).not.toBeInTheDocument();
    expect(screen.queryByText("3")).not.toBeInTheDocument();
    expect(container.querySelectorAll(".MuiSkeleton-root")).toHaveLength(3);
  });

  it("keeps the personal row while loading, since everyone has one", () => {
    render(panel({ spaces: [], loading: true }));

    const row = screen.getByText("my_content").closest("div[role='button']");
    expect(row).not.toBeNull();
    expect(row?.className).toContain("Mui-disabled");
    expect(screen.queryByText("organization")).not.toBeInTheDocument();
  });

  it("keeps its card chrome by default and drops it when flush", () => {
    const paperOf = (element: HTMLElement) => element.querySelector(".MuiPaper-root") as HTMLElement;

    const { container: page } = render(panel());
    expect(getComputedStyle(paperOf(page)).borderStyle).toBe("solid");
    expect(getComputedStyle(paperOf(page)).borderWidth).toBe("1px");

    const { container: rail } = render(panel({ flush: true }));
    // The host draws the single rule beside a flush rail.
    expect(getComputedStyle(paperOf(rail)).borderStyle).toBe("none");
    expect(getComputedStyle(paperOf(rail)).boxShadow).toBe("none");
  });

  it("lists the team spaces and their count once they are there", () => {
    const { container } = render(panel({ sharedCount: 3 }));

    expect(screen.getByText("Planning")).toBeInTheDocument();
    expect(screen.getByText("Mobility")).toBeInTheDocument();
    expect(screen.getByText("2")).toBeInTheDocument();
    expect(container.querySelectorAll(".MuiSkeleton-root")).toHaveLength(0);
  });
});
