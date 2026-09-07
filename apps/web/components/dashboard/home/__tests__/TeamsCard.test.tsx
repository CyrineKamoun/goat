import { fireEvent, render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type { Space } from "@/lib/validations/content";
import type { Team } from "@/lib/validations/team";

import TeamsCard from "@/components/dashboard/home/TeamsCard";

const { useTeamsMock, useSpacesMock, pushMock } = vi.hoisted(() => ({
  useTeamsMock: vi.fn(),
  useSpacesMock: vi.fn(),
  pushMock: vi.fn(),
}));

vi.mock("react-i18next", () => ({
  useTranslation: () => ({ t: (key: string) => key }),
}));
vi.mock("@/lib/api/teams", () => ({ useTeams: (...args: unknown[]) => useTeamsMock(...args) }));
vi.mock("@/lib/api/content", () => ({ useSpaces: () => useSpacesMock() }));
vi.mock("next/navigation", () => ({ useRouter: () => ({ push: pushMock }) }));

const team = (overrides: Partial<Team>): Team => ({
  id: "team-1",
  name: "Design QA",
  role: "team-member",
  ...overrides,
});

const teamSpace = (overrides: Partial<Space>): Space => ({
  id: "space-1",
  kind: "team",
  name: "Design QA",
  default_role: "viewer",
  my_role: "editor",
  team_id: "team-1",
  organization_id: null,
  ...overrides,
});

beforeEach(() => {
  useTeamsMock.mockReset();
  useSpacesMock.mockReset();
  pushMock.mockReset();
});

describe("TeamsCard", () => {
  it("renders nothing when the caller has no teams", () => {
    useTeamsMock.mockReturnValue({ teams: [] });
    useSpacesMock.mockReturnValue({ spaces: [] });

    const { container } = render(<TeamsCard />);

    expect(container).toBeEmptyDOMElement();
  });

  it("lists each of the caller's teams by name", () => {
    useTeamsMock.mockReturnValue({ teams: [team({ id: "team-1", name: "Design QA" })] });
    useSpacesMock.mockReturnValue({ spaces: [teamSpace({})] });

    render(<TeamsCard />);

    expect(screen.getByText("Design QA")).toBeInTheDocument();
  });

  it("renders nothing when the caller has teams but none of them resolve to a space", () => {
    useTeamsMock.mockReturnValue({
      teams: [team({ id: "team-1", name: "Orphan Team" })],
    });
    useSpacesMock.mockReturnValue({ spaces: [] });

    const { container } = render(<TeamsCard />);

    expect(container).toBeEmptyDOMElement();
  });

  it("skips a team whose space cannot be found", () => {
    useTeamsMock.mockReturnValue({
      teams: [team({ id: "team-1", name: "Design QA" }), team({ id: "team-2", name: "Orphan Team" })],
    });
    useSpacesMock.mockReturnValue({ spaces: [teamSpace({ team_id: "team-1" })] });

    render(<TeamsCard />);

    expect(screen.getByText("Design QA")).toBeInTheDocument();
    expect(screen.queryByText("Orphan Team")).not.toBeInTheDocument();
  });

  it("opens a team's space on Content when its row is clicked", () => {
    useTeamsMock.mockReturnValue({ teams: [team({ id: "team-1", name: "Design QA" })] });
    useSpacesMock.mockReturnValue({ spaces: [teamSpace({ id: "space-1", team_id: "team-1" })] });

    render(<TeamsCard />);
    fireEvent.click(screen.getByText("Design QA"));

    expect(pushMock).toHaveBeenCalledWith("/content/space-1");
  });

  it("routes all_teams to team settings and shows the team count", () => {
    useTeamsMock.mockReturnValue({
      teams: [team({ id: "team-1", name: "Design QA" }), team({ id: "team-2", name: "Mapping" })],
    });
    useSpacesMock.mockReturnValue({
      spaces: [
        teamSpace({ id: "space-1", team_id: "team-1" }),
        teamSpace({ id: "space-2", team_id: "team-2" }),
      ],
    });

    render(<TeamsCard />);
    fireEvent.click(screen.getByText("all_teams · 2"));

    expect(pushMock).toHaveBeenCalledWith("/settings/teams");
  });
});
