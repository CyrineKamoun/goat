import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import TeamMemberInviteModal from "@/components/modals/settings/InviteTeamMember";

vi.mock("react-i18next", () => ({ useTranslation: () => ({ t: (key: string) => key }) }));
vi.mock("react-toastify", () => ({ toast: { success: vi.fn(), error: vi.fn() } }));
vi.mock("@/lib/api/teams", () => ({ createTeamMember: vi.fn() }));

const members = [{ id: "user-1", firstname: "Ada", lastname: "Lovelace", email: "ada@example.com" }];

describe("InviteTeamMember", () => {
  it("offers the organization's members and the add action", () => {
    render(<TeamMemberInviteModal open teamId="team-1" members={members} onClose={() => {}} />);

    expect(screen.getByText("common:add_member")).toBeInTheDocument();
    expect(screen.getByText("common:select_an_organization_member")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "common:add" })).toBeInTheDocument();
  });

  it("refuses the add until a member is selected", () => {
    render(<TeamMemberInviteModal open teamId="team-1" members={members} onClose={() => {}} />);

    expect((screen.getByRole("button", { name: "common:add" }) as HTMLButtonElement).disabled).toBe(true);
  });
});
