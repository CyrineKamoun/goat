import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import OrgMemberInviteModal from "@/components/modals/settings/InviteOrgMember";

vi.mock("react-i18next", () => ({ useTranslation: () => ({ t: (key: string) => key }) }));
vi.mock("react-toastify", () => ({ toast: { success: vi.fn(), error: vi.fn() } }));
vi.mock("@/lib/api/organizations", () => ({ inviteMember: vi.fn() }));
vi.mock("@/lib/api/users", () => ({
  useOrganization: () => ({
    organization: {
      id: "org-1",
      name: "Acme",
      used_editors: 1,
      total_editors: 5,
      used_viewers: 1,
      total_viewers: 5,
    },
  }),
}));

describe("InviteOrgMember", () => {
  it("asks for the invitee's address and offers the invite", () => {
    render(<OrgMemberInviteModal open onClose={() => {}} />);

    expect(screen.getByText("common:invite_member")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "common:send_invite" })).toBeInTheDocument();
  });

  it("refuses the invite while the form is empty", () => {
    render(<OrgMemberInviteModal open onClose={() => {}} />);

    expect((screen.getByRole("button", { name: "common:send_invite" }) as HTMLButtonElement).disabled).toBe(
      true
    );
  });
});
