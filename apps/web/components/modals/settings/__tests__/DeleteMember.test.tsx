import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import DeleteMemberModal from "@/components/modals/settings/DeleteMember";

vi.mock("react-i18next", () => ({ useTranslation: () => ({ t: (key: string) => key }) }));
vi.mock("react-toastify", () => ({ toast: { success: vi.fn(), error: vi.fn() } }));
vi.mock("@/lib/api/organizations", () => ({ deleteInvitation: vi.fn(), deleteMember: vi.fn() }));
vi.mock("@/lib/api/teams", () => ({ deleteMember: vi.fn() }));

// eslint-disable-next-line @typescript-eslint/no-explicit-any
const member = { id: "member-1", email: "someone@example.com", invitation_status: "accepted" } as any;

describe("DeleteMember", () => {
  it("names the member and offers the removal", () => {
    render(<DeleteMemberModal open member={member} organizationId="org-1" onClose={() => {}} />);

    expect(screen.getByText("delete_member")).toBeInTheDocument();
    expect(screen.getByText("someone@example.com")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "remove" })).toBeInTheDocument();
  });

  it("refuses the removal while the caller has it disabled", () => {
    render(<DeleteMemberModal open disabled member={member} organizationId="org-1" onClose={() => {}} />);

    expect((screen.getByRole("button", { name: "remove" }) as HTMLButtonElement).disabled).toBe(true);
  });
});
