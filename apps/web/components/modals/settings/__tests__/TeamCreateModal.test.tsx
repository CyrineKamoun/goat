import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import TeamCreateModal from "@/components/modals/settings/TeamCreateModal";

vi.mock("react-i18next", () => ({ useTranslation: () => ({ t: (key: string) => key }) }));
vi.mock("react-toastify", () => ({ toast: { success: vi.fn(), error: vi.fn() } }));
vi.mock("@/lib/api/teams", () => ({ createTeam: vi.fn() }));

describe("TeamCreateModal", () => {
  it("asks for the new team's name and offers the create action", () => {
    render(<TeamCreateModal open onClose={() => {}} />);

    expect(screen.getByText("common:create_team")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "common:create" })).toBeInTheDocument();
  });

  it("refuses the create while the form is empty", () => {
    render(<TeamCreateModal open onClose={() => {}} />);

    expect((screen.getByRole("button", { name: "common:create" }) as HTMLButtonElement).disabled).toBe(true);
  });
});
