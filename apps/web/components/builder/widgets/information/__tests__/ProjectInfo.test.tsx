import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type { Project } from "@/lib/validations/project";

import { ProjectInfo } from "@/components/builder/widgets/information/ProjectInfo";

vi.mock("react-i18next", () => ({ useTranslation: () => ({ t: (k: string) => k }) }));
vi.mock("@/components/builder/widgets/common/MarkdownContentEditor", () => ({
  default: () => <div>markdown editor</div>,
}));
vi.mock("@/components/builder/widgets/common/PopupSettingsControls", () => ({
  default: () => <div>popup settings</div>,
}));
vi.mock("@/components/builder/widgets/common/PopupContentRenderer", () => ({
  default: () => <div>viewer popup</div>,
}));

const project = {
  id: "p1",
  name: "Munich",
  builder_config: { settings: { project_info_content: "About Munich" } },
} as unknown as Project;

describe("ProjectInfo (builder mode)", () => {
  it("opens the edit dialog from the info button, with the title and the Done action", () => {
    render(<ProjectInfo project={project} onProjectUpdate={vi.fn()} />);

    fireEvent.click(screen.getByRole("button", { name: "info" }));
    expect(screen.getByText("edit_popup_content")).toBeInTheDocument();
    expect(screen.getByText("markdown editor")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "done" })).toBeInTheDocument();
  });

  it("closes the dialog again from Done", async () => {
    render(<ProjectInfo project={project} onProjectUpdate={vi.fn()} />);

    fireEvent.click(screen.getByRole("button", { name: "info" }));
    fireEvent.click(screen.getByRole("button", { name: "done" }));
    await waitFor(() => expect(screen.queryByText("markdown editor")).not.toBeInTheDocument());
  });
});
