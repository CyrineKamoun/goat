import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import {
  NEW_PROJECT_ITEMS,
  NewProjectButton,
  NewProjectFlows,
} from "@/components/dashboard/common/NewProjectMenu";

const {
  createProjectMock,
  pushMock,
  refreshContentFeedMock,
  mutateMock,
  nameDialogMock,
  templateBrowserMock,
  templateFlowMock,
  projectImportMock,
} = vi.hoisted(() => ({
  createProjectMock: vi.fn(),
  pushMock: vi.fn(),
  refreshContentFeedMock: vi.fn(),
  mutateMock: vi.fn(),
  nameDialogMock: vi.fn(),
  templateBrowserMock: vi.fn(),
  templateFlowMock: vi.fn(),
  projectImportMock: vi.fn(),
}));

vi.mock("react-i18next", () => ({
  useTranslation: () => ({ t: (key: string) => key }),
}));
vi.mock("swr", () => ({ mutate: mutateMock }));
vi.mock("next/navigation", () => ({ useRouter: () => ({ push: pushMock }) }));
vi.mock("@/lib/api/content", () => ({ refreshContentFeed: refreshContentFeedMock }));
vi.mock("@/lib/api/users", () => ({ USERS_API_BASE_URL: "http://core/api/v2/users" }));
vi.mock("@/lib/api/projects", () => ({
  createProject: (payload: unknown) => createProjectMock(payload),
}));
vi.mock("@/hooks/templates/useUseTemplate", () => ({
  templateResultHref: (_template: unknown, result: { project_id: string }) => `/map/${result.project_id}`,
}));
vi.mock("@/components/dashboard/common/NameDialog", () => ({
  default: (props: { onSubmit: (name: string) => Promise<void> }) => {
    nameDialogMock(props);
    return (
      <button type="button" onClick={() => void props.onSubmit("Bus stops")}>
        submit-name
      </button>
    );
  },
}));
vi.mock("@/components/templates/TemplateBrowser", () => ({
  default: (props: { onUse: (template: { id: string }) => void }) => {
    templateBrowserMock(props);
    return (
      <button type="button" onClick={() => props.onUse({ id: "tmpl-1" })}>
        pick-template
      </button>
    );
  },
}));
vi.mock("@/components/templates/UseTemplateFlow", () => ({
  default: (props: { onDone: (result: { project_id: string }) => void }) => {
    templateFlowMock(props);
    return (
      <button type="button" onClick={() => props.onDone({ project_id: "p-new" })}>
        finish-use-template
      </button>
    );
  },
}));
vi.mock("@/components/modals/ProjectImport", () => ({
  default: (props: { open: boolean; defaultFolderId?: string }) => {
    projectImportMock(props);
    return <div data-testid="project-import" />;
  },
}));

describe("NewProjectMenu", () => {
  beforeEach(() => {
    createProjectMock.mockReset().mockResolvedValue({ id: "p-1" });
    pushMock.mockReset();
    refreshContentFeedMock.mockReset();
    mutateMock.mockReset();
    nameDialogMock.mockReset();
    templateBrowserMock.mockReset();
    templateFlowMock.mockReset();
    projectImportMock.mockReset();
  });

  it("offers the three project starts, in one order, behind the button", () => {
    render(<NewProjectButton location={{ folderId: "home-1" }} />);

    expect(NEW_PROJECT_ITEMS.map((item) => item.key)).toEqual(["blank", "template", "import"]);
    // Nothing is mounted until an entry is picked — the template browser
    // would otherwise issue its own requests on every page that offers this.
    expect(templateBrowserMock).not.toHaveBeenCalled();
    expect(projectImportMock).not.toHaveBeenCalled();

    fireEvent.click(screen.getByRole("button", { name: "new_project" }));

    expect(screen.getByText("blank_project")).toBeInTheDocument();
    expect(screen.getByText("from_template")).toBeInTheDocument();
    // The import asks for a file before anything happens, so its entry says so.
    expect(screen.getByText("import_project")).toBeInTheDocument();
  });

  it("creates a blank project by name alone in the given folder, then opens the builder", async () => {
    render(<NewProjectFlows intent="blank" onClose={vi.fn()} location={{ folderId: "folder-a" }} />);

    fireEvent.click(screen.getByText("submit-name"));

    await waitFor(() =>
      expect(createProjectMock).toHaveBeenCalledWith(
        expect.objectContaining({ name: "Bus stops", folder_id: "folder-a" })
      )
    );
    const payload = createProjectMock.mock.calls[0][0];
    expect(payload.initial_view_state).toEqual(expect.objectContaining({ zoom: 12 }));
    expect(payload.thumbnail_url).toContain("goat_new_project_artwork");
    await waitFor(() => expect(pushMock).toHaveBeenCalledWith("/map/p-1"));
    expect(refreshContentFeedMock).toHaveBeenCalled();
    expect(mutateMock).toHaveBeenCalledWith("http://core/api/v2/users/me/onboarding");
  });

  it("opens the template browser unfiltered and hands the pick to the use flow", async () => {
    const onClose = vi.fn();
    render(<NewProjectFlows intent="template" onClose={onClose} location={{ folderId: "folder-a" }} />);

    expect(templateBrowserMock).toHaveBeenCalledWith(expect.objectContaining({ mode: "dialog" }));
    expect(templateBrowserMock.mock.calls[0][0].initialSource).toBeUndefined();

    fireEvent.click(screen.getByText("pick-template"));

    expect(onClose).toHaveBeenCalled();
    expect(templateFlowMock).toHaveBeenCalledWith(
      expect.objectContaining({ template: { id: "tmpl-1" }, context: { kind: "new_project" } })
    );

    fireEvent.click(screen.getByText("finish-use-template"));

    await waitFor(() => expect(pushMock).toHaveBeenCalledWith("/map/p-new"));
  });

  it("opens the import modal with the current folder pre-selected", () => {
    render(<NewProjectFlows intent="import" onClose={vi.fn()} location={{ folderId: "folder-a" }} />);

    expect(screen.getByTestId("project-import")).toBeInTheDocument();
    expect(projectImportMock).toHaveBeenCalledWith(
      expect.objectContaining({ open: true, defaultFolderId: "folder-a" })
    );
  });

  it("mounts no flow at all while no start has been picked", () => {
    render(<NewProjectFlows intent={null} onClose={vi.fn()} location={{}} />);

    expect(nameDialogMock).not.toHaveBeenCalled();
    expect(templateBrowserMock).not.toHaveBeenCalled();
    expect(projectImportMock).not.toHaveBeenCalled();
  });
});
