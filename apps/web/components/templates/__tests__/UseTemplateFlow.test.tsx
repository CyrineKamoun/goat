import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { USERS_API_BASE_URL } from "@/lib/api/users";
import type { Folder } from "@/lib/validations/folder";
import type { TemplateInput, TemplateRead, TemplateUseResult } from "@/lib/validations/template";

import UseTemplateFlow from "@/components/templates/UseTemplateFlow";

const {
  applyTemplateMock,
  refreshTemplatesMock,
  refreshContentFeedMock,
  useSpacesMock,
  useFoldersMock,
  useProjectLayersMock,
  toastSuccessMock,
  toastInfoMock,
  swrMutateMock,
} = vi.hoisted(() => ({
  applyTemplateMock: vi.fn(),
  refreshTemplatesMock: vi.fn(),
  refreshContentFeedMock: vi.fn(),
  useSpacesMock: vi.fn(),
  useFoldersMock: vi.fn(),
  useProjectLayersMock: vi.fn(),
  toastSuccessMock: vi.fn(),
  toastInfoMock: vi.fn(),
  swrMutateMock: vi.fn(),
}));

vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    t: (key: string, vars?: Record<string, unknown>) => (vars ? `${key}:${JSON.stringify(vars)}` : key),
  }),
}));
vi.mock("react-toastify", () => ({ toast: { success: toastSuccessMock, info: toastInfoMock } }));
vi.mock("swr", () => ({ mutate: swrMutateMock }));
vi.mock("@/lib/api/templates", () => ({
  applyTemplate: applyTemplateMock,
  refreshTemplates: refreshTemplatesMock,
}));
vi.mock("@/lib/api/content", () => ({
  useSpaces: useSpacesMock,
  refreshContentFeed: refreshContentFeedMock,
}));
vi.mock("@/lib/api/folders", () => ({ useFolders: useFoldersMock }));
vi.mock("@/lib/api/projects", () => ({ useProjectLayers: useProjectLayersMock }));

const personalSpace = {
  id: "space-1",
  kind: "personal" as const,
  name: "Majk",
  default_role: "viewer" as const,
  my_role: "owner" as const,
  team_id: null,
  organization_id: null,
};

const homeFolder: Folder = {
  id: "folder-home",
  name: "home",
  parent_id: null,
  space_id: "space-1",
  depth: 0,
  is_owned: true,
  restricted: false,
};

const askInput: TemplateInput = {
  key: "input_layer",
  label: "Streets",
  mode: "ask",
  layer_id: null,
  layer_type: "feature",
  geometry_type: "point",
  from_catalog: false,
};

const baseTemplate: TemplateRead = {
  id: "template-1",
  name: "Isochrone starter",
  description: "A starter workflow",
  categories: [],
  thumbnail_url: null,
  space_id: "space-1",
  folder_id: "folder-home",
  created_by: null,
  payload_kind: "workflow",
  kinds: ["workflow"],
  inputs: [],
  ships_sample_data: false,
  datasets_needing_share: [],
  catalog_status: "none",
  source_ref: {},
  my_role: "viewer",
  created_at: "2026-09-01T10:00:00Z",
  updated_at: "2026-09-01T10:00:00Z",
};

const templateWithAsk: TemplateRead = { ...baseTemplate, inputs: [askInput] };

const useResult: TemplateUseResult = {
  project_id: "project-1",
  workflow_id: "workflow-1",
  layout_id: null,
  added_layer_project_ids: [],
  unresolved_inputs: [],
};

const noop = () => {};

describe("UseTemplateFlow", () => {
  beforeEach(() => {
    applyTemplateMock.mockReset().mockResolvedValue(useResult);
    refreshTemplatesMock.mockReset();
    refreshContentFeedMock.mockReset();
    toastSuccessMock.mockReset();
    toastInfoMock.mockReset();
    swrMutateMock.mockReset();
    useSpacesMock
      .mockReset()
      .mockReturnValue({ spaces: [personalSpace], isLoading: false, isError: undefined });
    useFoldersMock
      .mockReset()
      .mockReturnValue({ folders: [homeFolder], isLoading: false, isError: undefined });
    useProjectLayersMock.mockReset().mockReturnValue({ layers: [], isLoading: false, isError: undefined });
  });

  it("shows the location step for a new_project context", () => {
    render(
      <UseTemplateFlow
        template={templateWithAsk}
        context={{ kind: "new_project" }}
        onClose={noop}
        onDone={noop}
      />
    );

    expect(screen.getByText("location")).toBeInTheDocument();
    // The name field carries its label above itself, not as a floating one
    // inside the input.
    expect(document.querySelector(".MuiInputLabel-root")).toBeNull();
    expect((screen.getByLabelText("name") as HTMLInputElement).value).toBe(templateWithAsk.name);
  });

  it("skips the location step for an in_project context", () => {
    render(
      <UseTemplateFlow
        template={templateWithAsk}
        context={{ kind: "in_project", projectId: "project-1" }}
        onClose={noop}
        onDone={noop}
      />
    );

    expect(screen.queryByText("location")).not.toBeInTheDocument();
    expect(screen.queryByLabelText("name")).not.toBeInTheDocument();
  });

  it("offers a layer picker for an ask input when a matching project layer exists (in_project)", () => {
    useProjectLayersMock.mockReturnValue({
      layers: [
        {
          id: 1,
          layer_id: "layer-uuid-1",
          name: "Roads",
          type: "feature",
          feature_layer_geometry_type: "point",
        },
      ],
      isLoading: false,
      isError: undefined,
    });

    render(
      <UseTemplateFlow
        template={templateWithAsk}
        context={{ kind: "in_project", projectId: "project-1" }}
        onClose={noop}
        onDone={noop}
      />
    );

    const picker = screen.getByLabelText(askInput.label);
    expect(picker).toBeInTheDocument();
    expect(screen.getByText("Roads")).toBeInTheDocument();
    expect(screen.getByText("decide_later")).toBeInTheDocument();
  });

  it("excludes a locked layer from an ask input's candidates (in_project)", () => {
    useProjectLayersMock.mockReturnValue({
      layers: [
        {
          id: 1,
          layer_id: "layer-uuid-1",
          name: "Roads",
          type: "feature",
          feature_layer_geometry_type: "point",
          locked: false,
        },
        {
          id: 2,
          layer_id: "layer-uuid-2",
          name: "No Access Layer",
          type: "feature",
          feature_layer_geometry_type: "point",
          locked: true,
        },
      ],
      isLoading: false,
      isError: undefined,
    });

    render(
      <UseTemplateFlow
        template={templateWithAsk}
        context={{ kind: "in_project", projectId: "project-1" }}
        onClose={noop}
        onDone={noop}
      />
    );

    expect(screen.getByText("Roads")).toBeInTheDocument();
    expect(screen.queryByText("No Access Layer")).not.toBeInTheDocument();
  });

  it("offers only Decide later for an ask input in a new_project context", () => {
    render(
      <UseTemplateFlow
        template={templateWithAsk}
        context={{ kind: "new_project" }}
        onClose={noop}
        onDone={noop}
      />
    );

    // Advance past the location step (defaults are already valid).
    fireEvent.click(screen.getByRole("button", { name: "next_step" }));

    expect(screen.queryByLabelText(askInput.label)).not.toBeInTheDocument();
    expect(screen.getByText("decide_later")).toBeInTheDocument();
  });

  it("applies with target_folder_id + name for a new_project context", async () => {
    render(
      <UseTemplateFlow
        template={baseTemplate}
        context={{ kind: "new_project" }}
        onClose={noop}
        onDone={noop}
      />
    );

    fireEvent.click(screen.getByRole("button", { name: "create" }));

    await waitFor(() =>
      expect(applyTemplateMock).toHaveBeenCalledWith("template-1", {
        target_folder_id: "folder-home",
        name: "Isochrone starter",
        bindings: {},
      })
    );
  });

  it("applies with project_id for an in_project context and includes a chosen binding", async () => {
    useProjectLayersMock.mockReturnValue({
      layers: [
        {
          id: 1,
          layer_id: "layer-uuid-1",
          name: "Roads",
          type: "feature",
          feature_layer_geometry_type: "point",
        },
      ],
      isLoading: false,
      isError: undefined,
    });

    render(
      <UseTemplateFlow
        template={templateWithAsk}
        context={{ kind: "in_project", projectId: "project-1" }}
        onClose={noop}
        onDone={noop}
      />
    );

    fireEvent.change(screen.getByLabelText(askInput.label), { target: { value: "layer-uuid-1" } });
    fireEvent.click(screen.getByRole("button", { name: "create" }));

    await waitFor(() =>
      expect(applyTemplateMock).toHaveBeenCalledWith("template-1", {
        project_id: "project-1",
        bindings: { input_layer: "layer-uuid-1" },
      })
    );
  });

  it("calls onDone with the result after a successful apply", async () => {
    const onDone = vi.fn();
    render(
      <UseTemplateFlow
        template={baseTemplate}
        context={{ kind: "in_project", projectId: "project-1" }}
        onClose={noop}
        onDone={onDone}
      />
    );

    fireEvent.click(screen.getByRole("button", { name: "create" }));

    await waitFor(() => expect(onDone).toHaveBeenCalledWith(useResult));
    expect(toastSuccessMock).toHaveBeenCalled();
  });

  it("refreshes the content feed and revalidates onboarding facts after a successful apply", async () => {
    render(
      <UseTemplateFlow
        template={baseTemplate}
        context={{ kind: "in_project", projectId: "project-1" }}
        onClose={noop}
        onDone={noop}
      />
    );

    fireEvent.click(screen.getByRole("button", { name: "create" }));

    await waitFor(() => expect(refreshContentFeedMock).toHaveBeenCalled());
    expect(swrMutateMock).toHaveBeenCalledWith(`${USERS_API_BASE_URL}/me/onboarding`);
  });

  it("shows a toast with the unresolved input count after a successful apply", async () => {
    applyTemplateMock.mockReset().mockResolvedValue({
      ...useResult,
      unresolved_inputs: [askInput, { ...askInput, key: "input_layer_2" }],
    });

    render(
      <UseTemplateFlow
        template={baseTemplate}
        context={{ kind: "in_project", projectId: "project-1" }}
        onClose={noop}
        onDone={noop}
      />
    );

    fireEvent.click(screen.getByRole("button", { name: "create" }));

    await waitFor(() =>
      expect(toastInfoMock).toHaveBeenCalledWith(`unresolved_input_count:${JSON.stringify({ count: 2 })}`)
    );
  });

  it("shows an error with role=alert and does not call onDone when apply rejects (e.g. a 409)", async () => {
    applyTemplateMock.mockReset().mockRejectedValue(new Error("Failed to use template"));
    const onDone = vi.fn();
    render(
      <UseTemplateFlow
        template={baseTemplate}
        context={{ kind: "in_project", projectId: "project-1" }}
        onClose={noop}
        onDone={onDone}
      />
    );

    fireEvent.click(screen.getByRole("button", { name: "create" }));

    await waitFor(() => expect(screen.getByRole("alert")).toHaveTextContent("Failed to use template"));
    expect(onDone).not.toHaveBeenCalled();
  });
});
