import { act, renderHook } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { useDatasetPickerFlow } from "@/hooks/addLayer/useDatasetPickerFlow";

// `vi.mock` factories are hoisted above the file's own statements, so the
// spies they hand out have to be hoisted with them.
const { addProjectLayers, addBundleToProject, mutate, toast } = vi.hoisted(() => ({
  addProjectLayers: vi.fn(async () => undefined),
  addBundleToProject: vi.fn(async () => undefined),
  mutate: vi.fn(async () => undefined),
  toast: { success: vi.fn(), error: vi.fn() },
}));
vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    t: (k: string, o?: { count?: number }) => (o?.count !== undefined ? `${k}:${o.count}` : k),
  }),
}));
vi.mock("react-toastify", () => ({ toast }));
vi.mock("swr", () => ({ mutate }));
vi.mock("@/lib/api/projects", () => ({
  addProjectLayers,
  addBundleToProject,
  projectLayersKey: (id: string) => [`/project/${id}/layer`],
  PROJECTS_API_BASE_URL: "/project",
}));
vi.mock("@/hooks/addLayer/useShareNotice", () => ({ useShareNotice: () => undefined }));

const [roads, rails, network] = [
  { type: "layer", id: "l1", name: "roads", space_id: "p1", updated_at: "", is_shortcut: false },
  { type: "layer", id: "l2", name: "rails", space_id: "p1", updated_at: "", is_shortcut: false },
  { type: "bundle", id: "b1", name: "Network", space_id: "p1", updated_at: "", is_shortcut: false },
] as never[];

const PROJECT_KEYS = [["/project/pr1/layer"], ["/project/pr1/group"], ["/project/pr1"]];

describe("useDatasetPickerFlow", () => {
  it("still toasts when a revalidation request fails", async () => {
    mutate.mockRejectedValueOnce(new Error("refetch failed"));
    const { result } = renderHook(() => useDatasetPickerFlow({ projectId: "pr1" }));
    act(() => result.current.selection.toggle(roads));
    await act(async () => {
      await result.current.action.run();
    });
    expect(toast.success).toHaveBeenCalled();
    expect(toast.error).not.toHaveBeenCalled();
  });

  beforeEach(() => vi.clearAllMocks());

  it("labels the footer by how many are selected and explains why it is disabled", () => {
    const { result } = renderHook(() => useDatasetPickerFlow({ projectId: "pr1" }));
    expect(result.current.action.disabled).toBe(true);
    expect(result.current.action.reason).toBe("catalog_select_datasets_first");
    act(() => result.current.selection.toggle(roads));
    expect(result.current.action.label).toBe("add_layer");
    act(() => result.current.selection.toggle(network));
    expect(result.current.action.label).toBe("catalog_add_n_layers:2");
    expect(result.current.action.disabled).toBe(false);
  });

  it("needs a project", () => {
    const { result } = renderHook(() => useDatasetPickerFlow({ projectId: undefined }));
    act(() => result.current.selection.toggle(roads));
    expect(result.current.action.disabled).toBe(true);
    expect(result.current.action.reason).toBe("add_needs_project");
  });

  it("keeps the selected items themselves, and drops one on a second toggle", () => {
    const { result } = renderHook(() => useDatasetPickerFlow({ projectId: "pr1" }));
    act(() => {
      result.current.selection.toggle(roads);
      result.current.selection.toggle(network);
    });
    expect(result.current.selection.ids).toEqual(["l1", "b1"]);
    expect(result.current.selection.items).toEqual([roads, network]);
    act(() => result.current.selection.toggle(roads));
    expect(result.current.selection.ids).toEqual(["b1"]);
  });

  it("clears the selection on reset", () => {
    const { result } = renderHook(() => useDatasetPickerFlow({ projectId: "pr1" }));
    act(() => result.current.selection.toggle(roads));
    act(() => result.current.reset());
    expect(result.current.selection.ids).toEqual([]);
  });

  it("adds layers in one call and bundles one by one, then refreshes the project keys", async () => {
    const onDone = vi.fn();
    const { result } = renderHook(() => useDatasetPickerFlow({ projectId: "pr1", onDone }));
    act(() => {
      result.current.selection.toggle(roads);
      result.current.selection.toggle(rails);
      result.current.selection.toggle(network);
    });
    await act(async () => {
      await result.current.action.run();
    });
    expect(onDone).toHaveBeenCalled();
    expect(addProjectLayers).toHaveBeenCalledWith("pr1", ["l1", "l2"]);
    expect(addBundleToProject).toHaveBeenCalledWith("pr1", "b1");
    for (const key of PROJECT_KEYS) expect(mutate).toHaveBeenCalledWith(key);
    expect(toast.success).toHaveBeenCalledWith("catalog_layers_added:3");
    expect(toast.error).not.toHaveBeenCalled();
    expect(result.current.selection.ids).toEqual([]);
  });

  it("adds a bundle on its own without posting an empty layer request", async () => {
    const { result } = renderHook(() => useDatasetPickerFlow({ projectId: "pr1" }));
    act(() => result.current.selection.toggle(network));
    await act(async () => {
      await result.current.action.run();
    });
    expect(addProjectLayers).not.toHaveBeenCalled();
    expect(addBundleToProject).toHaveBeenCalledTimes(1);
    expect(addBundleToProject).toHaveBeenCalledWith("pr1", "b1");
    expect(toast.success).toHaveBeenCalledWith("catalog_layers_added:1");
  });

  it("still refreshes the project and reports both halves when only part of a batch lands", async () => {
    addBundleToProject.mockRejectedValueOnce(new Error("bundle refused"));
    const { result } = renderHook(() => useDatasetPickerFlow({ projectId: "pr1" }));
    act(() => {
      result.current.selection.toggle(roads);
      result.current.selection.toggle(rails);
      result.current.selection.toggle(network);
    });
    await act(async () => {
      await result.current.action.run();
    });
    // The layers were added server-side, so the tree has to be refetched even
    // though the bundle failed.
    for (const key of PROJECT_KEYS) expect(mutate).toHaveBeenCalledWith(key);
    expect(toast.success).toHaveBeenCalledWith("catalog_layers_added:2");
    expect(toast.error).toHaveBeenCalledWith("bundle refused");
  });

  it("reports a failure by toast", async () => {
    addProjectLayers.mockRejectedValueOnce(new Error("nope"));
    const { result } = renderHook(() => useDatasetPickerFlow({ projectId: "pr1" }));
    act(() => result.current.selection.toggle(roads));
    await act(async () => {
      await result.current.action.run();
    });
    expect(toast.error).toHaveBeenCalledWith("nope");
    expect(toast.success).not.toHaveBeenCalled();
    for (const key of PROJECT_KEYS) expect(mutate).toHaveBeenCalledWith(key);
  });
});
