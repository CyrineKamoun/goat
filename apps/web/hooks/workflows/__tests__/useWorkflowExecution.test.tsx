/**
 * Run-gate coverage: a dataset node with an unresolved template "ask" input
 * (`data.unresolved === true`, set by apps/core's freeze_workflow_config/
 * bind_workflow_config, T7) must block `canExecute` regardless of the
 * workflow otherwise being runnable, and `hasUnresolvedInputs` must say why.
 */
import { configureStore } from "@reduxjs/toolkit";
import { renderHook } from "@testing-library/react";
import type { ReactNode } from "react";
import React from "react";
import { Provider } from "react-redux";
import { describe, expect, it } from "vitest";
import { vi } from "vitest";

import { jobsReduces } from "@/lib/store/jobs/slice";
import { workflowReducer } from "@/lib/store/workflow/slice";
import type { WorkflowNode } from "@/lib/validations/workflow";

import { useWorkflowExecution } from "@/hooks/workflows/useWorkflowExecution";

vi.mock("react-i18next", () => ({
  useTranslation: () => ({ t: (key: string) => key }),
}));

vi.mock("@/lib/api/processes", () => ({
  useJobs: () => ({ jobs: [], mutate: vi.fn() }),
  dismissJob: vi.fn(),
}));

vi.mock("@/lib/api/workflows", () => ({
  executeWorkflow: vi.fn(),
  finalizeWorkflowLayer: vi.fn(),
  cleanupWorkflowTemp: vi.fn(),
}));

const toolNode: WorkflowNode = {
  id: "tool-1",
  type: "tool",
  position: { x: 0, y: 0 },
  data: { type: "tool", processId: "buffer", label: "Buffer", config: {} },
} as unknown as WorkflowNode;

const unresolvedDatasetNode: WorkflowNode = {
  id: "dataset-1",
  type: "dataset",
  position: { x: 0, y: 0 },
  data: { type: "dataset", label: "Dataset", unresolved: true },
} as unknown as WorkflowNode;

const resolvedDatasetNode: WorkflowNode = {
  id: "dataset-2",
  type: "dataset",
  position: { x: 0, y: 0 },
  data: { type: "dataset", label: "Dataset", layerId: "11111111-1111-1111-1111-111111111111" },
} as unknown as WorkflowNode;

const makeStore = (nodes: WorkflowNode[]) =>
  configureStore({
    reducer: { workflow: workflowReducer, jobs: jobsReduces },
    preloadedState: {
      workflow: {
        workflows: [],
        selectedWorkflowId: null,
        selectedNodeId: null,
        nodes,
        edges: [],
        viewport: { x: 0, y: 0, zoom: 1 },
        variables: [],
        isDirty: false,
        requestMapView: false,
        requestTableView: false,
        activeDataPanelView: null,
      },
      jobs: { runningJobIds: [] },
    },
  });

const renderExecution = (nodes: WorkflowNode[]) => {
  const store = makeStore(nodes);
  const wrapper = ({ children }: { children: ReactNode }) => <Provider store={store}>{children}</Provider>;
  return renderHook(() => useWorkflowExecution({ workflow: { id: "w1" }, projectId: "p1", folderId: "f1" }), {
    wrapper,
  });
};

describe("useWorkflowExecution run gate", () => {
  it("allows running a workflow with a tool node and no unresolved inputs", () => {
    const { result } = renderExecution([toolNode, resolvedDatasetNode]);
    expect(result.current.hasUnresolvedInputs).toBe(false);
    expect(result.current.canExecute).toBe(true);
  });

  it("blocks canExecute while any dataset node is unresolved, even with a tool node present", () => {
    const { result } = renderExecution([toolNode, unresolvedDatasetNode]);
    expect(result.current.hasUnresolvedInputs).toBe(true);
    expect(result.current.canExecute).toBe(false);
  });

  it("stays blocked for the usual reasons (no tool node) independent of unresolved state", () => {
    const { result } = renderExecution([resolvedDatasetNode]);
    expect(result.current.hasUnresolvedInputs).toBe(false);
    expect(result.current.canExecute).toBe(false);
  });
});
