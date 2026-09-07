/**
 * `data.unresolved` is set by the backend when a template's "ask" dataset
 * slot isn't bound to a layer on use (apps/core/src/core/templates/snapshot.py's
 * freeze_workflow_config/bind_workflow_config, T7). The node should flag this
 * clearly and let the user jump to the picker that clears it — the sidebar's
 * Dataset settings panel (components/workflows/panels/DatasetNodeSettings.tsx),
 * opened by selecting the node.
 */
import { configureStore } from "@reduxjs/toolkit";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import type { ComponentProps } from "react";
import React from "react";
import { Provider } from "react-redux";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { workflowReducer } from "@/lib/store/workflow/slice";
import type { DatasetNodeData } from "@/lib/validations/workflow";

import DatasetNode from "@/components/workflows/nodes/DatasetNode";

vi.mock("react-i18next", () => ({
  useTranslation: () => ({ t: (key: string) => key }),
}));

vi.mock("next/navigation", () => ({
  useParams: () => ({ projectId: "p1" }),
}));

vi.mock("@xyflow/react", () => ({
  Handle: () => null,
  NodeToolbar: ({ children }: { children: React.ReactNode }) => <>{children}</>,
  Position: { Top: "top", Right: "right", Left: "left", Bottom: "bottom" },
}));

vi.mock("@/lib/api/layers", () => ({
  useDatasetCollectionItems: () => ({ data: undefined }),
}));

vi.mock("@/lib/api/projects", () => ({
  useProjectLayers: () => ({ layers: [] }),
}));

vi.mock("@/hooks/map/CommonHooks", () => ({
  default: () => ({ layerFields: [] }),
}));

type DatasetNodeProps = ComponentProps<typeof DatasetNode>;

const makeStore = () => configureStore({ reducer: { workflow: workflowReducer } });

const nodeProps = (data: DatasetNodeData): DatasetNodeProps =>
  ({
    id: "dataset-1",
    type: "dataset",
    data,
    selected: false,
    dragging: false,
    zIndex: 0,
    isConnectable: true,
    positionAbsoluteX: 0,
    positionAbsoluteY: 0,
  }) as unknown as DatasetNodeProps;

const renderNode = (data: DatasetNodeData, store: ReturnType<typeof makeStore>) =>
  render(
    <Provider store={store}>
      <DatasetNode {...nodeProps(data)} />
    </Provider>
  );

const baseData: DatasetNodeData = { type: "dataset", label: "Dataset" };

describe("DatasetNode unresolved template input", () => {
  let store: ReturnType<typeof makeStore>;

  beforeEach(() => {
    store = makeStore();
  });

  it("shows nothing extra when the node is resolved", () => {
    renderNode(baseData, store);
    expect(screen.queryByLabelText("unresolved_input")).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "pick_a_layer" })).not.toBeInTheDocument();
  });

  it("shows the unresolved badge and a pick-a-layer action", () => {
    renderNode({ ...baseData, unresolved: true } as DatasetNodeData, store);
    expect(screen.getByLabelText("unresolved_input")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "pick_a_layer" })).toBeInTheDocument();
  });

  it("selects the node (opening its Dataset settings picker) when Pick a layer is clicked", async () => {
    const user = userEvent.setup();
    renderNode({ ...baseData, unresolved: true } as DatasetNodeData, store);

    await user.click(screen.getByRole("button", { name: "pick_a_layer" }));

    expect(store.getState().workflow.selectedNodeId).toBe("dataset-1");
  });

  it("hides the badge and action once the node is resolved (data.unresolved cleared)", () => {
    const { rerender } = renderNode({ ...baseData, unresolved: true } as DatasetNodeData, store);
    expect(screen.getByLabelText("unresolved_input")).toBeInTheDocument();

    rerender(
      <Provider store={store}>
        <DatasetNode {...nodeProps({ ...baseData, unresolved: false } as DatasetNodeData)} />
      </Provider>
    );

    expect(screen.queryByLabelText("unresolved_input")).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "pick_a_layer" })).not.toBeInTheDocument();
  });
});
