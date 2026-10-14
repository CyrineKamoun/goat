import { describe, expect, it } from "vitest";

import {
  closeExplorer,
  explorerReducer,
  openExplorer,
  selectExplorerRoute,
  setExplorerDrawing,
  setExplorerLeftTab,
} from "@/lib/store/explorer/slice";
import { setMapMode } from "@/lib/store/map/slice";

const open = (state = explorerReducer(undefined, { type: "init" })) =>
  explorerReducer(state, openExplorer({ bundleId: "b1", bundleName: "Feed" }));

describe("explorer slice", () => {
  it("opens on a bundle with nothing selected", () => {
    const state = open();
    expect(state).toMatchObject({ active: true, bundleId: "b1", bundleName: "Feed", selectedRouteId: null });
  });

  it("keeps the tab the user chose for the session", () => {
    let state = open();
    state = explorerReducer(state, setExplorerLeftTab("layers"));
    state = explorerReducer(state, closeExplorer());
    state = open(state);
    expect(state.leftTab).toBe("layers");
  });

  it("drops the drawing when the route is deselected", () => {
    let state = explorerReducer(open(), selectExplorerRoute("R1"));
    state = explorerReducer(
      state,
      setExplorerDrawing({
        routeId: "R1",
        color: "#123456",
        shapeLinesLayerId: null,
        shapeIds: [],
        stops: [],
        stopChains: [],
      })
    );
    state = explorerReducer(state, selectExplorerRoute(null));
    expect(state.drawing).toBeNull();
  });

  it("ends when the map leaves the data view", () => {
    expect(explorerReducer(open(), setMapMode("builder")).active).toBe(false);
    expect(explorerReducer(open(), setMapMode("data")).active).toBe(true);
  });
});
