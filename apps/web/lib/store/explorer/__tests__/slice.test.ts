import { describe, expect, it } from "vitest";

import {
  clearRestoreGroup,
  closeExplorer,
  explorerReducer,
  openExplorer,
  pickExplorerPattern,
  selectExplorerRoute,
  selectExplorerStop,
  setExplorerDate,
  setExplorerDrawing,
  setExplorerFilters,
  setExplorerLeftTab,
  setExplorerRouteTab,
  setPlaybackTime,
  setPlaying,
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
        patternShapes: {},
        patternChains: {},
      })
    );
    state = explorerReducer(state, selectExplorerRoute(null));
    expect(state.drawing).toBeNull();
  });

  it("starts every bundle unfiltered with no stop picked", () => {
    let state = explorerReducer(
      open(),
      setExplorerFilters({ types: ["stops"], modes: ["bus"], agencies: [0] })
    );
    state = explorerReducer(
      state,
      selectExplorerStop({ stop_idx: 4, name: "Fourth", lon: 13.43, lat: 52.5 })
    );
    expect(state.filters.modes).toEqual(["bus"]);
    state = open(explorerReducer(state, closeExplorer()));
    expect(state.filters).toEqual({ types: [], modes: [], agencies: [] });
    expect(state.selectedStop).toBeNull();
  });

  it("opens another route on its Overview, in its first direction", () => {
    let state = explorerReducer(open(), selectExplorerRoute("R1"));
    state = explorerReducer(state, setExplorerRouteTab("stops"));
    state = explorerReducer(state, pickExplorerPattern({ direction: 3, pattern_idx: 4 }));
    // Back to the list and into the same route: where the user was.
    state = explorerReducer(state, selectExplorerRoute("R1"));
    expect(state.routeTab).toBe("stops");
    state = explorerReducer(state, selectExplorerRoute("R2"));
    expect(state.routeTab).toBe("overview");
    expect(state.pattern).toBeNull();
  });

  it("lets go of a stop picked along a route when the route is left", () => {
    let state = explorerReducer(open(), selectExplorerRoute("R1"));
    state = explorerReducer(
      state,
      selectExplorerStop({ stop_idx: 4, name: "Fourth", lon: 13.43, lat: 52.5 })
    );
    state = explorerReducer(state, selectExplorerRoute(null));
    expect(state.selectedStop).toBeNull();
  });

  it("stops playback and forgets its time when the date changes", () => {
    let state = explorerReducer(open(), setExplorerDate("2026-10-14"));
    state = explorerReducer(state, setPlaybackTime(8 * 3600));
    state = explorerReducer(state, setPlaying(true));
    state = explorerReducer(state, setExplorerDate("2026-10-14"));
    expect(state).toMatchObject({ playing: true, time_s: 8 * 3600 });
    state = explorerReducer(state, setExplorerDate("2026-10-15"));
    expect(state).toMatchObject({ playing: false, time_s: null });
  });

  it("hands back the group it hid when it closes", () => {
    let state = explorerReducer(
      undefined,
      openExplorer({ bundleId: "b1", bundleName: "Feed", hiddenGroupId: 7 })
    );
    expect(state.hiddenGroupId).toBe(7);
    state = explorerReducer(state, closeExplorer());
    expect(state).toMatchObject({ active: false, hiddenGroupId: null, restoreGroupId: 7 });
    state = explorerReducer(state, clearRestoreGroup());
    expect(state.restoreGroupId).toBeNull();
  });

  it("ends when the map leaves the data view", () => {
    expect(explorerReducer(open(), setMapMode("builder")).active).toBe(false);
    expect(explorerReducer(open(), setMapMode("data")).active).toBe(true);
  });
});
