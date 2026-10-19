/**
 * The PT network Explorer's view state: which bundle is open, which tab, which
 * route, what the right panel shows, which date, what the list is filtered by. The data itself is in DuckDB-WASM (useExplorerDb), not
 * here; the selected route's drawing — a handful of shape ids and stops — is,
 * because the map draws it from inside <Map>, away from the panels that hold
 * the database.
 */
import { type PayloadAction, createSlice } from "@reduxjs/toolkit";

import type { ModeKey } from "@/lib/explorer/modes";
import { setMapMode } from "@/lib/store/map/slice";

export type ExplorerLeftTab = "layers" | "explorer";

/** What the Explorer tab lists; an empty choice means everything. */
export interface ExplorerFilters {
  types: ("routes" | "stops")[];
  modes: ModeKey[];
  /** agency_idx values. */
  agencies: number[];
}

export type ExplorerRouteTab = "overview" | "stops" | "service" | "calendar";

/** The destination picker's choice: a direction, and one of its patterns. */
export interface ExplorerPatternChoice {
  direction: number;
  pattern_idx: number;
}

/** The settings dialog's tabs (settings.ts). */
export type ExplorerSettingsTab = "rates" | "periods" | "vehicles";

/** Playback speeds: how many seconds of the day pass in one second. */
export const PLAYBACK_SPEEDS = [1, 10, 60] as const;
export type PlaybackSpeed = (typeof PLAYBACK_SPEEDS)[number];

export const NO_FILTERS: ExplorerFilters = { types: [], modes: [], agencies: [] };

export interface ExplorerRouteDrawing {
  routeId: string;
  /** `#RRGGBB`; the feed's colour, or none. */
  color: string | null;
  /** The bundle's shape_lines layer, and the route's shape ids in it. */
  shapeLinesLayerId: string | null;
  shapeIds: string[];
  /** Its stops, for the map and for fitting the view. */
  stops: { stop_idx: number; lon: number; lat: number; name: string }[];
  /** Each pattern's shapes — the feed's, or the line through its stops —:
   * what the map highlights when the Stops tab shows that pattern. */
  patternShapes: Record<number, string[]>;
  /** Each pattern's extent from its stops, `[west, south, east, north]`: the
   * map fits it when the Stops tab switches to it. */
  patternBounds: Record<number, [number, number, number, number]>;
}

/** A stop the right panel shows, and where the map marks it (a station
 * may have no position of its own). */
export interface ExplorerPickedStop {
  stop_idx: number;
  name: string;
  lon: number | null;
  lat: number | null;
}

/** What the right panel shows (02-panel-behaviour-spec › Viewers): a stop's
 * timetable, or one run of a trip — a trip and when the run left its first
 * stop, as a frequency-based trip runs many times. */
export type ExplorerPanelItem =
  | { kind: "stop"; stop: ExplorerPickedStop }
  | { kind: "trip"; trip_idx: number; run_dep_s: number };

export interface ExplorerPanel {
  /** The item shown is the last; "‹" goes back to the one before (a trip
   * opened from a stop's timetable goes back to the stop). */
  stack: ExplorerPanelItem[];
  /** Folded to its rail. The user's choice: opening another item does not
   * unfold it (02-panel-behaviour-spec › Sticky preferences). */
  collapsed: boolean;
  /** The route the item was opened from, if any: leaving that route lets
   * the item go. */
  fromRoute: string | null;
}

export interface ExplorerState {
  active: boolean;
  bundleId: string | null;
  bundleName: string | null;
  /** The layer-tree group the Explorer hid on opening (its bundle's own
   * layers, drawn by the Explorer itself), and, once it closes, the one to
   * show again — the layer tree does both. */
  hiddenGroupId: number | null;
  restoreGroupId: number | null;
  leftTab: ExplorerLeftTab;
  selectedRouteId: string | null;
  /** The one date every view shows, `YYYY-MM-DD`; null until the feed's
   * window is known. */
  date: string | null;
  drawing: ExplorerRouteDrawing | null;
  filters: ExplorerFilters;
  panel: ExplorerPanel;
  routeTab: ExplorerRouteTab;
  /** One picker for the Stops tab now, and the strip and stop timetable
   * later (02-panel-behaviour-spec › Directions). */
  pattern: ExplorerPatternChoice | null;
  /** The settings dialog, open on a tab, or closed. */
  settingsTab: ExplorerSettingsTab | null;
  /** The network's extent, `[west, south, east, north]`: the map fits it
   * once on opening. */
  bounds: [number, number, number, number] | null;
  /** Playback: whether it runs, how fast, and the time of the day it shows,
   * in service-day seconds — null until the day's service is known. While it
   * runs the live time is the playback clock's (lib/explorer/playback.ts);
   * this is where it starts and stops. */
  playing: boolean;
  speed: PlaybackSpeed;
  time_s: number | null;
}

const initialState: ExplorerState = {
  active: false,
  bundleId: null,
  bundleName: null,
  hiddenGroupId: null,
  restoreGroupId: null,
  leftTab: "explorer",
  selectedRouteId: null,
  date: null,
  drawing: null,
  filters: NO_FILTERS,
  panel: { stack: [], collapsed: false, fromRoute: null },
  routeTab: "overview",
  pattern: null,
  settingsTab: null,
  bounds: null,
  playing: false,
  speed: 10,
  time_s: null,
};

const explorerSlice = createSlice({
  name: "explorer",
  initialState,
  reducers: {
    openExplorer: (
      state,
      action: PayloadAction<{ bundleId: string; bundleName: string; hiddenGroupId?: number | null }>
    ) => {
      // The tab the user chose stays for the session.
      return {
        ...initialState,
        leftTab: state.leftTab,
        panel: { ...initialState.panel, collapsed: state.panel.collapsed },
        active: true,
        bundleId: action.payload.bundleId,
        bundleName: action.payload.bundleName,
        hiddenGroupId: action.payload.hiddenGroupId ?? null,
      };
    },
    closeExplorer: (state) => ({
      ...initialState,
      leftTab: state.leftTab,
      panel: { ...initialState.panel, collapsed: state.panel.collapsed },
      restoreGroupId: state.hiddenGroupId,
    }),
    /** The layer tree has shown the group again. */
    clearRestoreGroup: (state) => {
      state.restoreGroupId = null;
    },
    setExplorerLeftTab: (state, action: PayloadAction<ExplorerLeftTab>) => {
      state.leftTab = action.payload;
    },
    selectExplorerRoute: (state, action: PayloadAction<string | null>) => {
      // Another route starts on its Overview, in its own first direction;
      // what the right panel opened from the route it leaves goes with it.
      if (action.payload !== state.selectedRouteId) {
        state.routeTab = "overview";
        state.pattern = null;
        if (state.panel.fromRoute !== null && state.panel.fromRoute === state.selectedRouteId)
          state.panel = { ...state.panel, stack: [], fromRoute: null };
      }
      state.selectedRouteId = action.payload;
      if (action.payload === null) state.drawing = null;
    },
    setExplorerRouteTab: (state, action: PayloadAction<ExplorerRouteTab>) => {
      state.routeTab = action.payload;
    },
    pickExplorerPattern: (state, action: PayloadAction<ExplorerPatternChoice | null>) => {
      state.pattern = action.payload;
    },
    openExplorerSettings: (state, action: PayloadAction<ExplorerSettingsTab | null>) => {
      state.settingsTab = action.payload;
    },
    setExplorerBounds: (state, action: PayloadAction<[number, number, number, number] | null>) => {
      state.bounds = action.payload;
    },
    setPlaying: (state, action: PayloadAction<boolean>) => {
      state.playing = action.payload;
    },
    setPlaybackSpeed: (state, action: PayloadAction<PlaybackSpeed>) => {
      state.speed = action.payload;
    },
    /** Where playback stands: set by the scrubber, and by the clock when it stops. */
    setPlaybackTime: (state, action: PayloadAction<number | null>) => {
      state.time_s = action.payload;
    },
    setExplorerDate: (state, action: PayloadAction<string>) => {
      // Another day's service has another span: playback starts it afresh.
      if (action.payload !== state.date) {
        state.playing = false;
        state.time_s = null;
      }
      state.date = action.payload;
    },
    setExplorerDrawing: (state, action: PayloadAction<ExplorerRouteDrawing | null>) => {
      state.drawing = action.payload;
    },
    setExplorerFilters: (state, action: PayloadAction<ExplorerFilters>) => {
      state.filters = action.payload;
    },
    /** Show an item in the right panel: in place of everything it shows;
     * with `push`, over it, to go back to; with `step`, in place of the item
     * shown only (the trip before or after it). */
    openExplorerPanel: (
      state,
      action: PayloadAction<{
        item: ExplorerPanelItem;
        push?: boolean;
        step?: boolean;
        fromRoute?: string | null;
      }>
    ) => {
      const { item, push, step, fromRoute } = action.payload;
      if (push) state.panel.stack = [...state.panel.stack, item];
      else if (step && state.panel.stack.length)
        state.panel.stack = [...state.panel.stack.slice(0, -1), item];
      else {
        state.panel.stack = [item];
        state.panel.fromRoute = fromRoute ?? null;
      }
    },
    explorerPanelBack: (state) => {
      state.panel.stack = state.panel.stack.slice(0, -1);
      if (!state.panel.stack.length) state.panel.fromRoute = null;
    },
    closeExplorerPanel: (state) => {
      state.panel.stack = [];
      state.panel.fromRoute = null;
    },
    setExplorerPanelCollapsed: (state, action: PayloadAction<boolean>) => {
      state.panel.collapsed = action.payload;
    },
  },
  extraReducers: (builder) => {
    // The Explorer lives in the data view; leaving it ends the session.
    builder.addCase(setMapMode, (state, action) =>
      action.payload === "data"
        ? state
        : {
            ...initialState,
            leftTab: state.leftTab,
            panel: { ...initialState.panel, collapsed: state.panel.collapsed },
            restoreGroupId: state.hiddenGroupId,
          }
    );
  },
});

export const {
  openExplorer,
  closeExplorer,
  clearRestoreGroup,
  setExplorerLeftTab,
  selectExplorerRoute,
  setExplorerDate,
  setExplorerDrawing,
  setExplorerFilters,
  openExplorerPanel,
  explorerPanelBack,
  closeExplorerPanel,
  setExplorerPanelCollapsed,
  setExplorerRouteTab,
  pickExplorerPattern,
  openExplorerSettings,
  setExplorerBounds,
  setPlaying,
  setPlaybackSpeed,
  setPlaybackTime,
} = explorerSlice.actions;

export const explorerReducer = explorerSlice.reducer;

/** The item the right panel shows, if any. */
export function panelItem(state: ExplorerState): ExplorerPanelItem | null {
  return state.panel.stack[state.panel.stack.length - 1] ?? null;
}

/** The stop the right panel shows, highlighted on the map. */
export function pickedStop(state: ExplorerState): ExplorerPickedStop | null {
  const item = panelItem(state);
  return item?.kind === "stop" ? item.stop : null;
}
