/**
 * The PT network Explorer's view state: which bundle is open, which tab, which
 * route or stop, which date, what the list is filtered by. The data itself is in DuckDB-WASM (useExplorerDb), not
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
  stops: { lon: number; lat: number; name: string }[];
  /** Straight lines between stops, for trips without a shape. */
  stopChains: [number, number][][];
}

/** Where the map shows a picked stop; it draws from outside the panels. */
export interface ExplorerPickedStop {
  stop_idx: number;
  name: string;
  lon: number;
  lat: number;
}

export interface ExplorerState {
  active: boolean;
  bundleId: string | null;
  bundleName: string | null;
  leftTab: ExplorerLeftTab;
  selectedRouteId: string | null;
  /** The one date every view shows, `YYYY-MM-DD`; null until the feed's
   * window is known. */
  date: string | null;
  drawing: ExplorerRouteDrawing | null;
  filters: ExplorerFilters;
  /** The stop (or station) picked in a list, highlighted on the map. */
  selectedStop: ExplorerPickedStop | null;
  routeTab: ExplorerRouteTab;
  /** One picker for the Stops tab now, and the strip and stop timetable
   * later (02-panel-behaviour-spec › Directions). */
  pattern: ExplorerPatternChoice | null;
  /** The settings dialog, open on a tab, or closed. */
  settingsTab: ExplorerSettingsTab | null;
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
  leftTab: "explorer",
  selectedRouteId: null,
  date: null,
  drawing: null,
  filters: NO_FILTERS,
  selectedStop: null,
  routeTab: "overview",
  pattern: null,
  settingsTab: null,
  playing: false,
  speed: 60,
  time_s: null,
};

const explorerSlice = createSlice({
  name: "explorer",
  initialState,
  reducers: {
    openExplorer: (state, action: PayloadAction<{ bundleId: string; bundleName: string }>) => {
      // The tab the user chose stays for the session.
      return {
        ...initialState,
        leftTab: state.leftTab,
        active: true,
        bundleId: action.payload.bundleId,
        bundleName: action.payload.bundleName,
      };
    },
    closeExplorer: (state) => ({
      ...initialState,
      leftTab: state.leftTab,
    }),
    setExplorerLeftTab: (state, action: PayloadAction<ExplorerLeftTab>) => {
      state.leftTab = action.payload;
    },
    selectExplorerRoute: (state, action: PayloadAction<string | null>) => {
      // Another route starts on its Overview, in its own first direction.
      if (action.payload !== state.selectedRouteId) {
        state.routeTab = "overview";
        state.pattern = null;
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
    selectExplorerStop: (state, action: PayloadAction<ExplorerPickedStop | null>) => {
      state.selectedStop = action.payload;
    },
  },
  extraReducers: (builder) => {
    // The Explorer lives in the data view; leaving it ends the session.
    builder.addCase(setMapMode, (state, action) =>
      action.payload === "data" ? state : { ...initialState, leftTab: state.leftTab }
    );
  },
});

export const {
  openExplorer,
  closeExplorer,
  setExplorerLeftTab,
  selectExplorerRoute,
  setExplorerDate,
  setExplorerDrawing,
  setExplorerFilters,
  selectExplorerStop,
  setExplorerRouteTab,
  pickExplorerPattern,
  openExplorerSettings,
  setPlaying,
  setPlaybackSpeed,
  setPlaybackTime,
} = explorerSlice.actions;

export const explorerReducer = explorerSlice.reducer;
