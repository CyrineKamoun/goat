/**
 * The PT network Explorer's view state: which bundle is open, which tab, which
 * route, which date. The data itself is in DuckDB-WASM (useExplorerDb), not
 * here; the selected route's drawing — a handful of shape ids and stops — is,
 * because the map draws it from inside <Map>, away from the panels that hold
 * the database.
 */
import { type PayloadAction, createSlice } from "@reduxjs/toolkit";

import { setMapMode } from "@/lib/store/map/slice";

export type ExplorerLeftTab = "layers" | "explorer";

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

export interface ExplorerState {
  active: boolean;
  bundleId: string | null;
  bundleName: string | null;
  leftTab: ExplorerLeftTab;
  leftCollapsed: boolean;
  selectedRouteId: string | null;
  /** The one date every view shows, `YYYY-MM-DD`; null until the feed's
   * window is known. */
  date: string | null;
  drawing: ExplorerRouteDrawing | null;
}

const initialState: ExplorerState = {
  active: false,
  bundleId: null,
  bundleName: null,
  leftTab: "explorer",
  leftCollapsed: false,
  selectedRouteId: null,
  date: null,
  drawing: null,
};

const explorerSlice = createSlice({
  name: "explorer",
  initialState,
  reducers: {
    openExplorer: (state, action: PayloadAction<{ bundleId: string; bundleName: string }>) => {
      // The tab and collapse the user chose stay for the session.
      return {
        ...initialState,
        leftTab: state.leftTab,
        leftCollapsed: state.leftCollapsed,
        active: true,
        bundleId: action.payload.bundleId,
        bundleName: action.payload.bundleName,
      };
    },
    closeExplorer: (state) => ({
      ...initialState,
      leftTab: state.leftTab,
      leftCollapsed: state.leftCollapsed,
    }),
    setExplorerLeftTab: (state, action: PayloadAction<ExplorerLeftTab>) => {
      state.leftTab = action.payload;
    },
    setExplorerLeftCollapsed: (state, action: PayloadAction<boolean>) => {
      state.leftCollapsed = action.payload;
    },
    selectExplorerRoute: (state, action: PayloadAction<string | null>) => {
      state.selectedRouteId = action.payload;
      if (action.payload === null) state.drawing = null;
    },
    setExplorerDate: (state, action: PayloadAction<string>) => {
      state.date = action.payload;
    },
    setExplorerDrawing: (state, action: PayloadAction<ExplorerRouteDrawing | null>) => {
      state.drawing = action.payload;
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
  setExplorerLeftCollapsed,
  selectExplorerRoute,
  setExplorerDate,
  setExplorerDrawing,
} = explorerSlice.actions;

export const explorerReducer = explorerSlice.reducer;
