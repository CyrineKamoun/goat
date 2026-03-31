import { createSlice, type PayloadAction } from "@reduxjs/toolkit";

import type { FeatureEditMode, FeatureEditorState, PendingFeature } from "./types";

const MAX_HISTORY_SIZE = 50;

const initialState: FeatureEditorState = {
  activeLayerId: null,
  geometryType: null,
  mode: "select",
  pendingFeatures: {},
  activeFeatureId: null,
  isSaving: false,
  undoStack: [],
  redoStack: [],
};

const featureEditorSlice = createSlice({
  name: "featureEditor",
  initialState,
  reducers: {
    startEditing(
      state,
      action: PayloadAction<{ layerId: string; geometryType: "point" | "line" | "polygon" }>
    ) {
      state.activeLayerId = action.payload.layerId;
      state.geometryType = action.payload.geometryType;
      state.mode = "select";
      state.pendingFeatures = {};
      state.activeFeatureId = null;
      state.isSaving = false;
      state.undoStack = [];
      state.redoStack = [];
    },

    stopEditing(state) {
      state.activeLayerId = null;
      state.geometryType = null;
      state.mode = "select";
      state.pendingFeatures = {};
      state.activeFeatureId = null;
      state.isSaving = false;
      state.undoStack = [];
      state.redoStack = [];
    },

    setMode(state, action: PayloadAction<FeatureEditMode>) {
      state.mode = action.payload;
    },

    addPendingFeature(state, action: PayloadAction<PendingFeature>) {
      state.pendingFeatures[action.payload.id] = action.payload;
      state.activeFeatureId = action.payload.id;
    },

    updatePendingGeometry(
      state,
      action: PayloadAction<{ id: string; geometry: GeoJSON.Geometry }>
    ) {
      const feature = state.pendingFeatures[action.payload.id];
      if (feature) {
        feature.geometry = action.payload.geometry;
      }
    },

    updatePendingProperties(
      state,
      action: PayloadAction<{ id: string; properties: Record<string, unknown> }>
    ) {
      const feature = state.pendingFeatures[action.payload.id];
      if (feature) {
        feature.properties = action.payload.properties;
      }
    },

    setDrawFeatureId(
      state,
      action: PayloadAction<{ id: string; drawFeatureId: string | null }>
    ) {
      const feature = state.pendingFeatures[action.payload.id];
      if (feature) {
        feature.drawFeatureId = action.payload.drawFeatureId;
      }
    },

    commitFeature(state, action: PayloadAction<string>) {
      const feature = state.pendingFeatures[action.payload];
      if (feature) {
        feature.committed = true;
        feature.drawFeatureId = null;
      }
      state.activeFeatureId = null;
    },

    markForDeletion(state, action: PayloadAction<string>) {
      const feature = state.pendingFeatures[action.payload];
      if (feature) {
        feature.action = "delete";
        feature.committed = true;
        feature.drawFeatureId = null;
      }
      state.activeFeatureId = null;
    },

    removePendingFeature(state, action: PayloadAction<string>) {
      delete state.pendingFeatures[action.payload];
      if (state.activeFeatureId === action.payload) {
        state.activeFeatureId = null;
      }
    },

    setActiveFeature(state, action: PayloadAction<string | null>) {
      state.activeFeatureId = action.payload;
    },

    setIsSaving(state, action: PayloadAction<boolean>) {
      state.isSaving = action.payload;
    },

    clearPendingFeatures(state) {
      state.pendingFeatures = {};
      state.activeFeatureId = null;
      state.undoStack = [];
      state.redoStack = [];
    },

    /** Push a full snapshot to the undo stack. Called BEFORE any action. */
    pushSnapshot(state, action: PayloadAction<{ drawFeatures: GeoJSON.FeatureCollection }>) {
      state.undoStack.push({
        pendingFeatures: JSON.parse(JSON.stringify(state.pendingFeatures)),
        activeFeatureId: state.activeFeatureId,
        mode: state.mode,
        drawFeatures: action.payload.drawFeatures,
      });
      if (state.undoStack.length > MAX_HISTORY_SIZE) {
        state.undoStack = state.undoStack.slice(-MAX_HISTORY_SIZE);
      }
      state.redoStack = [];
    },

    /** Undo: restore previous snapshot. The current state is pushed to redo. */
    undo(
      state,
      action: PayloadAction<{ drawFeatures: GeoJSON.FeatureCollection }>
    ) {
      if (state.undoStack.length === 0) return;
      const previous = state.undoStack.pop()!;

      // Save current state to redo stack
      state.redoStack.push({
        pendingFeatures: JSON.parse(JSON.stringify(state.pendingFeatures)),
        activeFeatureId: state.activeFeatureId,
        mode: state.mode,
        drawFeatures: action.payload.drawFeatures,
      });

      // Restore previous state
      state.pendingFeatures = previous.pendingFeatures;
      state.activeFeatureId = previous.activeFeatureId;
      state.mode = previous.mode;
      // drawFeatures are restored by the hook via drawControl
    },

    /** Redo: restore next snapshot. */
    redo(
      state,
      action: PayloadAction<{ drawFeatures: GeoJSON.FeatureCollection }>
    ) {
      if (state.redoStack.length === 0) return;
      const next = state.redoStack.pop()!;

      // Save current state to undo stack
      state.undoStack.push({
        pendingFeatures: JSON.parse(JSON.stringify(state.pendingFeatures)),
        activeFeatureId: state.activeFeatureId,
        mode: state.mode,
        drawFeatures: action.payload.drawFeatures,
      });

      // Restore next state
      state.pendingFeatures = next.pendingFeatures;
      state.activeFeatureId = next.activeFeatureId;
      state.mode = next.mode;
    },
  },
});

export const {
  startEditing,
  stopEditing,
  setMode,
  addPendingFeature,
  updatePendingGeometry,
  updatePendingProperties,
  setDrawFeatureId,
  commitFeature,
  markForDeletion,
  removePendingFeature,
  setActiveFeature,
  setIsSaving,
  clearPendingFeatures,
  pushSnapshot,
  undo,
  redo,
} = featureEditorSlice.actions;

export const featureEditorReducer = featureEditorSlice.reducer;
