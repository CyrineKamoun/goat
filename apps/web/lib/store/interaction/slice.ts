import { createSlice } from "@reduxjs/toolkit";
import type { PayloadAction } from "@reduxjs/toolkit";

export interface InteractionEvent {
  type: "group_activated" | "visibility_changed";
  sourceId: number;
  value?: boolean;
  timestamp: number;
}

export interface InteractionState {
  pendingEvent: InteractionEvent | null;
  activeTabOverrides: Record<string, string>;
  suppressEvents: boolean;
}

const initialState: InteractionState = {
  pendingEvent: null,
  activeTabOverrides: {},
  suppressEvents: false,
};

const interactionSlice = createSlice({
  name: "interaction",
  initialState,
  reducers: {
    emitInteractionEvent: (
      state,
      action: PayloadAction<Omit<InteractionEvent, "timestamp">>
    ) => {
      if (state.suppressEvents) return;
      state.pendingEvent = {
        ...action.payload,
        timestamp: Date.now(),
      };
    },
    clearPendingEvent: (state) => {
      state.pendingEvent = null;
    },
    setActiveTabOverride: (
      state,
      action: PayloadAction<{ widgetId: string; tabId: string }>
    ) => {
      state.activeTabOverrides[action.payload.widgetId] = action.payload.tabId;
    },
    clearActiveTabOverride: (state, action: PayloadAction<string>) => {
      delete state.activeTabOverrides[action.payload];
    },
    setSuppressEvents: (state, action: PayloadAction<boolean>) => {
      state.suppressEvents = action.payload;
    },
  },
});

export const {
  emitInteractionEvent,
  clearPendingEvent,
  setActiveTabOverride,
  clearActiveTabOverride,
  setSuppressEvents,
} = interactionSlice.actions;

export const interactionReducer = interactionSlice.reducer;
