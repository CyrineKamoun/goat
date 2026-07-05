import { createSlice } from "@reduxjs/toolkit";
import type { PayloadAction } from "@reduxjs/toolkit";

export interface InteractionEvent {
  type: "group_activated" | "visibility_changed";
  sourceId: number;
  value?: boolean;
  timestamp: number;
}

export interface InteractionState {
  // Queue of events awaiting dispatch. A single slot would drop events when
  // several are emitted synchronously (e.g. the "Show all" master toggle emits
  // one per layer), because only the last would survive to the dispatcher.
  pendingEvents: InteractionEvent[];
  activeTabOverrides: Record<string, string>;
  suppressEvents: boolean;
}

const initialState: InteractionState = {
  pendingEvents: [],
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
      state.pendingEvents.push({
        ...action.payload,
        timestamp: Date.now(),
      });
    },
    clearPendingEvents: (state) => {
      state.pendingEvents = [];
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
  clearPendingEvents,
  setActiveTabOverride,
  clearActiveTabOverride,
  setSuppressEvents,
} = interactionSlice.actions;

export const interactionReducer = interactionSlice.reducer;
