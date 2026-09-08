import { createSlice } from "@reduxjs/toolkit";
import type { PayloadAction } from "@reduxjs/toolkit";

export interface JobsState {
  runningJobIds: string[];
}

const initialState = {
  runningJobIds: [],
} as JobsState;

const jobsSlice = createSlice({
  name: "jobs",
  initialState: initialState,
  reducers: {
    setRunningJobIds: (state, action: PayloadAction<string[]>) => {
      state.runningJobIds = action.payload;
    },
    /** Append without reading the array first: two concurrent uploads each
     * held a stale copy and the second one's `set` dropped the first's job. */
    addRunningJobIds: (state, action: PayloadAction<string[]>) => {
      for (const id of action.payload) {
        if (!state.runningJobIds.includes(id)) state.runningJobIds.push(id);
      }
    },
  },
});

export const { setRunningJobIds, addRunningJobIds } = jobsSlice.actions;

export const jobsReduces = jobsSlice.reducer;
