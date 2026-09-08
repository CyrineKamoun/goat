import { createSlice } from "@reduxjs/toolkit";
import type { PayloadAction } from "@reduxjs/toolkit";

/**
 * A file on its way from the browser to S3.
 *
 * Deliberately not in the jobs slice: this is the phase before a job exists. `/jobs` has
 * never heard of it, none of the OGC statuses describes it, and it cannot survive a reload
 * because the bytes are in the page. The job machinery takes over the moment `jobId` is set.
 */
export interface UploadTransfer {
  /** Client-side only; there is no server id to use yet. */
  id: string;
  fileName: string;
  sent: number;
  total: number;
  /** `starting` covers presign and the create-layer call, where there is nothing to measure. */
  status: "starting" | "uploading" | "handed-off" | "failed";
  jobId?: string;
  error?: string;
}

export interface UploadsState {
  transfers: UploadTransfer[];
}

const initialState = { transfers: [] } as UploadsState;

const uploadsSlice = createSlice({
  name: "uploads",
  initialState,
  reducers: {
    transferStarted: (state, action: PayloadAction<{ id: string; fileName: string; total: number }>) => {
      state.transfers.push({ ...action.payload, sent: 0, status: "starting" });
    },
    transferProgress: (state, action: PayloadAction<{ id: string; sent: number }>) => {
      const transfer = state.transfers.find((entry) => entry.id === action.payload.id);
      if (!transfer) return;
      transfer.sent = action.payload.sent;
      transfer.status = "uploading";
    },
    /** The job exists, so the tray owns it from here; the banner says so, then clears. */
    transferHandedOff: (state, action: PayloadAction<{ id: string; jobId?: string }>) => {
      const transfer = state.transfers.find((entry) => entry.id === action.payload.id);
      if (!transfer) return;
      transfer.status = "handed-off";
      transfer.jobId = action.payload.jobId;
      transfer.sent = transfer.total;
    },
    transferFailed: (state, action: PayloadAction<{ id: string; error?: string }>) => {
      const transfer = state.transfers.find((entry) => entry.id === action.payload.id);
      if (!transfer) return;
      transfer.status = "failed";
      transfer.error = action.payload.error;
    },
    transferCleared: (state, action: PayloadAction<string>) => {
      state.transfers = state.transfers.filter((entry) => entry.id !== action.payload);
    },
  },
});

export const {
  transferStarted,
  transferProgress,
  transferHandedOff,
  transferFailed,
  transferCleared,
} = uploadsSlice.actions;

export const uploadsReducer = uploadsSlice.reducer;
