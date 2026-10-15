/**
 * The playback clock's frames: the time playback shows and where the
 * network's vehicles are then. Kept outside React and Redux, so a frame
 * reaches only what draws it — the bar's clock and the map — and the panels
 * do not re-render many times a second. The map reads it from outside the
 * Explorer's provider, as it reads the route drawing from the store.
 */
import type { FeatureCollection, Point } from "geojson";

export interface VehicleProps {
  run: number;
  route_id: string;
  /** `#RRGGBB`, the route's or its mode's. */
  color: string;
  /** What the vehicle's badge reads: the route's number. */
  label: string;
}

export interface PlaybackFrame {
  /** Service-day seconds; null before the day's service is known. */
  time_s: number | null;
  vehicles: FeatureCollection<Point, VehicleProps>;
  /** The day's first departure and last arrival, which bound the scrubber. */
  span: { first_s: number; last_s: number } | null;
}

const EMPTY: PlaybackFrame = {
  time_s: null,
  vehicles: { type: "FeatureCollection", features: [] },
  span: null,
};

let frame: PlaybackFrame = EMPTY;
const listeners = new Set<() => void>();

export const playbackClock = {
  subscribe(listener: () => void): () => void {
    listeners.add(listener);
    return () => listeners.delete(listener);
  },
  getSnapshot(): PlaybackFrame {
    return frame;
  },
  publish(next: Partial<PlaybackFrame>): void {
    frame = { ...frame, ...next };
    listeners.forEach((l) => l());
  },
  /** No playback: the Explorer closed, or another bundle opened. */
  reset(): void {
    frame = EMPTY;
    listeners.forEach((l) => l());
  },
};
