/**
 * The open bundle's Explorer data, for every panel of the Explorer: its
 * DuckDB-WASM database and the member layers the map draws from.
 */
import { createContext, useCallback, useContext, useEffect, useMemo, useState } from "react";
import type { ReactNode } from "react";

import { useBundleLayers } from "@/lib/api/bundles";
import type { SqlRunner } from "@/lib/explorer/db/types";
import { networkShapes, networkStore } from "@/lib/explorer/network";
import { DEFAULT_SETTINGS, type ExplorerSettings, loadSettings, saveSettings } from "@/lib/explorer/settings";
import { setExplorerDate } from "@/lib/store/explorer/slice";

import { type ExplorerDbState, FIRST_TABLES, hasTables, useExplorerDb } from "@/hooks/explorer/useExplorerDb";
import { useAppDispatch, useAppSelector } from "@/hooks/store/ContextHooks";

interface ExplorerData {
  state: ExplorerDbState;
  /** The database, once the tables that list the network are loaded. */
  db: SqlRunner | undefined;
  /** The stop times are in too: per-trip figures can be computed. */
  complete: boolean;
  /** The feed's service window, `[first, last]` day. */
  window: [string, string] | null;
  /** The bundle's shape_lines layer, which the map draws routes from. */
  shapeLinesLayerId: string | null;
  /** Share of the snapshot's bytes in the browser, 0–1; null before the download. */
  progress: number | null;
  /** Start a failed open over. */
  retry: () => void;
  /** The feed's `shape_dist_traveled` unit, as the snapshot found it. */
  shapeDistUnit: string | null;
  /** The assumptions figures rest on (settings.ts), and how to change them:
   * false when this browser could not keep them. */
  settings: ExplorerSettings;
  setSettings: (next: ExplorerSettings) => boolean;
}

const Context = createContext<ExplorerData | null>(null);

/** Today if the feed runs then, else the nearest end of its window. */
export function defaultDate(window: [string, string], today = new Date()): string {
  const iso = today.toISOString().slice(0, 10);
  if (iso < window[0]) return window[0];
  if (iso > window[1]) return window[1];
  return iso;
}

export function ExplorerProvider({ bundleId, children }: { bundleId: string; children: ReactNode }) {
  const dispatch = useAppDispatch();
  const date = useAppSelector((s) => s.explorer.date);
  const { state, retry } = useExplorerDb(bundleId);
  const [settings, setSettingsState] = useState<ExplorerSettings>(DEFAULT_SETTINGS);
  useEffect(() => setSettingsState(loadSettings(bundleId)), [bundleId]);
  const setSettings = useCallback(
    (next: ExplorerSettings) => {
      setSettingsState(next);
      return saveSettings(bundleId, next);
    },
    [bundleId]
  );
  const { members } = useBundleLayers(bundleId);

  const manifest = state.status === "loading" || state.status === "ready" ? state.manifest : undefined;
  const window = useMemo<[string, string] | null>(() => {
    const [first, last] = manifest?.service_window ?? [null, null];
    return first && last ? [first, last] : null;
  }, [manifest]);

  useEffect(() => {
    if (window && !date) dispatch(setExplorerDate(defaultDate(window)));
  }, [window, date, dispatch]);

  // The member the map draws shapes from: whichever the snapshot found the
  // lines in (an older import keeps them as its shapes).
  const linesRole = manifest ? (manifest.shape_lines_role ?? null) : undefined;
  const shapeLinesLayerId = linesRole ? (members?.find((m) => m.role === linesRole)?.layer_id ?? null) : null;

  // The network the map draws by itself, once the snapshot is in.
  const db = state.status === "ready" ? state.db : undefined;
  useEffect(() => {
    if (!db) return;
    let current = true;
    networkShapes(db, shapeLinesLayerId).then((n) => current && networkStore.set(n));
    return () => {
      current = false;
      networkStore.reset();
    };
  }, [db, shapeLinesLayerId]);

  const value = useMemo<ExplorerData>(() => {
    const ready = state.status === "loading" || state.status === "ready";
    return {
      state,
      db: ready && hasTables(state, FIRST_TABLES) ? state.db : undefined,
      complete: state.status === "ready",
      window,
      shapeLinesLayerId,
      progress:
        state.status === "ready"
          ? 1
          : state.status === "loading" && state.progress.total
            ? state.progress.received / state.progress.total
            : null,
      retry,
      shapeDistUnit: manifest?.shape_dist_unit ?? null,
      settings,
      setSettings,
    };
  }, [state, window, retry, manifest, settings, setSettings, shapeLinesLayerId]);

  return <Context.Provider value={value}>{children}</Context.Provider>;
}

export function useExplorer(): ExplorerData {
  const value = useContext(Context);
  if (!value) throw new Error("useExplorer outside an ExplorerProvider");
  return value;
}
