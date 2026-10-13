/**
 * A bundle's Explorer database: built on the server if need be, downloaded,
 * and loaded into DuckDB-WASM. One per bundle while the Explorer is open,
 * released when it closes.
 */
import { useEffect, useState } from "react";

import type { ExplorerDb } from "@/lib/explorer/db/ExplorerDb";
import type { SnapshotManifest } from "@/lib/explorer/db/types";
import { FIRST_TABLES, type LoadProgress, loadSnapshot, waitForManifest } from "@/lib/explorer/snapshot";

export type ExplorerDbState =
  | { status: "preparing" }
  | { status: "loading"; progress: LoadProgress; db: ExplorerDb; manifest: SnapshotManifest }
  | { status: "ready"; db: ExplorerDb; manifest: SnapshotManifest }
  | { status: "error"; error: string };

/** Whether the tables a view needs are loaded. */
export function hasTables(state: ExplorerDbState, tables: string[]): boolean {
  if (state.status === "ready") return true;
  if (state.status !== "loading") return false;
  return tables.every((t) => state.progress.loaded.includes(t));
}

export function useExplorerDb(bundleId: string | undefined): ExplorerDbState {
  const [state, setState] = useState<ExplorerDbState>({ status: "preparing" });

  useEffect(() => {
    if (!bundleId) return;
    const abort = new AbortController();
    let db: ExplorerDb | undefined;
    setState({ status: "preparing" });

    (async () => {
      const manifest = await waitForManifest(bundleId, undefined, abort.signal);
      const { ExplorerDb } = await import("@/lib/explorer/db/ExplorerDb");
      db = await ExplorerDb.open();
      if (abort.signal.aborted) return;
      const opened = db;
      await loadSnapshot(
        opened,
        manifest,
        (progress) => {
          if (!abort.signal.aborted) setState({ status: "loading", progress, db: opened, manifest });
        },
        abort.signal
      );
      if (!abort.signal.aborted) setState({ status: "ready", db: opened, manifest });
    })().catch((error: unknown) => {
      if (abort.signal.aborted) return;
      setState({ status: "error", error: error instanceof Error ? error.message : String(error) });
    });

    return () => {
      abort.abort();
      void db?.close();
    };
  }, [bundleId]);

  return state;
}

export { FIRST_TABLES };
