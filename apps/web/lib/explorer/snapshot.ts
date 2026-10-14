/**
 * Getting a bundle's Explorer snapshot into the browser.
 *
 * geoapi builds the snapshot on the first request for a bundle revision and
 * answers 202 meanwhile; after that the manifest lists the files, whose URLs
 * name the revision and are cached for good. Files load smallest first, so
 * the route list is there while the stop times are still on their way.
 */
import { apiRequestAuth } from "@/lib/api/fetcher";
import { GEOAPI_BASE_URL } from "@/lib/constants";
import type { ExplorerDb } from "@/lib/explorer/db/ExplorerDb";
import type { SnapshotFile, SnapshotManifest, SnapshotPending } from "@/lib/explorer/db/types";
import { ExplorerLoadError, kindOfBuildFailure, kindOfStatus } from "@/lib/explorer/errors";

export type ManifestAnswer = SnapshotManifest | SnapshotPending;

/** The manifest, or word that it is being built or failed. `retry` asks the
 * server to forget a failed build and build again. */
export async function fetchManifest(bundleId: string, retry = false): Promise<ManifestAnswer> {
  const query = retry ? "?retry=true" : "";
  const res = await apiRequestAuth(`${GEOAPI_BASE_URL}/bundles/${bundleId}/explorer${query}`);
  if (res.status === 200 || res.status === 202) return (await res.json()) as ManifestAnswer;
  // A failed build answers 500 with its reason; a 500 without one is the server's.
  if (res.status === 500) {
    const answer = (await res.json().catch(() => null)) as ManifestAnswer | null;
    if (answer?.status === "failed") return answer;
  }
  throw new ExplorerLoadError(kindOfStatus(res.status), `Explorer snapshot: ${res.status} ${res.statusText}`);
}

/** Poll until the snapshot is built: every second at first, every five later. */
export async function waitForManifest(
  bundleId: string,
  onPending?: (pending: SnapshotPending) => void,
  signal?: AbortSignal,
  retry = false
): Promise<SnapshotManifest> {
  let delay = 1000;
  for (let first = true; ; first = false) {
    const answer = await fetchManifest(bundleId, retry && first);
    if (answer.status === "ready") return answer;
    if (answer.status === "failed") {
      const detail = answer.detail || "The snapshot could not be built";
      throw new ExplorerLoadError(kindOfBuildFailure(detail), detail);
    }
    onPending?.(answer);
    await new Promise((resolve, reject) => {
      const timer = setTimeout(resolve, delay);
      signal?.addEventListener("abort", () => {
        clearTimeout(timer);
        reject(new DOMException("Aborted", "AbortError"));
      });
    });
    delay = Math.min(delay * 1.5, 5000);
  }
}

/** One file's bytes, reporting how many have arrived. */
async function download(
  file: SnapshotFile,
  onBytes: (received: number) => void,
  signal?: AbortSignal
): Promise<Uint8Array> {
  const res = await apiRequestAuth(`${GEOAPI_BASE_URL}${file.url}`, { signal });
  if (!res.ok || !res.body) {
    // A file is named for its revision and never changes: a missing one was
    // cleaned up under a long load, and a fresh manifest finds its successor.
    const kind = res.status >= 500 ? "unreachable" : "other";
    throw new ExplorerLoadError(kind, `${file.name}: ${res.status} ${res.statusText}`);
  }
  const reader = res.body.getReader();
  const out = new Uint8Array(file.bytes);
  let received = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    out.set(value, received);
    received += value.length;
    onBytes(received);
  }
  return received === out.length ? out : out.slice(0, received);
}

export interface LoadProgress {
  received: number;
  total: number;
  /** Tables loaded so far. */
  loaded: string[];
}

/** The files that list the network, before which nothing can be shown. */
export const FIRST_TABLES = ["agencies", "routes", "stops", "services", "calendar_dates"];

/**
 * Download and load every file of `manifest` into `db`, three at a time,
 * smallest first. `onTable` is called as each table becomes queryable.
 */
export async function loadSnapshot(
  db: ExplorerDb,
  manifest: SnapshotManifest,
  onProgress: (progress: LoadProgress) => void,
  signal?: AbortSignal
): Promise<void> {
  const files = [...manifest.files].sort((a, b) => a.bytes - b.bytes);
  const total = files.reduce((n, f) => n + f.bytes, 0);
  const received = new Map<string, number>();
  const loaded: string[] = [];
  const report = () =>
    onProgress({ received: [...received.values()].reduce((a, b) => a + b, 0), total, loaded: [...loaded] });

  let next = 0;
  const worker = async () => {
    while (next < files.length) {
      const file = files[next++];
      const bytes = await download(
        file,
        (n) => {
          received.set(file.name, n);
          report();
        },
        signal
      );
      await db.loadParquet(file.name, bytes);
      loaded.push(file.name);
      report();
    }
  };
  await Promise.all([worker(), worker(), worker()]);
}
