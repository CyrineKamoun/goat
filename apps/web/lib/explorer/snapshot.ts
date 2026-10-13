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

export type ManifestAnswer = SnapshotManifest | SnapshotPending;

export async function fetchManifest(bundleId: string): Promise<ManifestAnswer> {
  const res = await apiRequestAuth(`${GEOAPI_BASE_URL}/bundles/${bundleId}/explorer`);
  if (res.status === 200 || res.status === 202 || res.status === 500) {
    return (await res.json()) as ManifestAnswer;
  }
  throw new Error(`Explorer snapshot: ${res.status} ${res.statusText}`);
}

/** Poll until the snapshot is built: every second at first, every five later. */
export async function waitForManifest(
  bundleId: string,
  onPending?: (pending: SnapshotPending) => void,
  signal?: AbortSignal
): Promise<SnapshotManifest> {
  let delay = 1000;
  for (;;) {
    const answer = await fetchManifest(bundleId);
    if (answer.status === "ready") return answer;
    if (answer.status === "failed") {
      throw new Error(answer.detail || "The snapshot could not be built");
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
  if (!res.ok || !res.body) throw new Error(`${file.name}: ${res.status} ${res.statusText}`);
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
