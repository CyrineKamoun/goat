import { useEffect, useRef, useState } from "react";

import { patchPreferences, usePreferences } from "@/lib/api/preferences";
import { nextSpotlight } from "@/lib/api/releases";
import type { ReleaseEntry, UserPreferences } from "@/lib/validations/home";

import type { HomeStage } from "@/hooks/dashboard/home/useHomeStage";
import { useAppSelector } from "@/hooks/store/ContextHooks";

/** The entry a `?preview=spotlight[:id]` asks for: that id, else the newest flagged one, else the newest entry. */
const previewCandidate = (entries: ReleaseEntry[], preview: string): ReleaseEntry | undefined => {
  const newestFirst = [...entries].sort((a, b) => new Date(b.date).getTime() - new Date(a.date).getTime());
  if (preview !== "*") return newestFirst.find((entry) => entry.id === preview);
  return newestFirst.find((entry) => entry.spotlight) ?? newestFirst[0];
};

/**
 * H7's spotlight rules: never in the New stage (or while the stage is still
 * loading), never while a job is running, one per page load, and never an
 * entry the caller already dismissed (`spotlight_seen`). An entry the caller
 * closed without acting on comes back on the next load, until they do. Once
 * an entry clears every rule it is held in state until `dismiss` is called,
 * so a later change to `runningJobIds` or `stage` cannot yank the modal away
 * mid-read.
 */
export const useSpotlight = (
  entries: ReleaseEntry[],
  preferences: UserPreferences | undefined,
  stage: HomeStage | undefined,
  options: {
    /** `"*"` reopens the newest spotlight, an id that entry, regardless of
     * every rule above and without recording anything (`?preview=spotlight`). */
    preview?: string | null;
    /** Another announcement has the screen — never open on top of it. */
    suppress?: boolean;
  } = {}
): { entry: ReleaseEntry | undefined; dismiss: () => Promise<void> } => {
  const runningJobIds = useAppSelector((state) => state.jobs.runningJobIds);
  // A file transfer lives in its own slice until the upload hands off to a
  // job, so "nothing running" has to look at both.
  const transfers = useAppSelector((state) => state.uploads.transfers);
  const uploading = transfers.some((t) => t.status === "starting" || t.status === "uploading");
  const { mutate } = usePreferences();

  const [entry, setEntry] = useState<ReleaseEntry | undefined>(undefined);
  const claimedRef = useRef(false);
  const previewingRef = useRef(false);

  useEffect(() => {
    if (claimedRef.current || options.suppress) return;
    if (options.preview) {
      const candidate = previewCandidate(entries, options.preview);
      if (!candidate) return;
      claimedRef.current = true;
      previewingRef.current = true;
      setEntry(candidate);
      return;
    }
    if (stage === undefined || stage === "new") return;
    if (runningJobIds.length > 0 || uploading) return;
    if (!preferences) return;

    const candidate = nextSpotlight(entries, preferences.spotlight_seen);
    if (!candidate) return;

    claimedRef.current = true;
    setEntry(candidate);
  }, [entries, preferences, stage, runningJobIds.length, uploading, options.preview, options.suppress]);

  const dismiss = async (): Promise<void> => {
    if (!entry) return;
    if (previewingRef.current) {
      setEntry(undefined);
      return;
    }
    const seen = preferences?.spotlight_seen ?? [];
    await patchPreferences({ spotlight_seen: [...seen, entry.id] });
    await mutate();
    setEntry(undefined);
  };

  return { entry, dismiss };
};
