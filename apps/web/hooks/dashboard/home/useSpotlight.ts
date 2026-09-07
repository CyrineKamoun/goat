import { useEffect, useRef, useState } from "react";

import { patchPreferences, usePreferences } from "@/lib/api/preferences";
import { nextSpotlight } from "@/lib/api/releases";
import type { ReleaseEntry, UserPreferences } from "@/lib/validations/home";

import type { HomeStage } from "@/hooks/dashboard/home/useHomeStage";
import { useAppSelector } from "@/hooks/store/ContextHooks";

/** Guards "at most one spotlight per session" across every mount of this
 * hook in the tab, not just re-renders of one component instance. */
const SPOTLIGHT_SESSION_KEY = "goat.spotlight.shown";

const alreadyShownThisSession = (): boolean => {
  try {
    return sessionStorage.getItem(SPOTLIGHT_SESSION_KEY) !== null;
  } catch {
    // Private browsing or a disabled sessionStorage: fail open to "not shown"
    // rather than crash — the modal still shows once for this render tree.
    return false;
  }
};

const claimSession = (id: string): void => {
  try {
    sessionStorage.setItem(SPOTLIGHT_SESSION_KEY, id);
  } catch {
    // See alreadyShownThisSession — nothing to persist, the in-memory state
    // below still keeps this instance from showing a second entry.
  }
};

/**
 * H7's spotlight rules: never in the New stage (or while the stage is still
 * loading), never while a job is running, at most one per session, and never
 * an entry the caller already dismissed (`spotlight_seen`). Once an entry
 * clears every rule it is held in state until `dismiss` is called, so a
 * later change to `runningJobIds` or `stage` cannot yank the modal away
 * mid-read.
 */
export const useSpotlight = (
  entries: ReleaseEntry[],
  preferences: UserPreferences | undefined,
  stage: HomeStage | undefined
): { entry: ReleaseEntry | undefined; dismiss: () => Promise<void> } => {
  const runningJobIds = useAppSelector((state) => state.jobs.runningJobIds);
  // A file transfer lives in its own slice until the upload hands off to a
  // job, so "nothing running" has to look at both.
  const transfers = useAppSelector((state) => state.uploads.transfers);
  const uploading = transfers.some((t) => t.status === "starting" || t.status === "uploading");
  const { mutate } = usePreferences();

  const [entry, setEntry] = useState<ReleaseEntry | undefined>(undefined);
  const claimedRef = useRef(false);

  useEffect(() => {
    if (claimedRef.current) return;
    if (stage === undefined || stage === "new") return;
    if (runningJobIds.length > 0 || uploading) return;
    if (!preferences) return;
    if (alreadyShownThisSession()) return;

    const candidate = nextSpotlight(entries, preferences.spotlight_seen);
    if (!candidate) return;

    claimedRef.current = true;
    claimSession(candidate.id);
    setEntry(candidate);
  }, [entries, preferences, stage, runningJobIds.length, uploading]);

  const dismiss = async (): Promise<void> => {
    if (!entry) return;
    const seen = preferences?.spotlight_seen ?? [];
    await patchPreferences({ spotlight_seen: [...seen, entry.id] });
    await mutate();
    setEntry(undefined);
  };

  return { entry, dismiss };
};
