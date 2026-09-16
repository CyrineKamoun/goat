import { useEffect, useRef, useState } from "react";

import { patchPreferences, usePreferences } from "@/lib/api/preferences";
import type { UserPreferences } from "@/lib/validations/home";

import type { HomeStage } from "@/hooks/dashboard/home/useHomeStage";

/** The Welcome's entry in `spotlight_seen`, beside the release spotlight ids; namespaced so no changelog `translationKey` can ever collide with it. */
export const WELCOME_ID = "welcome:v1";

/**
 * The first-run Welcome shows once per user, ever: only in the New stage,
 * and it is recorded as seen the moment it is displayed rather than when it
 * is dismissed, so closing it any way — Skip, Escape, the backdrop, or a
 * reload — is final. The record lives in the caller's preferences, so it
 * holds across browsers and devices.
 */
export const useWelcome = (
  stage: HomeStage | undefined,
  preferences: UserPreferences | undefined,
  options: {
    /** Open regardless of stage or history and record nothing (`?preview=welcome`). */
    preview?: boolean;
    /** Another announcement has the screen — never open on top of it, and record nothing. */
    suppress?: boolean;
  } = {}
): { open: boolean; dismiss: () => void } => {
  const { mutate } = usePreferences();
  const [open, setOpen] = useState(false);
  const claimedRef = useRef(false);

  useEffect(() => {
    if (claimedRef.current || options.suppress) return;
    if (options.preview) {
      claimedRef.current = true;
      setOpen(true);
      return;
    }
    if (stage !== "new" || !preferences) return;
    if (preferences.spotlight_seen.includes(WELCOME_ID)) return;

    claimedRef.current = true;
    setOpen(true);
    void patchPreferences({ spotlight_seen: [...preferences.spotlight_seen, WELCOME_ID] })
      .then(() => mutate())
      .catch(() => {
        // Recording failed: the Welcome shows again next time, which is the
        // safer failure for a one-time screen.
      });
  }, [stage, preferences, mutate, options.preview, options.suppress]);

  return { open, dismiss: () => setOpen(false) };
};
