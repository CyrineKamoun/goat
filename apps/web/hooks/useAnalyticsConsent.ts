"use client";

import { useCallback, useEffect, useState } from "react";

/**
 * Visitor's analytics consent state, persisted in localStorage so the
 * decision survives reloads. Three states:
 *
 *   "granted" — visitor accepted; tracker fires events.
 *   "denied"  — visitor declined; tracker stays gated.
 *   null      — visitor hasn't decided yet; banner is shown.
 *
 * localStorage is per-origin, so consent on `klima.ministry.de` doesn't
 * leak to `goat.dev.plan4better.de` or vice versa. That's the correct
 * GDPR scope: each domain asks for its own consent.
 */

export type ConsentDecision = "granted" | "denied";

const STORAGE_KEY = "goat-analytics-consent";

function readDecision(): ConsentDecision | null {
  if (typeof window === "undefined") return null;
  try {
    const v = window.localStorage.getItem(STORAGE_KEY);
    return v === "granted" || v === "denied" ? v : null;
  } catch {
    // localStorage can throw in private modes / quota errors.
    return null;
  }
}

function rememberConsent(decision: ConsentDecision) {
  if (typeof window === "undefined") return;
  try {
    window.localStorage.setItem(STORAGE_KEY, decision);
  } catch {
    /* ignore */
  }
}

// Push to Matomo's _paq queue. The MatomoTracker already arranged for
// `requireConsent` to run BEFORE `trackPageView`, so events are queued
// pending consent. `rememberConsentGiven` flushes the queue once granted.
function pushMatomoConsent(decision: ConsentDecision) {
  if (typeof window === "undefined") return;
  const w = window as unknown as { _paq?: unknown[] };
  if (!w._paq) w._paq = [];
  if (decision === "granted") {
    w._paq.push(["rememberConsentGiven"]);
  }
  // "denied" needs no Matomo call — the tracker stays in requireConsent
  // state, no events leave the visitor's browser.
}

export function useAnalyticsConsent() {
  const [decision, setDecision] = useState<ConsentDecision | null>(null);

  useEffect(() => {
    // Initial read on mount. On a return visit with a granted decision,
    // we silently re-push to Matomo so the tracker honours the stored
    // consent without re-prompting.
    const stored = readDecision();
    setDecision(stored);
    if (stored === "granted") {
      pushMatomoConsent("granted");
    }
  }, []);

  const grant = useCallback(() => {
    rememberConsent("granted");
    pushMatomoConsent("granted");
    setDecision("granted");
  }, []);

  const deny = useCallback(() => {
    rememberConsent("denied");
    setDecision("denied");
  }, []);

  return { decision, grant, deny };
}
