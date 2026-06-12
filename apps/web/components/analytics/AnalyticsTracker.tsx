"use client";

/**
 * Dispatch a public-dashboard analytics config to the right tracker
 * component. Returns null when the project has no analytics configured
 * (org has no provider set, or the project hasn't opted in).
 */

import { MatomoTracker } from "./MatomoTracker";

export interface AnalyticsConfig {
  provider: string;
  config: Record<string, unknown>;
}

interface Props {
  analytics: AnalyticsConfig | null | undefined;
  projectId: string;
  projectName?: string;
  /** When true, the injected tracker calls `requireConsent` so no events
   * fire until the visitor's consent banner grants permission. Defaults
   * to true (safe for GDPR/TDDDG); explicit false disables the gate. */
  requireConsent?: boolean;
}

export function AnalyticsTracker({
  analytics,
  projectId,
  projectName,
  requireConsent = true,
}: Props) {
  if (!analytics) return null;
  switch (analytics.provider) {
    case "matomo": {
      const url = typeof analytics.config.url === "string" ? analytics.config.url : null;
      const siteId =
        typeof analytics.config.site_id === "string" ? analytics.config.site_id : null;
      if (!url || !siteId) return null;
      return (
        <MatomoTracker
          url={url}
          siteId={siteId}
          projectId={projectId}
          projectName={projectName}
          requireConsent={requireConsent}
        />
      );
    }
    default:
      return null;
  }
}
