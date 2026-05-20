"use client";

import Script from "next/script";

/**
 * Injects the customer's Matomo tracker on a public dashboard.
 *
 * Defence-in-depth on the URL/site_id: the backend already validates
 * https-only, no path, and numeric site IDs. We re-validate here at the
 * trust boundary so a malformed or hostile JSONB row from the DB can't
 * end up in a <script> tag.
 *
 * Sends:
 *   - setDocumentTitle(projectName)        — readable in Matomo's UI
 *   - setCustomDimension(1, projectId)     — per-project breakdown
 *   - trackPageView + enableLinkTracking   — standard
 * We deliberately don't override `setCustomUrl`: each custom domain
 * shows up distinctly in Matomo, and the custom dimension gives the
 * dashboard-level grouping regardless of which URL the visitor used.
 */

interface Props {
  url: string;
  siteId: string;
  projectId: string;
  projectName?: string;
  /** When true, inject `_paq.push(['requireConsent'])` so the tracker
   * defers events until the visitor's consent banner calls
   * `_paq.push(['rememberConsentGiven'])`. */
  requireConsent?: boolean;
}

function isSafeMatomoUrl(u: string): boolean {
  try {
    const parsed = new URL(u);
    if (parsed.protocol !== "https:") return false;
    if (parsed.search || parsed.hash) return false;
    return parsed.pathname === "/" || parsed.pathname === "";
  } catch {
    return false;
  }
}

const SITE_ID_RE = /^\d+$/;

/** Matomo Cloud serves the JS from a separate CDN. Self-hosted serves it
 * from the same instance. Detect by hostname suffix. */
function matomoScriptSrc(base: string): string {
  try {
    const u = new URL(base);
    if (u.hostname.endsWith(".matomo.cloud")) {
      return `https://cdn.matomo.cloud/${u.hostname}/matomo.js`;
    }
  } catch {
    /* fall through */
  }
  return `${base}matomo.js`;
}

export function MatomoTracker({
  url,
  siteId,
  projectId,
  projectName,
  requireConsent = true,
}: Props) {
  if (!isSafeMatomoUrl(url) || !SITE_ID_RE.test(siteId)) {
    return null;
  }
  const base = url.endsWith("/") ? url : `${url}/`;
  const scriptSrc = matomoScriptSrc(base);
  // Stringify values so they're safe to embed in the script body.
  const baseJson = JSON.stringify(base);
  const siteJson = JSON.stringify(siteId);
  const projectIdJson = JSON.stringify(projectId);
  const titleJson = JSON.stringify(projectName ?? "");
  const scriptSrcJson = JSON.stringify(scriptSrc);
  // `requireConsent` MUST be pushed BEFORE `trackPageView`, per Matomo's
  // tracking-consent guide. Otherwise the first pageview races the
  // consent gate and is sent immediately.
  const consentLine = requireConsent ? "_paq.push(['requireConsent']);" : "";

  return (
    <Script id="matomo-tracker" strategy="afterInteractive">
      {`
        var _paq = window._paq = window._paq || [];
        ${consentLine}
        if (${titleJson}) { _paq.push(['setDocumentTitle', ${titleJson}]); }
        _paq.push(['setCustomDimension', 1, ${projectIdJson}]);
        _paq.push(['trackPageView']);
        _paq.push(['enableLinkTracking']);
        (function() {
          var u = ${baseJson};
          _paq.push(['setTrackerUrl', u+'matomo.php']);
          _paq.push(['setSiteId', ${siteJson}]);
          var d = document, g = d.createElement('script'), s = d.getElementsByTagName('script')[0];
          g.async = true; g.src = ${scriptSrcJson}; s.parentNode.insertBefore(g, s);
        })();
      `}
    </Script>
  );
}
