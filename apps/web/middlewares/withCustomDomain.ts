/**
 * If the incoming Host header is not the canonical app host, try to
 * resolve it as a customer's custom domain and rewrite the request to
 * the public-dashboard route for the assigned project.
 *
 * Runs first in the stack — withAuth's `publicPaths` ("/map/public")
 * exemption then naturally permits the rewritten request.
 *
 * Unknown hosts fall through to existing routing (typically a 404 from
 * Next.js for an unrecognized origin), which is the desired behavior:
 * we never silently serve someone else's content from an unbound host.
 */

import type { NextRequest } from "next/server";
import { NextResponse } from "next/server";

import { lookupCustomDomain } from "@/lib/api/customDomainLookup";
import type { MiddlewareFactory } from "@/middlewares/types";

function deriveCanonicalHost(): string | null {
  const url = process.env.NEXT_PUBLIC_APP_URL;
  if (!url) return null;
  try {
    // hostname (port stripped) — the request host below is compared
    // port-stripped too, and lib/pwa/manifest.ts#isCustomDomainHost
    // must agree with this check.
    return new URL(url).hostname.toLowerCase();
  } catch {
    return null;
  }
}

export const withCustomDomain: MiddlewareFactory = (next) => {
  return async (request: NextRequest, _next) => {
    const host = request.headers.get("host")?.split(":")[0]?.toLowerCase();
    const canonical = deriveCanonicalHost();

    // Canonical host or no Host header → continue normally.
    if (!host || !canonical || host === canonical) {
      return next(request, _next);
    }

    // Static assets and API routes must not be rewritten — they're
    // served verbatim by Next.js / the routing layer. Only the page
    // route(s) get redirected to /map/public/<projectId>.
    const path = request.nextUrl.pathname;
    if (
      path.startsWith("/_next/") ||
      path.startsWith("/api/") ||
      path === "/favicon.ico" ||
      // any path with a file extension is almost certainly a static
      // asset (woff2, png, css, js, json, ico, svg, ...).
      /\.[a-z0-9]{2,5}$/i.test(path)
    ) {
      return next(request, _next);
    }

    const projectId = await lookupCustomDomain(host);
    if (!projectId) {
      // Unknown custom host: let the rest of the stack handle it.
      return next(request, _next);
    }

    // Rewrite to the public-dashboard route. Preserve any sub-path
    // and query string — they should keep working on the customer
    // domain just like on the canonical one.
    const url = request.nextUrl.clone();
    const trailing = url.pathname === "/" ? "" : url.pathname;
    url.pathname = `/map/public/${projectId}${trailing}`;
    return NextResponse.rewrite(url);
  };
};
