"use client";

import { useParams, useRouter, useSearchParams } from "next/navigation";
import { useEffect, useMemo } from "react";

import { useSpaces } from "@/lib/api/content";

import { contentPath } from "@/hooks/dashboard/content/useContentPageState";
import type { ContentView } from "@/hooks/dashboard/content/useContentPageState";

import ContentPage from "@/components/dashboard/content/ContentPage";

/** The cross-space views own a path segment each. A space id is a UUID, so
 * neither word can ever collide with one — reading the first segment against
 * this map before treating it as a space id is unambiguous. */
const VIEW_BY_SEGMENT: Record<string, ContentView> = {
  shared: "shared_with_me",
  recent: "recent",
};

type ContentLocation = { spaceId?: string; folderId?: string; view?: ContentView };

/** `[]` → no location yet (the redirect below resolves one), `["shared"]` /
 * `["recent"]` → a cross-space view, `[spaceId]` → a space's root,
 * `[spaceId, folderId]` → a folder inside it. */
const locationFromPath = (segments: string[]): ContentLocation => {
  const view = segments.length > 0 ? VIEW_BY_SEGMENT[segments[0]] : undefined;
  if (view) return { view };
  return { spaceId: segments[0], folderId: segments[1] };
};

/**
 * Every Content address is this one route: `/content`, `/content/{spaceId}`,
 * `/content/{spaceId}/{folderId}`, `/content/shared` and `/content/recent`.
 * One segment for all of them means walking into a folder re-renders the page
 * with new props instead of swapping route files, so what the page holds —
 * which sections are open, what is selected, whether the details panel is
 * showing — survives the navigation.
 *
 * `/content` has no location of its own: it stands for "my content" and
 * redirects to the location the query asks for — a `?space=`/`?folder=`/
 * `?view=` address, which is how the page used to be linked, or the personal
 * space otherwise — carrying the rest of the query (`?types=…` from Home's
 * "See all", a search) along. `layout` is dropped with them: grid-vs-list is
 * kept per browser, not in the URL. The page shell renders meanwhile so the
 * redirect is not a blank screen.
 */
const ContentRoute = () => {
  const router = useRouter();
  const params = useSearchParams();
  const route = useParams<{ path?: string[] }>();
  const { spaces } = useSpaces();

  const segments = useMemo(() => route.path ?? [], [route.path]);
  const location = useMemo(() => locationFromPath(segments), [segments]);

  useEffect(() => {
    if (segments.length > 0) return;

    const legacyView = params.get("view");
    const view: ContentView | undefined =
      legacyView === "shared_with_me" || legacyView === "recent" ? legacyView : undefined;
    const spaceId = params.get("space") ?? spaces.find((space) => space.kind === "personal")?.id;
    if (!view && !spaceId) return;

    const rest = new URLSearchParams(params.toString());
    ["space", "folder", "view", "layout"].forEach((key) => rest.delete(key));
    const qs = rest.toString();
    const path = contentPath({ spaceId, folderId: params.get("folder") ?? undefined, view });
    router.replace(qs ? `${path}?${qs}` : path);
  }, [segments, spaces, params, router]);

  return <ContentPage spaceId={location.spaceId} folderId={location.folderId} view={location.view} />;
};

export default ContentRoute;
