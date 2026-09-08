"use client";

import { useMemo, useState } from "react";

import { useSpaces } from "@/lib/api/content";
import { useFavoriteStars } from "@/lib/api/favorites";
import { useTemplateCategories, useTemplates } from "@/lib/api/templates";
import type { TemplateKind, TemplateSourceFilter } from "@/lib/validations/template";

import { useDebouncedValue } from "@/hooks/dashboard/home/useDebouncedValue";

const PAGE_SIZE = 60;
const SEARCH_DEBOUNCE_MS = 200;

interface UseTemplateBrowserStateOptions {
  /** The Workflows/Layouts panels open the browser pinned to one kind and
   * hide the kind pills entirely — the hook still reports that kind so a
   * caller reading `kind` never has to know about the lock. */
  lockedKind?: TemplateKind;
  /** The Catalog Templates tab browses one shelf and offers no way to leave
   * it — the hook reports that source and ignores every request to change
   * it, so a caller reading `source` never has to know about the lock. */
  lockedSource?: TemplateSourceFilter;
  initialSource?: TemplateSourceFilter;
}

/** T7's browser state: search (debounced), kind, source, category tags, and
 * the current page of templates (pinned first). One `useTemplates` call: the
 * kind pills are filters rather than statistics, so nothing needs a second,
 * kind-unfiltered page to count. The tag list beside it is its own request —
 * the categories facet, which knows what the whole shelf holds rather than
 * what one page happens to show. */
export const useTemplateBrowserState = ({
  lockedKind,
  lockedSource,
  initialSource,
}: UseTemplateBrowserStateOptions) => {
  const [search, setSearch] = useState("");
  const [kindState, setKind] = useState<TemplateKind | "all">(lockedKind ?? "all");
  const [sourceState, setSource] = useState<TemplateSourceFilter>(lockedSource ?? initialSource ?? "all");
  const [tags, setTags] = useState<string[]>([]);
  const debouncedSearch = useDebouncedValue(search, SEARCH_DEBOUNCE_MS) || undefined;

  const kind = lockedKind ?? kindState;
  const source = lockedSource ?? sourceState;

  // The categories in use across the whole shelf, with their counts —
  // narrowed by the same source/kind the list is showing, so the options and
  // the counts describe exactly what the list would hold. Counted server-side
  // over every readable template rather than over the loaded page, so a tag
  // only page 2 carries is still offered.
  const {
    categories: facets,
    isLoading: facetsLoading,
    isError: facetsError,
  } = useTemplateCategories({ source, kind: kind === "all" ? undefined : kind });
  // Without the facet there is no trustworthy tag list to filter by, so both
  // the options and the filtering fall back to the loaded page.
  const facetDown = !!facetsError;

  const { page, isLoading } = useTemplates({
    source,
    kind: kind === "all" ? undefined : kind,
    search: debouncedSearch,
    // The list is filtered server-side, over every readable template rather
    // than the page in hand.
    categories: !facetDown && tags.length > 0 ? tags.join(",") : undefined,
    size: PAGE_SIZE,
  });
  const { spaces } = useSpaces();
  const { starred, toggleStar } = useFavoriteStars("template");

  // The fallback tag list: every category on the loaded page with how many
  // templates carry it, most used first. Counted before the tag filter is
  // applied, so selecting one option neither removes the others nor changes
  // their counts.
  const pageTags = useMemo(() => {
    const counts = new Map<string, number>();
    for (const item of page?.items ?? []) {
      for (const category of item.categories ?? []) counts.set(category, (counts.get(category) ?? 0) + 1);
    }
    return [...counts.entries()]
      .map(([tag, count]) => ({ tag, count }))
      .sort((a, b) => b.count - a.count || a.tag.localeCompare(b.tag));
  }, [page]);

  const availableTags = useMemo(
    () => (facetDown ? pageTags : (facets ?? []).map(({ name, count }) => ({ tag: name, count }))),
    [facetDown, facets, pageTags]
  );

  const templates = useMemo(() => {
    // With the facet up, `GET /template` has already applied the tag filter;
    // without it, the same all-of rule is applied here over the loaded page.
    const items = facetDown
      ? (page?.items ?? []).filter((item) => tags.every((tag) => (item.categories ?? []).includes(tag)))
      : (page?.items ?? []);
    const pinned = items.filter((item) => starred[item.id]);
    const rest = items.filter((item) => !starred[item.id]);
    return [...pinned, ...rest];
  }, [facetDown, page, starred, tags]);

  const toggleTag = (tag: string) =>
    setTags((current) =>
      current.includes(tag) ? current.filter((entry) => entry !== tag) : [...current, tag]
    );

  const clearFilters = () => {
    setSource(lockedSource ?? "all");
    setTags([]);
  };

  return {
    search,
    setSearch,
    kind,
    setKind,
    source,
    setSource,
    tags,
    setTags,
    toggleTag,
    availableTags,
    /** The tag list is still on its way — the filter's categories section
     * stands in for it rather than appearing from nowhere. */
    tagsLoading: !facetDown && facetsLoading,
    // What the Filter pill's badge counts: the source, once it is narrower
    // than Everyone, plus one per selected tag. Search and the kind pills
    // carry their own state on screen and are not counted here, and neither
    // is a locked source — nothing can change it, so it is not a filter.
    activeFilterCount: (lockedSource || source === "all" ? 0 : 1) + tags.length,
    clearFilters,
    templates,
    total: page?.total ?? 0,
    isLoading,
    starred,
    toggleStar,
    spaces,
  };
};

export type TemplateBrowserState = ReturnType<typeof useTemplateBrowserState>;
