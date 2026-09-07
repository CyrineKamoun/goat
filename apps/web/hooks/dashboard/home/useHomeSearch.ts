"use client";

import { useRouter } from "next/navigation";
import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";

import { ICON_NAME } from "@p4b/ui/components/Icon";

import { useCatalogDatasets } from "@/lib/api/catalog";
import { useContent } from "@/lib/api/content";
import { iconFor, typeLabelKey } from "@/lib/utils/content";
import type { ContentItem } from "@/lib/validations/content";

import { contentPath } from "@/hooks/dashboard/content/useContentPageState";
import { useDebouncedValue } from "@/hooks/dashboard/home/useDebouncedValue";

export type SearchRow =
  | { kind: "head"; label: string }
  /** A caption the user reads rather than picks — what a group is waiting for. */
  | { kind: "note"; label: string }
  | { kind: "item"; id: string; icon: ICON_NAME; label: string; meta: string; go: () => void };

/** Which stores a query is put to. `all` unless the query opens with a prefix. */
export type SearchScope = "all" | "project" | "dataset" | "catalog";

const SCOPE_BY_PREFIX: Record<string, SearchScope> = {
  "project:": "project",
  "dataset:": "dataset",
  "catalog:": "catalog",
};

const SCOPE_PREFIXES = Object.keys(SCOPE_BY_PREFIX);

/** The group a scope stands in for, and what it asks the user for while empty. */
const SCOPE_GROUP: Record<Exclude<SearchScope, "all">, { head: string; prompt: string }> = {
  project: { head: "projects", prompt: "search_type_to_search_projects" },
  dataset: { head: "datasets", prompt: "search_type_to_search_datasets" },
  catalog: { head: "catalog", prompt: "search_type_to_search_catalog" },
};

/**
 * Splits a leading `project:` / `dataset:` / `catalog:` off the query. Anything
 * else — including a colon further in — is the query itself and searches
 * everything.
 */
export const parseScope = (raw: string): { scope: SearchScope; query: string } => {
  const trimmed = raw.trimStart();
  const lower = trimmed.toLowerCase();
  for (const prefix of SCOPE_PREFIXES) {
    if (lower.startsWith(prefix)) {
      return { scope: SCOPE_BY_PREFIX[prefix], query: trimmed.slice(prefix.length).trim() };
    }
  }
  return { scope: "all", query: raw.trim() };
};

/** A recent place, kept in `localStorage` — pushed to on every `go()`. A
 * layer carries its id too: picking it opens the preview dialog in place,
 * the way the Content page opens a layer, instead of leaving Home. */
type RecentPlace = { id: string; label: string; meta: string; href: string; layerId?: string };

const RECENT_KEY = "goat.home.recent";
const MAX_RECENT = 5;
/** Below this the query is too thin to spend a request on. */
export const MIN_QUERY_LENGTH = 2;
const DEBOUNCE_MS = 200;

const readRecent = (): RecentPlace[] => {
  if (typeof window === "undefined") return [];
  try {
    const raw = window.localStorage.getItem(RECENT_KEY);
    if (!raw) return [];
    const parsed: unknown = JSON.parse(raw);
    if (!Array.isArray(parsed)) return [];
    // A layer has no page of its own, so an entry stored under the old route
    // would push a URL that no longer resolves.
    return (parsed as RecentPlace[]).filter((place) => !place.href?.startsWith("/datasets/"));
  } catch {
    return [];
  }
};

const writeRecent = (entry: RecentPlace): void => {
  if (typeof window === "undefined") return;
  try {
    const rest = readRecent().filter((place) => place.id !== entry.id);
    window.localStorage.setItem(RECENT_KEY, JSON.stringify([entry, ...rest].slice(0, MAX_RECENT)));
  } catch {
    // Storage refused the write; the recent list just stays as it was.
  }
};

/**
 * The hero search's fan-out (H3): one `useContent` call for the user's
 * projects and datasets and one `useCatalogDatasets` call for the public
 * catalog, run in parallel on every query and shown as separate groups rather
 * than merged into one ranking — the two stores are different databases with
 * different relevance, so a merged order would be invented. A scope prefix
 * narrows which of the two is asked at all.
 *
 * Groups that have nothing in them are left out entirely, headers included.
 */
export const useHomeSearch = (
  raw: string
): { rows: SearchRow[]; isLoading: boolean; previewLayerId: string | null; closePreview: () => void } => {
  const router = useRouter();
  const { t } = useTranslation("common");
  const debounced = useDebouncedValue(raw, DEBOUNCE_MS);
  const { scope, query } = parseScope(debounced);
  const hasQuery = query.length >= MIN_QUERY_LENGTH;
  const asksContent = hasQuery && scope !== "catalog";
  const asksCatalog = hasQuery && (scope === "all" || scope === "catalog");

  const { page, isLoading: contentLoading } = useContent(
    asksContent ? { view: "recent", search: query, size: 8 } : null
  );
  const { datasets: catalogDatasets, isLoading: catalogLoading } = useCatalogDatasets(
    { q: query, limit: 5 },
    asksCatalog
  );

  const [recent, setRecent] = useState<RecentPlace[]>([]);
  // Hydrated after mount only: reading `localStorage` during the first
  // render would answer differently on the server (nothing) and the client
  // (whatever is stored), which is a hydration mismatch.
  useEffect(() => {
    setRecent(readRecent());
  }, []);

  const [previewLayerId, setPreviewLayerId] = useState<string | null>(null);

  const activate = (entry: RecentPlace) => {
    writeRecent(entry);
    if (entry.layerId) {
      setPreviewLayerId(entry.layerId);
      return;
    }
    router.push(entry.href);
  };

  const contentItemRow = (item: ContentItem): SearchRow => {
    const meta = t(typeLabelKey(item));
    // Each row opens the thing itself, as a click on the Content page does:
    // a project in the builder, a bundle on its page, a layer in the preview
    // dialog; anything else lands in its space.
    const href =
      item.type === "project"
        ? `/map/${item.id}`
        : item.type === "bundle"
          ? `/bundles/${item.id}`
          : contentPath({ spaceId: item.space_id });
    const layerId = item.type === "layer" ? item.id : undefined;
    return {
      kind: "item",
      id: item.id,
      icon: iconFor(item),
      label: item.name,
      meta,
      go: () => activate({ id: item.id, label: item.name, meta, href, ...(layerId ? { layerId } : {}) }),
    };
  };

  const rows: SearchRow[] = [];

  if (!hasQuery && scope !== "all") {
    // A prefix on its own: name the group it narrowed to and say what it wants,
    // rather than leaving the dropdown blank until the second character.
    const group = SCOPE_GROUP[scope];
    rows.push({ kind: "head", label: t(group.head) });
    rows.push({ kind: "note", label: t(group.prompt) });
  } else if (!hasQuery) {
    if (recent.length) {
      rows.push({ kind: "head", label: t("recent") });
      for (const place of recent) {
        rows.push({
          kind: "item",
          id: place.id,
          icon: ICON_NAME.CLOCK,
          label: place.label,
          meta: place.meta,
          go: () => activate(place),
        });
      }
    }
  } else {
    const items = asksContent ? (page?.items ?? []) : [];
    const projectItems = scope === "dataset" ? [] : items.filter((item) => item.type === "project");
    const datasetItems =
      scope === "project" ? [] : items.filter((item) => item.type === "layer" || item.type === "bundle");
    const catalogItems = asksCatalog ? catalogDatasets : [];

    if (projectItems.length) {
      rows.push({ kind: "head", label: t("projects") });
      rows.push(...projectItems.map(contentItemRow));
    }
    if (datasetItems.length) {
      rows.push({ kind: "head", label: t("datasets") });
      rows.push(...datasetItems.map(contentItemRow));
    }
    if (catalogItems.length) {
      rows.push({ kind: "head", label: t("catalog") });
      for (const dataset of catalogItems) {
        const label = dataset.title ?? dataset.id;
        const meta = dataset.description ?? "";
        rows.push({
          kind: "item",
          id: dataset.id,
          icon: ICON_NAME.GLOBE,
          label,
          meta,
          go: () => activate({ id: dataset.id, label, meta, href: `/catalog/${dataset.id}` }),
        });
      }
    }
  }

  return {
    rows,
    isLoading: (asksContent && contentLoading) || (asksCatalog && catalogLoading),
    previewLayerId,
    closePreview: () => setPreviewLayerId(null),
  };
};
