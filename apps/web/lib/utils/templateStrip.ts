import type { TemplateKind, TemplateRead } from "@/lib/validations/template";

/** The kinds the Home strip interleaves under All, in the order of the kind
 * pills: one workflow, one project, one layout, then round again. */
export const TEMPLATE_STRIP_KIND_ORDER: TemplateKind[] = ["workflow", "project", "layout"];

type Starred = Record<string, boolean>;

/** GOAT-published templates ahead of everything else, each group keeping the
 * feed's order. The strip is titled "Start from a template": the curated set
 * is the starter set, and a caller's own saves sit one pill away. */
const curatedFirst = (items: TemplateRead[]): TemplateRead[] => [
  ...items.filter((item) => item.catalog_status === "published"),
  ...items.filter((item) => item.catalog_status !== "published"),
];

/** One kind's (or one filter's) cards: pinned first, then curated, then the
 * rest in feed order — capped at `limit`. */
export const rankTemplateStrip = (items: TemplateRead[], starred: Starred, limit: number): TemplateRead[] => {
  const pinned = items.filter((item) => starred[item.id]);
  const rest = curatedFirst(items.filter((item) => !starred[item.id]));
  return [...pinned, ...rest].slice(0, limit);
};

/**
 * The All strip: pinned templates of any kind first, then the kinds
 * interleaved round robin in `TEMPLATE_STRIP_KIND_ORDER`, curated before
 * own within each kind. A kind that runs out is skipped, so the others
 * backfill the remaining slots; a template answering two kind feeds is
 * shown once. Capped at `limit`.
 */
export const interleaveTemplateStrip = (
  byKind: Partial<Record<TemplateKind, TemplateRead[]>>,
  starred: Starred,
  limit: number
): TemplateRead[] => {
  const seen = new Set<string>();
  const pinned: TemplateRead[] = [];
  const queues = TEMPLATE_STRIP_KIND_ORDER.map((kind) => {
    const fresh = (byKind[kind] ?? []).filter((item) => {
      if (seen.has(item.id)) return false;
      seen.add(item.id);
      return true;
    });
    pinned.push(...fresh.filter((item) => starred[item.id]));
    return curatedFirst(fresh.filter((item) => !starred[item.id]));
  });

  const out = pinned.slice(0, limit);
  while (out.length < limit && queues.some((queue) => queue.length > 0)) {
    for (const queue of queues) {
      if (out.length >= limit) break;
      const next = queue.shift();
      if (next) out.push(next);
    }
  }
  return out;
};
