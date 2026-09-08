/** When a catalog row's data is from. */

import type { CatalogCollection, CatalogItem } from "@/lib/validations/catalog";

/**
 * A temporal extent, however the row happened to state it. Both bounds are
 * optional: an open-ended dataset ("everything since 2020") publishes one.
 */
export type CatalogPeriod = { start?: string | null; end?: string | null };

/** When an item's data is from. */
export const itemPeriod = (item: CatalogItem): CatalogPeriod | undefined => {
  const { datetime, start_datetime: start, end_datetime: end } = item.properties;
  if (start || end) return { start: start ?? datetime, end: end ?? datetime };
  return datetime ? { start: datetime, end: datetime } : undefined;
};

/**
 * When a dataset's data is from. The served `extent.temporal` is already the
 * envelope of its layers' dates; `members` is the fallback for rows not in hand.
 */
export const datasetPeriod = (
  collection: CatalogCollection | undefined,
  members: CatalogItem[] = []
): CatalogPeriod | undefined => {
  const interval = collection?.extent?.temporal?.interval?.[0];
  if (interval && (interval[0] || interval[1])) {
    return { start: interval[0], end: interval[1] };
  }
  const periods = members.map(itemPeriod).filter((p): p is CatalogPeriod => !!p);
  if (!periods.length) return undefined;
  const starts = periods.map((p) => p.start).filter((v): v is string => !!v).sort();
  const ends = periods.map((p) => p.end).filter((v): v is string => !!v).sort();
  return { start: starts[0], end: ends[ends.length - 1] };
};
