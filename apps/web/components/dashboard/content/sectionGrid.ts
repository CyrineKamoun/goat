/** The tracks every feed section lays its tiles on: cards and folder tiles
 * fill the row rather than sitting in a fixed column count, so the feed
 * reflows with the panel. Shared by the Content feed, its loading skeleton
 * and the dataset picker, so a dataset is met as the same tile wherever it
 * is listed and a track change is one edit. */
export const SECTION_GRID = {
  cards: { desktop: 232, mobile: 168, gap: { desktop: "16px", mobile: "10px" } },
  folders: { desktop: 228, mobile: 250, gap: { desktop: "14px", mobile: "8px" } },
} as const;

export type SectionGridKind = keyof typeof SECTION_GRID;

/** The `sx` of one such grid at the caller's breakpoint. */
export const sectionGridSx = (kind: SectionGridKind, mobile: boolean) => {
  const { desktop, mobile: mobileMin, gap } = SECTION_GRID[kind];
  return {
    display: "grid",
    gridTemplateColumns: mobile
      ? `repeat(auto-fill, minmax(min(100%, ${mobileMin}px), 1fr))`
      : `repeat(auto-fill, minmax(${desktop}px, 1fr))`,
    gap: mobile ? gap.mobile : gap.desktop,
  };
};
