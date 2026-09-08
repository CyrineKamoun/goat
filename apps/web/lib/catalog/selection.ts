/**
 * What a card's checkbox shows, given what is selected.
 *
 * A dataset stands for one layer and is simply in or out. A bundle stands for
 * several, and its own box summarises them: checked when every layer is in, partial
 * when some are. Selection is always recorded per layer, never per bundle — that is
 * what makes a count of "3 layers" true, and it matches promotion, where members
 * promote individually into a shared bundle group.
 */
export type CardSelectionState = {
  selected: boolean;
  indeterminate: boolean;
  /** What the card's own checkbox should do next. */
  nextSelected: boolean;
};

export const cardSelectionState = (
  /** The layer ids this card stands for: one for a dataset, many for a bundle. */
  memberIds: string[],
  selectedIds: string[]
): CardSelectionState => {
  if (memberIds.length === 0) {
    return { selected: false, indeterminate: false, nextSelected: true };
  }
  const selectedSet = new Set(selectedIds);
  const picked = memberIds.filter((id) => selectedSet.has(id)).length;
  const selected = picked === memberIds.length;
  return {
    selected,
    indeterminate: picked > 0 && !selected,
    // Anything short of "all" means the click selects the rest; only a full card
    // clears itself. Partial → select all is what makes one click finish the job.
    nextSelected: !selected,
  };
};
