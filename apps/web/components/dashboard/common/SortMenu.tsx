"use client";

import { ICON_NAME } from "@p4b/ui/components/Icon";

import PillMenu from "@/components/dashboard/common/PillMenu";
import type { PillMenuOption } from "@/components/dashboard/common/PillMenu";

export type SortOption = PillMenuOption;

interface SortMenuProps {
  value: string;
  options: SortOption[];
  onChange: (value: string) => void;
  /** Phone layout: the pill keeps only its glyph, dropping the current
   * option's label and the chevron. */
  compact?: boolean;
  /** The pill's accessible name — "Sort" in both pages' toolbars. */
  label: string;
}

/** The sort control both pages carry: a `PillMenu` under the sort glyph. */
const SortMenu = (props: SortMenuProps) => <PillMenu icon={ICON_NAME.SORT} {...props} />;

export default SortMenu;
