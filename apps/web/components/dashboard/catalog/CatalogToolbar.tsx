"use client";

import { Stack } from "@mui/material";
import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";

import { ICON_NAME } from "@p4b/ui/components/Icon";

import LayoutToggle from "@/components/dashboard/common/LayoutToggle";
import SearchInput from "@/components/dashboard/common/SearchInput";
import type { SortOption } from "@/components/dashboard/common/SortMenu";
import SortMenu from "@/components/dashboard/common/SortMenu";
import ToolPill from "@/components/dashboard/common/ToolPill";

/** The catalog's toolbar: a search field, a Grid/List toggle, and a sort menu —
 * the same controls the Content feed's tool row carries. */

export type CatalogSortOption = SortOption;

const CatalogToolbar = ({
  q,
  onChangeQ,
  view,
  onChangeView,
  sort,
  sortOptions,
  onChangeSort,
  onOpenFilters,
  activeFilterCount = 0,
  compact,
}: {
  q: string;
  /** Called on every keystroke; the page decides how to debounce. */
  onChangeQ: (value: string) => void;
  view: "list" | "grid";
  onChangeView: (view: "list" | "grid") => void;
  sort: string;
  sortOptions: CatalogSortOption[];
  onChangeSort: (value: string) => void;
  /**
   * Opens the filter drawer. Passed only where the sidebar is not on screen —
   * below `md` — so the control appears exactly when it is the only way in.
   */
  onOpenFilters?: () => void;
  activeFilterCount?: number;
  /** Phone layout: search collapses to an icon that expands over the row, sort loses its label, and the Grid/List toggle is dropped — one column is the only layout a phone has, so the two views render identically there. */
  compact?: boolean;
}) => {
  const { t } = useTranslation("common");

  // The typed text, so the field answers every keystroke while the query it
  // stands for is debounced by the page; the URL is the source of truth and
  // pushes back in whenever it changes from outside.
  const [text, setText] = useState(q);
  useEffect(() => setText(q), [q]);

  return (
    <Stack direction="row" alignItems="center" useFlexGap flexWrap="wrap" gap={2.5}>
      <SearchInput
        value={text}
        onChange={(next) => {
          setText(next);
          onChangeQ(next);
        }}
        onClear={() => {
          setText("");
          onChangeQ("");
        }}
        placeholder={t("search_datasets")}
        collapsible={compact}
      />

      {onOpenFilters && (
        <ToolPill
          icon={ICON_NAME.FILTER}
          label={t("filter")}
          active={activeFilterCount > 0}
          badge={activeFilterCount > 0 ? activeFilterCount : undefined}
          onClick={onOpenFilters}
        />
      )}

      {!compact && <LayoutToggle value={view} onChange={onChangeView} />}

      <SortMenu
        value={sort}
        options={sortOptions}
        onChange={onChangeSort}
        compact={compact}
        label={t("sort")}
      />
    </Stack>
  );
};

export default CatalogToolbar;
