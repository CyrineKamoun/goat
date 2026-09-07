"use client";

import CancelIcon from "@mui/icons-material/Cancel";
import { Chip, Stack } from "@mui/material";
import type { ReactNode } from "react";
import { useTranslation } from "react-i18next";

interface TemplateFilterChipsProps {
  /** `label` takes a node rather than a string so the categories chip can
   * name its tags in their own colours. */
  items: { key: string; label: ReactNode; onRemove: () => void }[];
}

/** The filters narrowing the browser's list, each removable. One chip per
 * facet rather than per value: the categories chip names up to two tags and
 * counts the rest, and removing it clears the facet — a single tag is
 * unticked in the filter popover, where it was ticked. Each chip's delete
 * icon is named, so removing a filter is reachable without reading the
 * colours. */
const TemplateFilterChips = ({ items }: TemplateFilterChipsProps) => {
  const { t } = useTranslation("common");

  if (items.length === 0) return null;

  return (
    <Stack direction="row" useFlexGap flexWrap="wrap" spacing={2}>
      {items.map((item) => (
        <Chip
          key={item.key}
          data-testid={`template-filter-chip-${item.key}`}
          size="small"
          label={item.label}
          onDelete={item.onRemove}
          // The chip's own text names the facet; the icon that drops it has
          // to say so itself, since a coloured tag list reads as nothing to
          // a screen reader.
          deleteIcon={<CancelIcon titleAccess={t("clear_filter")} />}
        />
      ))}
    </Stack>
  );
};

export default TemplateFilterChips;
