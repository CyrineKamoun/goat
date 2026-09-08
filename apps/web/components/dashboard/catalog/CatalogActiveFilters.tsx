"use client";

import { Chip, Stack } from "@mui/material";

/**
 * The facet values currently narrowing a result list, each removable.
 *
 * Shared by the catalog page and the Add Layer picker: both hold their selections in
 * the same shape — a list of values per facet parameter — and both label them through
 * `useCatalogFacetSections`, so the only thing that differed was where the row sits.
 *
 * Facets only. The spatial filter, the period and favourites each have their own
 * control in the panel that shows its own state, so a chip would be a second place to
 * read the same thing.
 */
const CatalogActiveFilters = ({
  selections,
  facetLabel,
  optionLabel,
  onRemove,
}: {
  /** Selected values per facet parameter — `themes`, `license`, … */
  selections: Record<string, string[]>;
  facetLabel: (param: string) => string;
  optionLabel: (param: string, value: string) => string;
  onRemove: (param: string, value: string) => void;
}) => {
  const chips = Object.entries(selections).flatMap(([param, values]) =>
    values.map((value) => ({ param, value }))
  );
  if (chips.length === 0) return null;

  return (
    <Stack direction="row" useFlexGap flexWrap="wrap" spacing={2}>
      {chips.map(({ param, value }) => (
        <Chip
          key={`${param}-${value}`}
          size="small"
          label={`${facetLabel(param)}: ${optionLabel(param, value)}`}
          onDelete={() => onRemove(param, value)}
        />
      ))}
    </Stack>
  );
};

export default CatalogActiveFilters;
