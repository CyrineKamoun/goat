"use client";

import { ToggleButton, ToggleButtonGroup } from "@mui/material";
import { useTranslation } from "react-i18next";

import { templateSourceOptions } from "@/lib/utils/templates";
import type { Space } from "@/lib/validations/content";
import type { TemplateSourceFilter } from "@/lib/validations/template";

interface TemplateSourceSegmentsProps {
  source: TemplateSourceFilter;
  onChange: (source: TemplateSourceFilter) => void;
  /** Which of Team/Organization to show is read off the caller's spaces
   * (T7) — Everyone, GOAT and Mine are always available. */
  spaces: Space[];
}

/** T7's source segments: Everyone / GOAT / Mine always, Team only once the
 * caller is in a team space, Organization only once an organization space
 * exists — a personal-only account never sees a segment it has nothing
 * behind. */
const TemplateSourceSegments = ({ source, onChange, spaces }: TemplateSourceSegmentsProps) => {
  const { t } = useTranslation("common");
  const options = templateSourceOptions(spaces);

  return (
    <ToggleButtonGroup
      value={source}
      exclusive
      size="small"
      // The selected segment resends its own value on click; treated as a
      // no-op rather than clearing the filter to nothing.
      onChange={(_, value: TemplateSourceFilter | null) => value && onChange(value)}
      sx={{ flexWrap: "wrap" }}>
      {options.map((option) => (
        <ToggleButton
          key={option.value}
          value={option.value}
          sx={{ textTransform: "none", px: 3, fontSize: 13 }}>
          {t(option.labelKey)}
        </ToggleButton>
      ))}
    </ToggleButtonGroup>
  );
};

export default TemplateSourceSegments;
