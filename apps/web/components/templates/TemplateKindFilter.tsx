"use client";

import { Box, alpha, useTheme } from "@mui/material";
import { useTranslation } from "react-i18next";

import type { TemplateKind } from "@/lib/validations/template";

interface TemplateKindFilterProps {
  kind: TemplateKind | "all";
  onChange: (kind: TemplateKind | "all") => void;
}

const KIND_OPTIONS: TemplateKind[] = ["workflow", "dashboard", "layout"];

/** T7/§4's kind pills: All plus the three template kinds. They are filters,
 * not statistics — every kind stays selectable, and the count of what the
 * current filter found is stated once, in the "Showing n of N" footer. */
const TemplateKindFilter = ({ kind, onChange }: TemplateKindFilterProps) => {
  const { t } = useTranslation("common");
  const theme = useTheme();

  const options: { value: TemplateKind | "all"; label: string }[] = [
    { value: "all", label: t("all_templates") },
    ...KIND_OPTIONS.map((value) => ({ value, label: t(`template_kind_${value}`) })),
  ];

  return (
    <Box sx={{ display: "flex", flexWrap: "wrap", gap: "8px" }}>
      {options.map((option) => {
        const selected = kind === option.value;
        return (
          <Box
            key={option.value}
            component="button"
            type="button"
            onClick={() => onChange(option.value)}
            sx={{
              display: "inline-flex",
              alignItems: "center",
              gap: "6px",
              px: 3,
              py: "6px",
              borderRadius: "999px",
              border: `1px solid ${selected ? theme.palette.primary.main : theme.palette.divider}`,
              backgroundColor: selected ? alpha(theme.palette.primary.main, 0.1) : "transparent",
              color: selected ? theme.palette.primary.main : theme.palette.text.primary,
              font: "inherit",
              fontSize: 13,
              fontWeight: 600,
              cursor: "pointer",
            }}>
            {option.label}
          </Box>
        );
      })}
    </Box>
  );
};

export default TemplateKindFilter;
