import { Add } from "@mui/icons-material";
import { Button, Checkbox, FormControlLabel, Stack, TextField, Typography } from "@mui/material";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { v4 as uuidv4 } from "uuid";

import { ICON_NAME } from "@p4b/ui/components/Icon";

import type { RichTextDataSchema, RichTextVariableSchema } from "@/lib/validations/widget";

import CollapsibleConfigCard from "@/components/builder/widgets/common/CollapsibleConfigCard";
import RichTextVariableForm from "@/components/builder/widgets/data/RichTextVariableForm";
import SectionHeader from "@/components/map/panels/common/SectionHeader";
import SectionOptions from "@/components/map/panels/common/SectionOptions";

interface RichTextConfigProps {
  config: RichTextDataSchema;
  onChange: (config: RichTextDataSchema) => void;
}

const defaultVariable = (existingNames: string[]): RichTextVariableSchema => {
  let index = 1;
  while (existingNames.includes(`var_${index}`)) {
    index++;
  }
  return {
    id: uuidv4(),
    name: `var_${index}`,
    format: "none",
  };
};

const RichTextConfig = ({ config, onChange }: RichTextConfigProps) => {
  const { t } = useTranslation("common");
  const [expandedId, setExpandedId] = useState<string | null>(null);

  const variables = config.setup?.variables ?? [];
  const options = config.options ?? {};

  const handleSetupChange = (key: string, value: unknown) => {
    onChange({
      ...config,
      setup: { ...config.setup, [key]: value },
    } as RichTextDataSchema);
  };

  const handleOptionChange = (key: string, value: unknown) => {
    onChange({
      ...config,
      options: { ...config.options, [key]: value },
    } as RichTextDataSchema);
  };

  const handleVariableChange = (index: number, updated: RichTextVariableSchema) => {
    const next = [...variables];
    next[index] = updated;
    handleSetupChange("variables", next);
  };

  const handleRemoveVariable = (index: number) => {
    const next = variables.filter((_, i) => i !== index);
    handleSetupChange("variables", next);
    if (variables[index]?.id === expandedId) {
      setExpandedId(null);
    }
  };

  const handleAddVariable = () => {
    const newVar = defaultVariable(variables.map((v) => v.name));
    handleSetupChange("variables", [...variables, newVar]);
    setExpandedId(newVar.id);
  };

  const buildSummary = (v: RichTextVariableSchema): string => {
    const parts: string[] = [];
    if (v.operation_type) parts.push(v.operation_type);
    if (v.operation_value) parts.push(v.operation_value);
    return parts.join(" \u00B7 ") || "";
  };

  return (
    <Stack direction="column" spacing={2} justifyContent="space-between">
      {/* Variables section */}
      <SectionHeader
        active
        alwaysActive
        label={t("variables", { defaultValue: "Variables" })}
        icon={ICON_NAME.CODE}
        disableAdvanceOptions
      />
      <SectionOptions
        active
        baseOptions={
          <Stack spacing={1.5}>
            {variables.map((variable, index) => (
              <CollapsibleConfigCard
                key={variable.id}
                title={
                  <Typography
                    variant="body2"
                    noWrap
                    sx={{ fontFamily: "monospace", color: "primary.main", fontWeight: 600 }}>
                    {variable.name || t("variable", { defaultValue: "variable" })}
                  </Typography>
                }
                summary={buildSummary(variable)}
                expanded={expandedId === variable.id}
                onToggle={() =>
                  setExpandedId(expandedId === variable.id ? null : variable.id)
                }
                onRemove={() => handleRemoveVariable(index)}>
                <RichTextVariableForm
                  variable={variable}
                  onChange={(updated) => handleVariableChange(index, updated)}
                />
              </CollapsibleConfigCard>
            ))}
            <Button
              variant="text"
              fullWidth
              startIcon={<Add />}
              onClick={handleAddVariable}>
              {t("add_variable", { defaultValue: "Add Variable" })}
            </Button>
          </Stack>
        }
      />

      {/* Options section */}
      <SectionHeader
        active
        alwaysActive
        label={t("options")}
        icon={ICON_NAME.SLIDERS}
        disableAdvanceOptions
      />
      <SectionOptions
        active
        baseOptions={
          <Stack spacing={1}>
            <FormControlLabel
              control={
                <Checkbox
                  size="small"
                  color="primary"
                  checked={!!options.filter_by_viewport}
                  onChange={(e) => handleOptionChange("filter_by_viewport", e.target.checked)}
                />
              }
              label={<Typography variant="body2">{t("filter_viewport")}</Typography>}
            />
            <FormControlLabel
              control={
                <Checkbox
                  size="small"
                  color="primary"
                  checked={!!options.hide_when_no_filter}
                  onChange={(e) => handleOptionChange("hide_when_no_filter", e.target.checked)}
                />
              }
              label={<Typography variant="body2">{t("hide_when_no_filter", { defaultValue: "Hide when no filter" })}</Typography>}
            />
            {!options.hide_when_no_filter && (
              <TextField
                size="small"
                fullWidth
                label={t("no_filter_text", { defaultValue: "Fallback text" })}
                placeholder={t("no_filter_text_placeholder", { defaultValue: "e.g. Select a city to see details" })}
                value={options.no_filter_text || ""}
                onChange={(e) => handleOptionChange("no_filter_text", e.target.value)}
                helperText={t("no_filter_text_helper", { defaultValue: "Shown when no filter is active" })}
              />
            )}
          </Stack>
        }
      />
    </Stack>
  );
};

export default RichTextConfig;
