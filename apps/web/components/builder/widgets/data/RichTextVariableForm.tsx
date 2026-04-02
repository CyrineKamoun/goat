import { Stack, TextField } from "@mui/material";
import { useParams } from "next/navigation";
import { useMemo } from "react";
import { useTranslation } from "react-i18next";

import { type FormatNumberTypes, type RichTextVariableSchema } from "@/lib/validations/widget";

import type { SelectorItem } from "@/types/map/common";

import { useLayerByGeomType } from "@/hooks/map/ToolsHooks";

import { NumberFormatSelector } from "@/components/builder/widgets/common/WidgetCommonConfigs";
import { StatisticSelector } from "@/components/map/common/StatisticSelector";
import Selector from "@/components/map/panels/common/Selector";

interface RichTextVariableFormProps {
  variable: RichTextVariableSchema;
  onChange: (variable: RichTextVariableSchema) => void;
}

const RichTextVariableForm = ({ variable, onChange }: RichTextVariableFormProps) => {
  const { t } = useTranslation("common");
  const { projectId } = useParams();
  const { filteredLayers } = useLayerByGeomType(["feature", "table"], undefined, projectId as string);

  const selectedLayer = useMemo(() => {
    return filteredLayers.find((layer) => layer.value === variable.layer_project_id);
  }, [filteredLayers, variable.layer_project_id]);

  return (
    <Stack spacing={2}>
      {/* Variable name */}
      <TextField
        size="small"
        fullWidth
        required
        label={t("name")}
        value={variable.name}
        error={!variable.name}
        helperText={!variable.name ? t("field_required") : undefined}
        onChange={(e) => {
          const sanitized = e.target.value.replace(/[^a-zA-Z0-9_]/g, "");
          onChange({ ...variable, name: sanitized });
        }}
        inputProps={{ style: { fontFamily: "monospace" } }}
      />

      {/* Layer picker */}
      <Selector
        selectedItems={selectedLayer}
        setSelectedItems={(item: SelectorItem | undefined) => {
          onChange({
            ...variable,
            layer_project_id: item?.value as number | undefined,
            operation_type: undefined,
            operation_value: undefined,
          });
        }}
        items={filteredLayers}
        label={t("select_layer")}
        placeholder={t("select_layer")}
      />

      {/* Operation selector */}
      {variable.layer_project_id && (
        <StatisticSelector
          layerProjectId={variable.layer_project_id}
          hasGroupBy={false}
          value={{
            method: variable.operation_type,
            value: variable.operation_value,
          }}
          onChange={(nextValue) =>
            onChange({
              ...variable,
              operation_type: nextValue.method,
              operation_value: nextValue.value,
            })
          }
        />
      )}

      {/* Format picker */}
      <NumberFormatSelector
        numberFormat={variable.format as FormatNumberTypes}
        onNumberFormatChange={(format) => onChange({ ...variable, format })}
      />
    </Stack>
  );
};

export default RichTextVariableForm;
