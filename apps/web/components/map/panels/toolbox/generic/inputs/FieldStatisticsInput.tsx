/**
 * Field Statistics Input Component
 *
 * Renders a field statistics selector that combines:
 * 1. An operation dropdown (count, sum, min, max, mean, standard_deviation)
 * 2. A field selector (when operation is not 'count')
 * 3. An optional result column name input
 *
 * The related layer is determined by widget_options.source_layer.
 */
import { Box, Stack, TextField, Typography } from "@mui/material";
import { useParams } from "next/navigation";
import { useMemo } from "react";
import { useTranslation } from "react-i18next";

import type { LayerFieldType } from "@/lib/validations/layer";

import type { SelectorItem } from "@/types/map/common";
import type { ProcessedInput } from "@/types/map/ogc-processes";

import useLayerFields from "@/hooks/map/CommonHooks";
import { useFilteredProjectLayers } from "@/hooks/map/LayerPanelHooks";

import FormLabelHelper from "@/components/common/FormLabelHelper";
import LayerFieldSelector from "@/components/map/common/LayerFieldSelector";
import Selector from "@/components/map/panels/common/Selector";

// Define the statistic operations supported by the backend
const STATISTIC_OPERATIONS = [
  { value: "count", labelKey: "count" },
  { value: "sum", labelKey: "sum" },
  { value: "min", labelKey: "min" },
  { value: "max", labelKey: "max" },
  { value: "mean", labelKey: "mean" },
  { value: "standard_deviation", labelKey: "standard_deviation" },
] as const;

interface FieldStatisticsValue {
  operation: string;
  field?: string | null;
  result_name?: string | null;
}

// Stable empty object references to avoid creating new references on each render
const EMPTY_LAYER_DATASET_IDS: Record<string, string> = {};
const EMPTY_PREDICTED_COLUMNS: Record<string, Record<string, string>> = {};

interface FieldStatisticsInputProps {
  input: ProcessedInput;
  value: unknown;
  onChange: (value: unknown) => void;
  disabled?: boolean;
  /** All current form values - needed to get the related layer's value */
  formValues: Record<string, unknown>;
  /** Map of layer input names to their dataset IDs (for connected layers in workflows) */
  layerDatasetIds?: Record<string, string>;
  /** Map of layer input names to their predicted columns (for connected tool outputs) */
  predictedColumns?: Record<string, Record<string, string>>;
}

export default function FieldStatisticsInput({
  input,
  value,
  onChange,
  disabled,
  formValues,
  layerDatasetIds,
  predictedColumns,
}: FieldStatisticsInputProps) {
  const { t } = useTranslation("common");
  const { projectId } = useParams();

  // Ensure we have safe objects to access (handles explicit undefined) - use stable references
  const safeLayerDatasetIds =
    layerDatasetIds && Object.keys(layerDatasetIds).length > 0 ? layerDatasetIds : EMPTY_LAYER_DATASET_IDS;
  const safePredictedColumns =
    predictedColumns && Object.keys(predictedColumns).length > 0 ? predictedColumns : EMPTY_PREDICTED_COLUMNS;

  // Opt-in via widget_options.multi (aggregate sets it; join doesn't).
  const isMultiField = useMemo(() => {
    return input.uiMeta?.widget_options?.multi === true;
  }, [input.uiMeta]);

  const currentValue = useMemo((): FieldStatisticsValue => {
    if (Array.isArray(value) && value.length > 0) {
      const v = value[0] as FieldStatisticsValue;
      return {
        operation: v.operation || "",
        field: v.field ?? null,
        result_name: v.result_name ?? null,
      };
    }
    // Handle single object format (legacy)
    if (typeof value === "object" && value !== null && !Array.isArray(value)) {
      const v = value as FieldStatisticsValue;
      return {
        operation: v.operation || "",
        field: v.field ?? null,
        result_name: v.result_name ?? null,
      };
    }
    return { operation: "", field: null, result_name: null };
  }, [value]);

  const currentFieldNames = useMemo((): string[] => {
    if (!isMultiField || !Array.isArray(value)) return [];
    return value
      .map((entry) =>
        typeof entry === "object" && entry !== null
          ? (entry as FieldStatisticsValue).field
          : null
      )
      .filter((f): f is string => typeof f === "string");
  }, [isMultiField, value]);

  const emitChange = (newValue: FieldStatisticsValue) => {
    if (newValue.operation) {
      const cleanedValue = {
        ...newValue,
        result_name: newValue.result_name?.trim() || null,
      };
      onChange([cleanedValue]);
    } else {
      onChange(null);
    }
  };

  // Expands (operation, [f1, f2, ...]) into N FieldStatistic entries sharing
  // the same operation. When fields are still empty we emit a placeholder so
  // the operation persists and the field selector can render.
  const emitMultiChange = (operation: string, fieldNames: string[]) => {
    if (!operation) {
      onChange(null);
      return;
    }
    if (operation === "count") {
      onChange([{ operation, field: null, result_name: null }]);
      return;
    }
    if (fieldNames.length === 0) {
      onChange([{ operation, field: null, result_name: null }]);
      return;
    }
    onChange(
      fieldNames.map((field) => ({ operation, field, result_name: null }))
    );
  };

  // Determine which layer this field relates to from widget_options.source_layer
  const relatedLayerInputName = useMemo(() => {
    const sourceLayer = input.uiMeta?.widget_options?.source_layer;
    return typeof sourceLayer === "string" ? sourceLayer : null;
  }, [input.uiMeta]);

  // Get the selected layer ID from form values
  const selectedLayerId = useMemo(() => {
    if (!relatedLayerInputName) return null;
    const layerId = formValues[relatedLayerInputName];
    return typeof layerId === "string" ? layerId : null;
  }, [relatedLayerInputName, formValues]);

  // Get project layers to find the dataset ID
  const { layers: projectLayers } = useFilteredProjectLayers(projectId as string);

  // Find the dataset ID for the selected layer
  const datasetId = useMemo(() => {
    // First check if parent provided it (for connected layers in workflows)
    if (relatedLayerInputName && safeLayerDatasetIds[relatedLayerInputName]) {
      return safeLayerDatasetIds[relatedLayerInputName];
    }

    // Otherwise try to find it from project layers
    if (!selectedLayerId || !projectLayers) return "";

    const layer = projectLayers.find(
      (l) => l.id === Number(selectedLayerId) || l.layer_id === selectedLayerId
    );
    return layer?.layer_id || "";
  }, [selectedLayerId, projectLayers, relatedLayerInputName, safeLayerDatasetIds]);

  // Check if we have predicted columns for this layer input (for connected tool outputs)
  const hasPredictedColumns = useMemo(() => {
    return relatedLayerInputName && safePredictedColumns[relatedLayerInputName] != null;
  }, [relatedLayerInputName, safePredictedColumns]);

  // Convert predicted columns to LayerFieldType format (numeric only for statistics)
  const predictedNumericFields = useMemo((): LayerFieldType[] => {
    if (!relatedLayerInputName || !safePredictedColumns[relatedLayerInputName]) {
      return [];
    }
    const columns = safePredictedColumns[relatedLayerInputName];
    return Object.entries(columns)
      .filter(([name, type]) => {
        if (["geometry", "geom", "id", "layer_id"].includes(name.toLowerCase())) return false;
        const upperType = type.toUpperCase();
        return (
          upperType.includes("INT") ||
          upperType.includes("FLOAT") ||
          upperType.includes("DOUBLE") ||
          upperType.includes("DECIMAL") ||
          upperType.includes("NUMERIC")
        );
      })
      .map(([name]) => ({
        name,
        type: "number" as const,
      }));
  }, [relatedLayerInputName, safePredictedColumns]);

  // Fetch numeric fields for the layer (skip when predicted columns are available)
  const { layerFields, isLoading } = useLayerFields(hasPredictedColumns ? "" : datasetId, "number");

  // Use predicted numeric fields if available, otherwise use layer fields
  const numericFields = useMemo((): LayerFieldType[] => {
    if (hasPredictedColumns && predictedNumericFields.length > 0) {
      return predictedNumericFields;
    }
    return layerFields as LayerFieldType[];
  }, [hasPredictedColumns, predictedNumericFields, layerFields]);

  // Check if the current operation requires a field
  const requiresField = currentValue.operation && currentValue.operation !== "count";

  // Convert the selected field name to LayerFieldType format
  const selectedField = useMemo((): LayerFieldType | undefined => {
    if (!currentValue.field || !requiresField) return undefined;
    return numericFields.find((f) => f.name === currentValue.field);
  }, [currentValue.field, numericFields, requiresField]);

  const selectedFields = useMemo((): LayerFieldType[] => {
    if (!isMultiField || !requiresField) return [];
    return currentFieldNames
      .map((name) => numericFields.find((f) => f.name === name))
      .filter((f): f is LayerFieldType => f !== undefined);
  }, [isMultiField, requiresField, currentFieldNames, numericFields]);

  // Convert operations to SelectorItems for the Selector component
  const operationItems: SelectorItem[] = useMemo(() => {
    return STATISTIC_OPERATIONS.map((op) => ({
      value: op.value,
      label: t(op.labelKey),
    }));
  }, [t]);

  // Find selected operation item
  const selectedOperationItem = useMemo(() => {
    if (!currentValue.operation) return undefined;
    return operationItems.find((item) => item.value === currentValue.operation);
  }, [currentValue.operation, operationItems]);

  const handleOperationChange = (item: SelectorItem | SelectorItem[] | undefined) => {
    if (Array.isArray(item)) return;
    const operation = (item?.value as string) || "";
    if (isMultiField) {
      emitMultiChange(operation, currentFieldNames);
      return;
    }
    if (operation === "count") {
      emitChange({ operation, field: null, result_name: currentValue.result_name });
    } else {
      emitChange({ operation, field: currentValue.field || null, result_name: currentValue.result_name });
    }
  };

  const handleFieldChange = (field: LayerFieldType | undefined) => {
    emitChange({
      operation: currentValue.operation,
      field: field?.name ?? null,
      result_name: currentValue.result_name,
    });
  };

  const handleMultiFieldsChange = (fields: LayerFieldType[] | undefined) => {
    emitMultiChange(currentValue.operation, (fields ?? []).map((f) => f.name));
  };

  const handleResultNameChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    emitChange({
      operation: currentValue.operation,
      field: currentValue.field,
      result_name: event.target.value || null,
    });
  };

  // Generate placeholder for result name based on current selection
  const resultNamePlaceholder = useMemo(() => {
    if (!currentValue.operation) return "";
    if (currentValue.operation === "count") return "count";
    if (currentValue.field) return `${currentValue.field}_${currentValue.operation}`;
    return "";
  }, [currentValue.operation, currentValue.field]);

  // Get label from uiMeta or fallback to title
  const label = input.uiMeta?.label || input.title || input.name;

  // Show message if no layer is selected
  if (!selectedLayerId) {
    return (
      <Box>
        <Typography variant="body2" color="text.secondary" sx={{ fontStyle: "italic" }}>
          {label}: {t("select_layer_first")}
        </Typography>
      </Box>
    );
  }

  return (
    <Stack spacing={2}>
      {/* Operation Selector - uses Selector component like EnumInput */}
      <Selector
        selectedItems={selectedOperationItem}
        setSelectedItems={handleOperationChange}
        items={operationItems}
        label={t("select_operation")}
        placeholder={t("select_option")}
        disabled={disabled}
      />

      {requiresField && (
        isMultiField ? (
          <LayerFieldSelector
            // LayerFieldSelector's generic isn't exposed; cast as in FieldInput.
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            selectedField={selectedFields as any}
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            setSelectedField={handleMultiFieldsChange as any}
            fields={numericFields}
            label={t("select_fields")}
            tooltip={t("select_numeric_field_for_statistics")}
            disabled={disabled || isLoading}
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            multiple={true as any}
          />
        ) : (
          <LayerFieldSelector
            selectedField={selectedField}
            setSelectedField={handleFieldChange}
            fields={numericFields}
            label={t("select_field")}
            tooltip={t("select_numeric_field_for_statistics")}
            disabled={disabled || isLoading}
          />
        )
      )}

      {!isMultiField && currentValue.operation && (
        <Stack>
          <FormLabelHelper label={t("result_column_name")} color="inherit" tooltip={t("result_column_name_helper")} />
          <TextField
            size="small"
            fullWidth
            placeholder={resultNamePlaceholder}
            value={currentValue.result_name || ""}
            onChange={handleResultNameChange}
            disabled={disabled}
          />
        </Stack>
      )}
    </Stack>
  );
}
