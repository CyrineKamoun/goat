import { Button, Popover, Stack, TextField, Typography } from "@mui/material";
import React, { useCallback, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { useParams } from "next/navigation";
import { v4 } from "uuid";

import { updateProjectLayer, useProjectLayers } from "@/lib/api/projects";
import { createTheCQLBasedOnExpression, parseCQLQueryToObject } from "@/lib/transformers/filter";
import { FilterType } from "@/lib/validations/filter";
import type { ProjectLayer } from "@/lib/validations/project";

import useLayerFields from "@/hooks/map/CommonHooks";
import useLogicalExpressionOperations from "@/hooks/map/FilteringHooks";

import Selector from "@/components/map/panels/common/Selector";
import SelectorLayerValue from "@/components/map/panels/common/SelectorLayerValue";

import type { SelectorItem } from "@/types/map/common";

interface QuickFilterPopoverProps {
  anchorEl: HTMLElement | null;
  columnName: string;
  columnType: string;
  layerId: string;
  projectLayer: ProjectLayer;
  onClose: () => void;
}

const NO_VALUE_OPS = ["is_blank", "is_not_blank", "is_empty_string", "is_not_empty_string"];
const VALUE_SELECTOR_OPS = ["is", "is_not"];
const MULTI_VALUE_OPS = ["includes", "excludes"];

const QuickFilterPopover: React.FC<QuickFilterPopoverProps> = ({
  anchorEl,
  columnName,
  columnType,
  layerId,
  projectLayer,
  onClose,
}) => {
  const { t } = useTranslation("common");
  const { projectId } = useParams();
  const { layerFields } = useLayerFields(layerId);
  const { layers: projectLayers, mutate: mutateProjectLayers } = useProjectLayers(
    projectId as string
  );

  const { logicalExpressionTypes } = useLogicalExpressionOperations(columnType);

  const [selectedOp, setSelectedOp] = useState<SelectorItem | undefined>(undefined);
  const [value, setValue] = useState<string>("");
  const [selectedValues, setSelectedValues] = useState<string[]>([]);

  const opValue = selectedOp?.value as string | undefined;
  const needsNoValue = opValue ? NO_VALUE_OPS.includes(opValue) : false;
  const needsValueSelector = opValue ? VALUE_SELECTOR_OPS.includes(opValue) : false;
  const needsMultiValue = opValue ? MULTI_VALUE_OPS.includes(opValue) : false;
  const needsTextInput = opValue ? !needsNoValue && !needsValueSelector && !needsMultiValue : false;

  const isValid = useMemo(() => {
    if (!opValue) return false;
    if (needsNoValue) return true;
    if (needsValueSelector) return value !== "";
    if (needsMultiValue) return selectedValues.length > 0;
    return value !== "";
  }, [opValue, needsNoValue, needsValueSelector, needsMultiValue, value, selectedValues]);

  const handleApply = useCallback(async () => {
    if (!opValue || !projectId) return;

    // Build the new expression
    let filterValue: string | number | (string | number)[] = value;
    if (needsMultiValue) {
      filterValue = selectedValues;
    } else if (needsValueSelector) {
      filterValue = value;
    } else if (columnType === "number" && value !== "") {
      filterValue = Number(value);
    }

    const newExpression = {
      id: v4(),
      attribute: columnName,
      expression: opValue,
      value: filterValue,
      type: FilterType.Logical,
    };

    // Parse existing CQL expressions
    const existingExpressions = parseCQLQueryToObject(
      projectLayer.query?.cql as { op: string; args: unknown[] }
    );
    const allExpressions = [...existingExpressions, newExpression];

    // Build new CQL — use existing logical operator or default to "and"
    const existingOp = projectLayer.query?.cql?.op;
    const logicalOp = existingOp === "or" ? "or" : "and";
    const query = createTheCQLBasedOnExpression(allExpressions, layerFields, logicalOp);

    // Close popover first to avoid anchorEl warning
    onClose();

    // Save to layer
    if (projectLayers) {
      const layers = JSON.parse(JSON.stringify(projectLayers));
      const index = layers.findIndex((l: ProjectLayer) => l.id === projectLayer.id);
      if (index >= 0) {
        layers[index].query = { cql: query };
        await mutateProjectLayers(layers, false);
        await updateProjectLayer(projectId as string, projectLayer.id, layers[index]);
      }
    }
  }, [
    opValue,
    value,
    selectedValues,
    needsMultiValue,
    needsValueSelector,
    columnName,
    columnType,
    projectLayer,
    projectLayers,
    projectId,
    layerFields,
    mutateProjectLayers,
    onClose,
  ]);

  return (
    <Popover
      open={!!anchorEl}
      anchorEl={anchorEl}
      onClose={onClose}
      anchorOrigin={{ vertical: "bottom", horizontal: "left" }}
      transformOrigin={{ vertical: "top", horizontal: "left" }}
      slotProps={{
        paper: {
          sx: { width: 260, p: 2, mt: 0.5 },
        },
      }}>
      <Stack spacing={1.5}>
        {/* Column name */}
        <Typography variant="body2" fontWeight="bold">
          {columnName}
        </Typography>

        {/* Operator selector */}
        <Selector
          selectedItems={selectedOp}
          setSelectedItems={(item) => {
            setSelectedOp(item as SelectorItem);
            setValue("");
            setSelectedValues([]);
          }}
          items={logicalExpressionTypes}
          placeholder={t("filter_expressions.select_operator", { defaultValue: "Select operator" })}
        />

        {/* Value input — conditional on operator */}
        {needsValueSelector && (
          <SelectorLayerValue
            selectedValues={value || ""}
            onSelectedValuesChange={(v) => setValue((v as string) ?? "")}
            layerId={layerId}
            fieldName={columnName}
            placeholder={t("filter_expressions.select_value", { defaultValue: "Select value" })}
          />
        )}
        {needsMultiValue && (
          <SelectorLayerValue
            selectedValues={selectedValues}
            onSelectedValuesChange={(v) => setSelectedValues((v as string[]) ?? [])}
            layerId={layerId}
            fieldName={columnName}
            multiple
            placeholder={t("filter_expressions.select_values", { defaultValue: "Select values" })}
          />
        )}
        {needsTextInput && (
          <TextField
            autoFocus
            size="small"
            fullWidth
            type={columnType === "number" ? "number" : "text"}
            value={value}
            onChange={(e) => setValue(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter" && isValid) handleApply();
            }}
            placeholder={
              columnType === "number"
                ? t("filter_expressions.enter_number", { defaultValue: "Enter a number" })
                : t("filter_expressions.enter_value", { defaultValue: "Enter a value" })
            }
            sx={{ "& .MuiInputBase-root": { fontSize: "0.85rem" } }}
          />
        )}

        {/* Actions */}
        <Stack direction="row" justifyContent="flex-end" spacing={2} sx={{ pt: 0.5 }}>
          <Button variant="text" onClick={onClose}>
            <Typography variant="body2" fontWeight="bold">
              {t("cancel", { defaultValue: "Cancel" })}
            </Typography>
          </Button>
          <Button
            variant="contained"
            color="primary"
            disabled={!isValid}
            onClick={handleApply}>
            {t("done", { defaultValue: "Done" })}
          </Button>
        </Stack>
      </Stack>
    </Popover>
  );
};

export default QuickFilterPopover;
