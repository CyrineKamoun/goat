import { Button, Popover, Stack, TextField, Typography } from "@mui/material";
import React, { useCallback, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { useParams } from "next/navigation";
import { v4 } from "uuid";

import TemporalPicker from "@p4b/ui/components/TemporalPicker";

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

const NO_VALUE_OPS = [
  "is_blank",
  "is_not_blank",
  "is_empty_string",
  "is_not_empty_string",
  "is_true",
  "is_false",
];
const VALUE_SELECTOR_OPS = ["is", "is_not"];
const MULTI_VALUE_OPS = ["includes", "excludes"];
const DATE_SINGLE_OPS = ["is_on", "is_not_on", "is_before", "is_after"];
const DATE_RANGE_OPS = ["is_between", "is_not_between"];
const DAYS_OPS = ["in_the_last", "not_in_the_last"];

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
  const needsDateInput = columnType === "date" && !!opValue && DATE_SINGLE_OPS.includes(opValue);
  // Date range values live in selectedValues ([from, to])
  const needsDateRange = columnType === "date" && !!opValue && DATE_RANGE_OPS.includes(opValue);
  const needsDaysInput = columnType === "date" && !!opValue && DAYS_OPS.includes(opValue);
  const needsTextInput = opValue
    ? !needsNoValue && !needsValueSelector && !needsMultiValue && !needsDateInput && !needsDateRange
    : false;

  const isValid = useMemo(() => {
    if (!opValue) return false;
    if (needsNoValue) return true;
    if (needsValueSelector) return value !== "";
    if (needsDateRange) return !!selectedValues[0] && !!selectedValues[1];
    if (needsMultiValue) return selectedValues.length > 0;
    return value !== "";
  }, [opValue, needsNoValue, needsValueSelector, needsDateRange, needsMultiValue, value, selectedValues]);

  const handleApply = useCallback(async () => {
    if (!opValue || !projectId) return;

    // Build the new expression
    let filterValue: string | number | (string | number)[] = value;
    if (needsMultiValue || needsDateRange) {
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
    needsDateRange,
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
        {needsDateInput && (
          <TemporalPicker kind="datetime" value={value} onChange={(v) => setValue(v)} />
        )}
        {needsDateRange && (
          <Stack direction="column" spacing={1}>
            {[0, 1].map((index) => (
              <TemporalPicker
                key={index}
                kind="datetime"
                label={index === 0 ? t("from") : t("to")}
                value={selectedValues[index] ?? ""}
                onChange={(v) => {
                  setSelectedValues((prev) => {
                    const next = [prev[0] ?? "", prev[1] ?? ""];
                    next[index] = v;
                    return next;
                  });
                }}
              />
            ))}
          </Stack>
        )}
        {needsTextInput && (
          <TextField
            autoFocus
            size="small"
            fullWidth
            type={columnType === "number" || needsDaysInput ? "number" : "text"}
            value={value}
            onChange={(e) => setValue(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter" && isValid) handleApply();
            }}
            placeholder={
              needsDaysInput
                ? t("number_of_days", { defaultValue: "Number of days" })
                : columnType === "number"
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
