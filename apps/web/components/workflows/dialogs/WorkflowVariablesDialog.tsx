"use client";

import { Add as AddIcon, DeleteOutline as DeleteIcon } from "@mui/icons-material";
import {
  Box,
  Button,
  Divider,
  IconButton,
  MenuItem,
  Select,
  Stack,
  TextField,
  Tooltip,
  Typography,
} from "@mui/material";
import React, { useCallback, useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { useDispatch, useSelector } from "react-redux";
import { v4 as uuidv4 } from "uuid";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import type { AppDispatch } from "@/lib/store";
import { selectVariables } from "@/lib/store/workflow/selectors";
import { setVariables } from "@/lib/store/workflow/slice";
import type { WorkflowVariable } from "@/lib/validations/workflow";

import AppDialog, { AppDialogFooter } from "@/components/common/AppDialog";

const VARIABLE_NAME_REGEX = /^[a-zA-Z_][a-zA-Z0-9_]*$/;

interface WorkflowVariablesDialogProps {
  open: boolean;
  onClose: () => void;
}

const WorkflowVariablesDialog: React.FC<WorkflowVariablesDialogProps> = ({ open, onClose }) => {
  const { t } = useTranslation("common");
  const dispatch = useDispatch<AppDispatch>();
  const reduxVariables = useSelector(selectVariables);

  // Local editable copy
  const [localVars, setLocalVars] = useState<WorkflowVariable[]>([]);
  const [errors, setErrors] = useState<Record<string, string>>({});

  // Sync from Redux when dialog opens
  useEffect(() => {
    if (open) {
      setLocalVars(reduxVariables.map((v) => ({ ...v })));
      setErrors({});
    }
  }, [open, reduxVariables]);

  const validate = useCallback(
    (vars: WorkflowVariable[]): Record<string, string> => {
      const errs: Record<string, string> = {};
      const names = new Set<string>();
      for (const v of vars) {
        if (!v.name.trim()) {
          errs[v.id] = t("workflow_variable_name_required");
        } else if (!VARIABLE_NAME_REGEX.test(v.name)) {
          errs[v.id] = t("workflow_variable_name_invalid");
        } else if (names.has(v.name)) {
          errs[v.id] = t("workflow_variable_name_duplicate");
        } else if (
          v.type === "number" &&
          v.defaultValue !== undefined &&
          v.defaultValue !== "" &&
          isNaN(Number(v.defaultValue))
        ) {
          errs[v.id] = t("workflow_variable_invalid_number");
        }
        names.add(v.name);
      }
      return errs;
    },
    [t]
  );

  const handleAdd = useCallback(() => {
    const newVar: WorkflowVariable = {
      id: `var-${uuidv4()}`,
      name: "",
      type: "number",
      defaultValue: "",
      order: localVars.length,
    };
    setLocalVars((prev) => [...prev, newVar]);
  }, [localVars.length]);

  const handleUpdate = useCallback((id: string, changes: Partial<WorkflowVariable>) => {
    setLocalVars((prev) => prev.map((v) => (v.id === id ? { ...v, ...changes } : v)));
    setErrors((prev) => {
      const next = { ...prev };
      delete next[id];
      return next;
    });
  }, []);

  const handleRemove = useCallback((id: string) => {
    setLocalVars((prev) => prev.filter((v) => v.id !== id));
    setErrors((prev) => {
      const next = { ...prev };
      delete next[id];
      return next;
    });
  }, []);

  const handleDone = useCallback(() => {
    const errs = validate(localVars);
    if (Object.keys(errs).length > 0) {
      setErrors(errs);
      return;
    }
    // Normalize: recompute order, coerce number defaults
    const normalized = localVars.map((v, i) => ({
      ...v,
      order: i,
      defaultValue:
        v.type === "number" && v.defaultValue !== undefined && v.defaultValue !== ""
          ? Number(v.defaultValue)
          : v.defaultValue,
    }));
    dispatch(setVariables(normalized));
    onClose();
  }, [localVars, validate, dispatch, onClose]);

  return (
    <AppDialog
      open={open}
      onClose={onClose}
      icon={ICON_NAME.VARIABLE}
      title={t("workflow_variables")}
      maxWidth={600}
      // The rows between the variables run to the frame's edges; only the
      // vertical padding a dialog body has is kept.
      bleed
      bodySx={{ py: 5 }}
      footer={<AppDialogFooter onCancel={onClose} primaryLabel={t("done")} onPrimary={handleDone} />}>
      {localVars.length === 0 ? (
        <Stack alignItems="center" spacing={1} sx={{ py: 4 }}>
          <Icon iconName={ICON_NAME.VARIABLE} fontSize="small" htmlColor="text.secondary" />
          <Typography variant="body2" color="text.secondary" textAlign="center">
            {t("workflow_variable_none_defined")}
          </Typography>
        </Stack>
      ) : (
        <Stack spacing={0} sx={{ mt: 1 }}>
          {/* Header row */}
          <Box sx={{ display: "flex", gap: 1, px: 3, mb: 1 }}>
            <Typography variant="caption" color="text.secondary" fontWeight="bold" sx={{ flex: 2 }}>
              {t("workflow_variable_name")}
            </Typography>
            <Typography variant="caption" color="text.secondary" fontWeight="bold" sx={{ width: 100 }}>
              {t("workflow_variable_type")}
            </Typography>
            <Typography variant="caption" color="text.secondary" fontWeight="bold" sx={{ flex: 1.5 }}>
              {t("workflow_variable_default")}
            </Typography>
            <Box sx={{ width: 36 }} />
          </Box>

          <Divider />

          {/* Variable rows */}
          {localVars.map((variable, index) => (
            <React.Fragment key={variable.id}>
              <Box sx={{ display: "flex", gap: 1, alignItems: "flex-start", px: 3, py: 1.5 }}>
                <TextField
                  size="small"
                  placeholder={t("workflow_variable_name_placeholder")}
                  value={variable.name}
                  onChange={(e) => handleUpdate(variable.id, { name: e.target.value.replace(/\s/g, "_") })}
                  error={!!errors[variable.id]}
                  helperText={errors[variable.id]}
                  sx={{ flex: 2 }}
                  inputProps={{ style: { fontSize: "0.8125rem", fontFamily: "monospace" } }}
                />
                <Select
                  size="small"
                  value={variable.type}
                  onChange={(e) =>
                    handleUpdate(variable.id, {
                      type: e.target.value as "string" | "number",
                      defaultValue: "",
                    })
                  }
                  sx={{ width: 100, fontSize: "0.8125rem" }}>
                  <MenuItem value="string">{t("workflow_variable_type_string")}</MenuItem>
                  <MenuItem value="number">{t("workflow_variable_type_number")}</MenuItem>
                </Select>
                <TextField
                  size="small"
                  placeholder={variable.type === "number" ? "0" : t("workflow_variable_value_placeholder")}
                  type={variable.type === "number" ? "number" : "text"}
                  value={variable.defaultValue ?? ""}
                  onChange={(e) => handleUpdate(variable.id, { defaultValue: e.target.value })}
                  sx={{ flex: 1.5 }}
                  inputProps={{ style: { fontSize: "0.8125rem" } }}
                />
                <Tooltip title={t("delete")}>
                  <IconButton
                    size="small"
                    onClick={() => handleRemove(variable.id)}
                    color="error"
                    sx={{ mt: 0.25 }}>
                    <DeleteIcon fontSize="small" />
                  </IconButton>
                </Tooltip>
              </Box>
              {index < localVars.length - 1 && <Divider />}
            </React.Fragment>
          ))}
        </Stack>
      )}

      {/* Add button */}
      {localVars.length > 0 && <Divider />}
      <Stack alignItems="center" sx={{ mt: 2 }}>
        <Button
          startIcon={<AddIcon />}
          onClick={handleAdd}
          size="small"
          variant="text"
          sx={{ textTransform: "none" }}>
          {t("workflow_variable_add")}
        </Button>
      </Stack>
    </AppDialog>
  );
};

export default WorkflowVariablesDialog;
