"use client";

import {
  Add as AddIcon,
  DataObject as VariablesIcon,
  DeleteOutline as DeleteIcon,
} from "@mui/icons-material";
import {
  Box,
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
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

import type { AppDispatch } from "@/lib/store";
import { selectVariables } from "@/lib/store/workflow/selectors";
import { setVariables } from "@/lib/store/workflow/slice";
import type { WorkflowVariable } from "@/lib/validations/workflow";

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
      type: "string",
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
    <Dialog open={open} onClose={onClose} fullWidth maxWidth="sm">
      <DialogTitle sx={{ display: "flex", alignItems: "center", gap: 1 }}>
        <VariablesIcon fontSize="small" />
        {t("workflow_variables")}
      </DialogTitle>
      <DialogContent>
        {localVars.length === 0 ? (
          <Stack alignItems="center" spacing={1.5} sx={{ py: 4 }}>
            <VariablesIcon sx={{ fontSize: 40, color: "text.disabled" }} />
            <Typography variant="body2" color="text.secondary" textAlign="center">
              {t("workflow_variable_none_defined")}
            </Typography>
          </Stack>
        ) : (
          <Stack spacing={1} sx={{ mt: 1 }}>
            {/* Header row */}
            <Box sx={{ display: "flex", gap: 1, px: 0.5 }}>
              <Typography variant="caption" color="text.secondary" sx={{ flex: 2 }}>
                {t("workflow_variable_name")}
              </Typography>
              <Typography variant="caption" color="text.secondary" sx={{ width: 100 }}>
                {t("workflow_variable_type")}
              </Typography>
              <Typography variant="caption" color="text.secondary" sx={{ flex: 1.5 }}>
                {t("workflow_variable_default")}
              </Typography>
              <Box sx={{ width: 36 }} />
            </Box>

            {/* Variable rows */}
            {localVars.map((variable) => (
              <Box key={variable.id} sx={{ display: "flex", gap: 1, alignItems: "flex-start" }}>
                <TextField
                  size="small"
                  placeholder="variable_name"
                  value={variable.name}
                  onChange={(e) =>
                    handleUpdate(variable.id, { name: e.target.value.replace(/\s/g, "_") })
                  }
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
                  <MenuItem value="string">String</MenuItem>
                  <MenuItem value="number">Number</MenuItem>
                </Select>
                <TextField
                  size="small"
                  placeholder={variable.type === "number" ? "0" : "value"}
                  type={variable.type === "number" ? "number" : "text"}
                  value={variable.defaultValue ?? ""}
                  onChange={(e) => handleUpdate(variable.id, { defaultValue: e.target.value })}
                  sx={{ flex: 1.5 }}
                  inputProps={{ style: { fontSize: "0.8125rem" } }}
                />
                <Tooltip title={t("delete")}>
                  <IconButton size="small" onClick={() => handleRemove(variable.id)} sx={{ mt: 0.25 }}>
                    <DeleteIcon fontSize="small" />
                  </IconButton>
                </Tooltip>
              </Box>
            ))}
          </Stack>
        )}

        {/* Add button */}
        <Button
          startIcon={<AddIcon />}
          onClick={handleAdd}
          size="small"
          sx={{ mt: 2, textTransform: "none" }}>
          {t("workflow_variable_add")}
        </Button>
      </DialogContent>
      <DialogActions sx={{ pb: 2 }}>
        <Button onClick={handleDone} variant="text" sx={{ borderRadius: 0 }}>
          <Typography variant="body2" fontWeight="bold">
            {t("done")}
          </Typography>
        </Button>
      </DialogActions>
    </Dialog>
  );
};

export default WorkflowVariablesDialog;
