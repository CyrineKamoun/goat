/**
 * Generic Number Input Component
 *
 * Renders a number input with optional slider based on schema constraints.
 * Uses a local draft state — only commits to parent onChange on blur or Enter.
 * Supports cross-field validation via widget_options.max_value_from.
 */
import ErrorOutlineIcon from "@mui/icons-material/ErrorOutline";
import { InputAdornment, Stack, TextField, Tooltip } from "@mui/material";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import { getEffectiveSchema } from "@/lib/utils/ogc-utils";

import type { ProcessedInput } from "@/types/map/ogc-processes";

import FormLabelHelper from "@/components/common/FormLabelHelper";
import SliderInput from "@/components/map/panels/common/SliderInput";

interface NumberInputProps {
  input: ProcessedInput;
  value: number | undefined;
  onChange: (value: number | undefined) => void;
  disabled?: boolean;
  formValues?: Record<string, unknown>;
  onValidationChange?: (hasError: boolean) => void;
}

export default function NumberInput({ input, value, onChange, disabled, formValues = {}, onValidationChange }: NumberInputProps) {
  const effectiveSchema = useMemo(() => getEffectiveSchema(input.schema), [input.schema]);
  const [error, setError] = useState<string | null>(null);

  // When a placeholder is configured and value equals the default, show empty field
  const customPlaceholder = input.uiMeta?.widget_options?.placeholder as string | undefined;
  const showAsEmpty = customPlaceholder && value === input.defaultValue;

  // Local draft for the text field — only committed on blur/Enter
  const [draft, setDraft] = useState<string>(value !== undefined && !showAsEmpty ? String(value) : "");
  const isFocusedRef = useRef(false);

  // Sync draft from prop when value changes externally (not while user is editing)
  useEffect(() => {
    if (!isFocusedRef.current) {
      setDraft(value !== undefined && !showAsEmpty ? String(value) : "");
    }
  }, [value, showAsEmpty]);

  // Extract constraints
  const min = effectiveSchema.minimum;
  const max = effectiveSchema.maximum;
  const hasRange = min !== undefined && max !== undefined;

  // Resolve dynamic max from another field's value
  const maxValueFrom = input.uiMeta?.widget_options?.max_value_from as
    | { fields: (string | { field: string; when?: Record<string, string> })[]; message: string; max?: number; min?: number }
    | undefined;

  const dynamicMax = useMemo(() => {
    if (!maxValueFrom) return undefined;
    let resolved: number | undefined;
    for (const entry of maxValueFrom.fields) {
      const fieldName = typeof entry === "string" ? entry : entry.field;
      const when = typeof entry === "string" ? undefined : entry.when;

      if (when) {
        const match = Object.entries(when).every(([k, v]) => formValues[k] === v);
        if (!match) continue;
      }

      const val = formValues[fieldName];
      if (val !== undefined && val !== null) {
        resolved = Number(val);
        break;
      }
    }
    if (maxValueFrom.max !== undefined) {
      if (resolved === undefined) return maxValueFrom.max;
      return Math.min(resolved, maxValueFrom.max);
    }
    return resolved;
  }, [maxValueFrom, formValues]);

  const dynamicMin = maxValueFrom?.min;

  const validate = useCallback((val: number | undefined): string | null => {
    if (val === undefined) return null;
    if (min !== undefined && val < min) return `Value must be at least ${min}`;
    if (max !== undefined && val > max) return `Value must be at most ${max}`;
    if (dynamicMin !== undefined && val < dynamicMin && maxValueFrom) {
      return maxValueFrom.message;
    }
    if (dynamicMax !== undefined && val > dynamicMax && maxValueFrom) {
      return maxValueFrom.message;
    }
    return null;
  }, [min, max, dynamicMin, dynamicMax, maxValueFrom]);

  // Clear error when value becomes valid (e.g. when the referenced field changes)
  useEffect(() => {
    if (error && validate(value) === null) {
      setError(null);
      onValidationChange?.(false);
    }
  }, [value, error, validate, onValidationChange]);

  // Determine if we should use a slider (has reasonable range)
  const useSlider = useMemo(() => {
    if (!hasRange) return false;
    const range = (max as number) - (min as number);
    return range <= 10000 && range > 0;
  }, [hasRange, min, max]);

  const handleSliderChange = (newValue: number) => {
    onChange(newValue);
  };

  const handleTextChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    setDraft(event.target.value);
  };

  const commit = () => {
    const newValue = draft === "" ? (customPlaceholder ? input.defaultValue as number : undefined) : Number(draft);
    onChange(newValue);
    const err = validate(newValue);
    setError(err);
    onValidationChange?.(err !== null);
  };

  const handleFocus = () => {
    isFocusedRef.current = true;
  };

  const handleBlur = () => {
    isFocusedRef.current = false;
    commit();
  };

  const handleKeyDown = (event: React.KeyboardEvent) => {
    if (event.key === "Enter") {
      commit();
    }
  };

  if (useSlider) {
    return (
      <Stack>
        <FormLabelHelper label={input.title} tooltip={input.description} color="inherit" />
        <SliderInput
          value={value ?? (input.defaultValue as number) ?? min ?? 0}
          isRange={false}
          min={min as number}
          max={max as number}
          step={effectiveSchema.type === "integer" ? 1 : undefined}
          onChange={handleSliderChange}
        />
      </Stack>
    );
  }

  return (
    <Stack>
      <FormLabelHelper label={input.title} tooltip={input.description} color="inherit" />
      <TextField
        type="text"
        inputMode="numeric"
        size="small"
        value={draft}
        onChange={handleTextChange}
        onFocus={handleFocus}
        onBlur={handleBlur}
        onKeyDown={handleKeyDown}
        error={!!error}
        disabled={disabled}
        InputProps={
          error
            ? {
                endAdornment: (
                  <InputAdornment position="end">
                    <Tooltip title={error} arrow>
                      <ErrorOutlineIcon color="error" fontSize="small" />
                    </Tooltip>
                  </InputAdornment>
                ),
              }
            : undefined
        }
        placeholder={
          (input.uiMeta?.widget_options?.placeholder as string) ??
          (input.defaultValue !== undefined ? String(input.defaultValue) : undefined)
        }
        fullWidth
      />
    </Stack>
  );
}
