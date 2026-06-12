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
import { useTranslation } from "react-i18next";

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
  const { t } = useTranslation("common");
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

  // Resolve dynamic max from another field's value (or a literal cap).
  // Each entry in `fields` can be:
  //   - a plain string: name of a field whose value is the max
  //   - { field, when?, message? }: lookup the named field's value, optionally
  //     gated by a `when` condition; per-entry message overrides top-level
  //   - { value, when?, message? }: literal cap, optionally gated; used to
  //     express per-enum-value caps (e.g. mode-specific speed limits)
  // First matching entry wins. Falls back to top-level `max` if nothing matched.
  type MaxEntry =
    | string
    | { field: string; when?: Record<string, string>; message?: string }
    | { value: number; when?: Record<string, string>; message?: string };
  const maxValueFrom = input.uiMeta?.widget_options?.max_value_from as
    | { fields: MaxEntry[]; message: string; max?: number; min?: number }
    | undefined;

  const matched = useMemo(() => {
    if (!maxValueFrom) return undefined as { max: number; message?: string } | undefined;
    for (const entry of maxValueFrom.fields) {
      if (typeof entry === "string") {
        const v = formValues[entry];
        if (v !== undefined && v !== null) return { max: Number(v) };
        continue;
      }
      if (entry.when) {
        const ok = Object.entries(entry.when).every(([k, v]) => formValues[k] === v);
        if (!ok) continue;
      }
      if ("value" in entry) {
        return { max: entry.value, message: entry.message };
      }
      const v = formValues[entry.field];
      if (v !== undefined && v !== null) {
        return { max: Number(v), message: entry.message };
      }
    }
    return undefined;
  }, [maxValueFrom, formValues]);

  const dynamicMax = useMemo(() => {
    if (!maxValueFrom) return undefined;
    if (matched && maxValueFrom.max !== undefined) return Math.min(matched.max, maxValueFrom.max);
    if (matched) return matched.max;
    return maxValueFrom.max;
  }, [maxValueFrom, matched]);

  const dynamicMin = maxValueFrom?.min;
  const dynamicMessage = matched?.message ?? maxValueFrom?.message;

  const validate = useCallback((val: number | undefined): string | null => {
    if (val === undefined) return null;
    if (Number.isNaN(val)) return t("invalid_number");
    if (min !== undefined && val < min) return `Value must be at least ${min}`;
    if (max !== undefined && val > max) return `Value must be at most ${max}`;
    if (dynamicMin !== undefined && val < dynamicMin && dynamicMessage) {
      return t(dynamicMessage);
    }
    if (dynamicMax !== undefined && val > dynamicMax && dynamicMessage) {
      return t(dynamicMessage);
    }
    return null;
  }, [min, max, dynamicMin, dynamicMax, dynamicMessage, t]);

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

  // Allow only characters that can form a valid (signed, decimal) number.
  // Empty string is allowed (means "cleared"). Anything else must match the
  // pattern below — no letters, no scientific notation, no double signs/dots.
  const NUMERIC_INPUT_PATTERN = /^-?\d*\.?\d*$/;
  const handleTextChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    const next = event.target.value;
    if (next === "" || NUMERIC_INPUT_PATTERN.test(next)) {
      setDraft(next);
    }
  };

  const commit = () => {
    if (draft === "") {
      const fallback = customPlaceholder ? (input.defaultValue as number | undefined) : undefined;
      onChange(fallback);
      const err = validate(fallback);
      setError(err);
      onValidationChange?.(err !== null);
      return;
    }
    const parsed = Number(draft);
    if (Number.isNaN(parsed)) {
      // Don't propagate NaN to the backend; keep the bad text so the user
      // can see/fix it, and surface a validation error so submit is blocked.
      const err = t("invalid_number");
      setError(err);
      onValidationChange?.(true);
      return;
    }
    onChange(parsed);
    const err = validate(parsed);
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
        inputMode="decimal"
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
          customPlaceholder !== undefined
            ? t(customPlaceholder)
            : (input.defaultValue != null ? String(input.defaultValue) : undefined)
        }
        fullWidth
      />
    </Stack>
  );
}
