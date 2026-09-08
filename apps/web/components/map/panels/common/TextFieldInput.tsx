import ClearIcon from "@mui/icons-material/Clear";
import { FormControl, IconButton, InputAdornment, OutlinedInput, useTheme } from "@mui/material";
import type { InputBaseComponentProps } from "@mui/material";
import React, { useState } from "react";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import FormLabelHelper from "@/components/common/FormLabelHelper";

type TextFieldInputProps = {
  value?: string;
  onChange: (value: string) => void;
  label?: string;
  disabled?: boolean;
  tooltip?: string;
  onFocus?: () => void;
  type?: "text" | "number" | "date";
  clearable?: boolean;
  /** Something other than the user maintains this value. Shows a lock rather
   *  than leaving the field looking merely greyed out, so it reads as "not
   *  yours to set" instead of "temporarily unavailable". */
  locked?: boolean;
  placeholder?: string;
  multiline?: boolean;
  rows?: number;
  autoFocus?: boolean;
  /** Attributes for the native input — an `aria-label` when no visible
   * label names the field, a `data-testid`. */
  inputProps?: InputBaseComponentProps;
};

const TextFieldInput: React.FC<TextFieldInputProps> = ({
  value,
  onChange,
  label,
  disabled,
  tooltip,
  onFocus,
  type,
  placeholder = "",
  locked,
  clearable = true,
  multiline = false,
  rows = 2,
  autoFocus,
  inputProps,
}) => {
  const theme = useTheme();
  const [focused, setFocused] = useState(false);
  return (
    <FormControl size="small" fullWidth>
      {!!label && (
        <FormLabelHelper
          label={label}
          color={disabled ? theme.palette.secondary.main : focused ? theme.palette.primary.main : "inherit"}
          tooltip={tooltip}
        />
      )}

      <OutlinedInput
        value={value}
        onChange={(event) => onChange(event.target.value)}
        onFocus={() => {
          setFocused(true);
          if (onFocus) onFocus();
        }}
        multiline={multiline}
        rows={multiline ? rows : undefined}
        autoFocus={autoFocus}
        placeholder={placeholder}
        onBlur={() => setFocused(false)}
        disabled={disabled}
        size="small"
        sx={{ pr: 0, fontSize: "0.875rem" }}
        inputProps={{
          type,
          ...inputProps,
          style: {
            width: "100%",
            padding: multiline ? "8px 15px 8px 12px" : "0px 15px 0px 12px",
            // A multiline field is sized by its rows: the autosize shadow
            // textarea shares this style, and any height key here (even an
            // undefined one) replaces the shadow's zero height and inflates
            // the row measurement.
            ...(multiline ? {} : { height: "40px" }),
            // GOAT green accent for number input spinners
            accentColor: "#2BB381",
            ...inputProps?.style,
          },
        }}
        endAdornment={
          locked ? (
            <InputAdornment position="end" sx={{ mr: 2 }}>
              <Icon
                iconName={ICON_NAME.LOCK}
                htmlColor={theme.palette.text.disabled}
                style={{ fontSize: "14px" }}
              />
            </InputAdornment>
          ) : (
            !disabled &&
            !!value &&
            clearable && (
              <InputAdornment position="end" sx={{ mr: 2 }}>
                <IconButton size="small" aria-label="clear input" onClick={() => onChange("")} edge="end">
                  <ClearIcon />
                </IconButton>
              </InputAdornment>
            )
          )
        }
      />
    </FormControl>
  );
};

export default TextFieldInput;
