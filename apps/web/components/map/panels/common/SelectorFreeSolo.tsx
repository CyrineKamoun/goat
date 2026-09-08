import { FormControl, MenuItem, TextField, Typography, useTheme } from "@mui/material";
import Autocomplete, { createFilterOptions } from "@mui/material/Autocomplete";
import { useState } from "react";

import type { SelectorItem } from "@/types/map/common";

import FormLabelHelper from "@/components/common/FormLabelHelper";

interface SelectorFreeSoloProps {
  label?: string;
  tooltip?: string;
  placeholder?: string;
  disabled?: boolean;
  options: SelectorItem[];
  selectedItem: SelectorItem | undefined;
  onClear?: () => void;
  onSelect?: (value: SelectorItem | undefined) => void;
  inputType?: "number" | "text";
  /** Commit what has been typed when focus leaves, rather than only on Enter
   *  or a pick. For a field whose value is the point of the form, losing the
   *  text on a click elsewhere reads as the input having been ignored. */
  commitOnBlur?: boolean;
}

const filter = createFilterOptions<SelectorItem>();

const SelectorFreeSolo = (props: SelectorFreeSoloProps) => {
  const theme = useTheme();
  const [focused, setFocused] = useState(false);
  return (
    <FormControl size="small" fullWidth>
      {props.label && (
        <FormLabelHelper
          label={props.label}
          color={
            props.disabled
              ? theme.palette.secondary.main
              : focused
                ? theme.palette.primary.main
                : theme.palette.text.secondary
          }
          tooltip={props.tooltip}
        />
      )}

      <Autocomplete
        freeSolo
        autoSelect={props.commitOnBlur}
        disabled={props.disabled}
        options={props.options}
        // `undefined` would make the Autocomplete uncontrolled until the first
        // value arrives; a field with nothing in it is still controlled.
        value={props.selectedItem ?? null}
        onFocus={() => {
          setFocused(true);
        }}
        onBlur={() => setFocused(false)}
        size="small"
        onChange={(_event, newValue) => {
          if (typeof newValue === "string") {
            props.onSelect?.({
              value: newValue,
              label: newValue,
            });
          } else if (typeof newValue === "object" && newValue !== null) {
            // Create a new value from the user input
            props.onSelect?.(newValue);
          } else {
            props.onSelect?.(undefined);
          }
        }}
        filterOptions={(options, params) => {
          const filtered = filter(options, params);
          const { inputValue } = params;
          // Suggest the creation of a new value
          const isExisting = options.some((option) => inputValue === option.label);
          if (inputValue !== "" && !isExisting) {
            filtered.push({
              value: inputValue,
              label: inputValue,
            });
          }

          return filtered;
        }}
        renderOption={(props, option) => (
          <MenuItem {...props} key={option.value}>
            <Typography
              variant="body2"
              fontWeight="bold"
              style={{
                overflow: "hidden",
                textOverflow: "ellipsis",
                whiteSpace: "nowrap",
              }}>
              {option.label}
            </Typography>
          </MenuItem>
        )}
        renderInput={(params) => (
          <TextField
            type={props.inputType || "text"}
            sx={{
              "& input::placeholder": {
                fontSize: "0.875rem",
              },
            }}
            placeholder={props.placeholder}
            {...params}
          />
        )}
      />
    </FormControl>
  );
};

export default SelectorFreeSolo;
