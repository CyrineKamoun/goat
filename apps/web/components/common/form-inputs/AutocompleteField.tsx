import { Box, InputAdornment, ListItemIcon, Typography } from "@mui/material";
import Autocomplete from "@mui/material/Autocomplete";
import TextField from "@mui/material/TextField";
import * as React from "react";
import type { Control, FieldValues, Path } from "react-hook-form";
import { Controller } from "react-hook-form";

import { type ICON_NAME, Icon } from "@p4b/ui/components/Icon";

interface RhfAutocompleteFieldProps<
  O extends { value: string; label: string; icon?: React.ReactNode },
  TField extends FieldValues,
> {
  control: Control<TField>;
  name: Path<TField>;
  options: O[];
  startIcon?: ICON_NAME;
  label?: string;
  disabled?: boolean;
  required?: boolean;
}

export const RhfAutocompleteField = <
  O extends { value: string; label: string; icon?: React.ReactNode },
  TField extends FieldValues,
>(
  props: RhfAutocompleteFieldProps<O, TField>
) => {
  const { control, options, name } = props;
  return (
    <Controller
      name={name}
      control={control}
      rules={{
        required: "this field is requried",
      }}
      render={({ field, fieldState: { error } }) => {
        const { onChange, value, ref } = field;
        const selectedOption =
          options.find((option) => {
            return value === option.value;
          }) ?? null;
        return (
          <>
            <Autocomplete
              value={value ? selectedOption : null}
              disableClearable={value ? true : false}
              getOptionLabel={(option) => {
                return option.label;
              }}
              onChange={(_event: unknown, newValue) => {
                onChange(newValue ? newValue.value : null);
              }}
              id="controllable-autocomplete"
              options={options}
              renderInput={(params) => (
                <Box
                  sx={{
                    display: "flex",
                    alignItems: "center",
                    justifyContent: "center",
                  }}>
                  {props.startIcon && <Icon iconName={props.startIcon} style={{ marginRight: 10 }} />}
                  <TextField
                    {...params}
                    disabled={props.disabled}
                    label={props.label}
                    inputRef={ref}
                    name={name}
                    required={props.required}
                    InputProps={{
                      ...params.InputProps,
                      startAdornment: selectedOption?.icon && (
                        <InputAdornment position="start">{selectedOption.icon}</InputAdornment>
                      ),
                    }}
                    helperText={error ? error.message : null}
                    error={!!error}
                  />
                </Box>
              )}
              renderOption={(props, option) => {
                // MUI 5.15 puts the option key inside `props`. We pull
                // it out and apply it as a real React key so spreading
                // the rest doesn't trip the "key in spread" warning,
                // and so MUI's internal listbox sees the same key it
                // emitted. `option.value` is the fallback for any older
                // MUI build that doesn't emit one.
                const { key, ...rest } =
                  props as React.HTMLAttributes<HTMLLIElement> & { key?: React.Key };
                return (
                  <li key={key ?? option.value} {...rest}>
                    {option.icon && (
                      <ListItemIcon
                        sx={{
                          minWidth: 35,
                        }}>
                        {option.icon}
                      </ListItemIcon>
                    )}
                    <Typography variant="body1">{option.label}</Typography>
                  </li>
                );
              }}
            />
          </>
        );
      }}
    />
  );
};
