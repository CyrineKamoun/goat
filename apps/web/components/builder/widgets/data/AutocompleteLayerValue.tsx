/* eslint-disable @typescript-eslint/no-explicit-any */
import {
  Autocomplete,
  Checkbox,
  CircularProgress,
  FormControl,
  TextField,
  Typography,
  useTheme,
} from "@mui/material";
import { useMemo, useState } from "react";
import { useTranslation } from "react-i18next";

import { useDatasetValueSelectorMethods } from "@/hooks/map/DatasetHooks";

import FormLabelHelper from "@/components/common/FormLabelHelper";

export type AutocompleteLayerValueProps = {
  selectedValues: string[] | string | null;
  onSelectedValuesChange: (values: string[] | string | null) => void;
  layerId: string;
  fieldName: string;
  label?: string;
  tooltip?: string;
  disabled?: boolean;
  multiple?: boolean;
  placeholder?: string;
  onFocus?: () => void;
  onClose?: () => void;
  cqlFilter?: object | undefined;
  labelMap?: [string, string][];
};

type OptionItem = {
  value: string;
  count?: number;
};

const AutocompleteLayerValue = (props: AutocompleteLayerValueProps) => {
  const theme = useTheme();
  const { t } = useTranslation("common");

  const labelMapLookup = useMemo(() => new Map(props.labelMap || []), [props.labelMap]);

  const getLabel = (value: string) => labelMapLookup.get(value) || value;

  const [open, setOpen] = useState(false);

  const { data, isLoading, searchText, setSearchText, debouncedSetSearchText } =
    useDatasetValueSelectorMethods({
      selectedValues: props.multiple
        ? (props.selectedValues as string[])
        : [props.selectedValues as string],
      onSelectedValuesChange: props.onSelectedValuesChange,
      fieldName: props.fieldName,
      datasetId: props.layerId,
      cqlFilter: props.cqlFilter,
    });

  // Build the options list: selected values first, then the rest
  const selectedValuesArray: string[] = props.multiple
    ? ((props.selectedValues as string[]) || []).filter((v) => v != null)
    : props.selectedValues
      ? [props.selectedValues as string]
      : [];

  const selectedValuesSet = new Set(selectedValuesArray);

  const options: OptionItem[] = useMemo(() => {
    const selectedItems = selectedValuesArray.map((val) => {
      const existing = data?.items?.find((item) => item.value === val);
      return existing || { value: val };
    });
    const otherItems = data?.items?.filter((item) => !selectedValuesSet.has(item.value)) || [];
    return [...selectedItems, ...otherItems];
    // selectedValuesArray is re-derived each render; join produces a stable string dependency
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [data?.items, selectedValuesArray.join(",")]);

  // Compute the Autocomplete value from props
  const autocompleteValue = props.multiple
    ? options.filter((opt) => selectedValuesSet.has(opt.value))
    : options.find((opt) => opt.value === props.selectedValues) || null;

  return (
    <FormControl size="small" fullWidth>
      {props.label && (
        <FormLabelHelper
          label={props.label}
          color={props.disabled ? theme.palette.secondary.main : "inherit"}
          tooltip={props.tooltip}
        />
      )}
      <Autocomplete
        size="small"
        fullWidth
        multiple={props.multiple as any}
        disabled={props.disabled}
        options={options}
        value={autocompleteValue as any}
        loading={isLoading}
        disableCloseOnSelect={props.multiple}
        getOptionLabel={(option: OptionItem) => getLabel(option.value)}
        isOptionEqualToValue={(option: OptionItem, value: OptionItem) =>
          option.value === value.value
        }
        open={open}
        onOpen={() => setOpen(true)}
        onClose={() => {
          setOpen(false);
          setSearchText("");
          debouncedSetSearchText("");
          props.onClose?.();
        }}
        filterOptions={(x) => x}
        inputValue={open ? searchText : (autocompleteValue && !props.multiple ? getLabel((autocompleteValue as OptionItem).value) : searchText)}
        onInputChange={(_event, newInputValue, reason) => {
          if (reason === "input") {
            setSearchText(newInputValue);
            debouncedSetSearchText(newInputValue);
          } else if (reason === "reset" || reason === "clear") {
            setSearchText("");
            debouncedSetSearchText("");
          }
        }}
        onChange={(_event, newValue) => {
          if (props.multiple) {
            const values = (newValue as OptionItem[] | null)?.map((v) => v.value) || [];
            props.onSelectedValuesChange(values.length > 0 ? values : []);
          } else {
            const value = newValue as OptionItem | null;
            props.onSelectedValuesChange(value ? value.value : null);
          }
        }}
        onFocus={props.onFocus}
        noOptionsText={t("no_values_found")}
        loadingText={t("loading")}
        slotProps={{
          listbox: {
            sx: {
              maxHeight: "350px",
            },
          },
        }}
        renderOption={(renderProps, option: OptionItem, { selected }) => (
          <li {...renderProps} key={option.value}>
            {props.multiple && <Checkbox sx={{ mr: 1, p: 0 }} size="small" checked={selected} />}
            <Typography variant="body2" fontWeight="bold">
              {getLabel(option.value)}
            </Typography>
          </li>
        )}
        renderInput={(params) => (
          <TextField
            {...params}
            placeholder={
              props.multiple && selectedValuesArray.length > 0
                ? undefined
                : (props.placeholder ?? t(props.multiple ? "select_values" : "select_value"))
            }
            InputProps={{
              ...params.InputProps,
              endAdornment: (
                <>
                  {isLoading ? <CircularProgress color="inherit" size={18} /> : null}
                  {params.InputProps.endAdornment}
                </>
              ),
            }}
          />
        )}
      />
    </FormControl>
  );
};

export default AutocompleteLayerValue;
