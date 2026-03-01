import ClearIcon from "@mui/icons-material/Clear";
import SearchIcon from "@mui/icons-material/Search";
import {
  Checkbox,
  FormControl,
  IconButton,
  InputAdornment,
  ListSubheader,
  MenuItem,
  Select,
  TextField,
  Typography,
  styled,
  useTheme,
} from "@mui/material";
import { useMemo, useState } from "react";
import { useTranslation } from "react-i18next";

import type { LayerFieldType } from "@/lib/validations/layer";

import FormLabelHelper from "@/components/common/FormLabelHelper";

export type SelectorProps<T extends boolean = false> = {
  selectedField: T extends true ? LayerFieldType[] | undefined : LayerFieldType | undefined;
  setSelectedField: (
    field: T extends true ? LayerFieldType[] | undefined : LayerFieldType | undefined
  ) => void;
  fields: LayerFieldType[];
  label?: string;
  tooltip?: string;
  disabled?: boolean;
  multiple?: T;
  onFocus?: () => void;
  onClose?: () => void;
};

export const containsText = (text: string, searchText: string) =>
  text.toLowerCase().indexOf(searchText.toLowerCase()) > -1;

export const FieldTypeColors = {
  string: [140, 210, 205],
  number: [248, 194, 28],
  object: [255, 138, 101],
};

export const FieldTypeTag = styled("div")<{ fieldType: string }>(({ fieldType }) => ({
  backgroundColor: `rgba(${FieldTypeColors[fieldType]}, 0.1)`,
  borderRadius: 4,
  border: `1px solid rgb(${FieldTypeColors[fieldType]})`,
  color: `rgb(${FieldTypeColors[fieldType]})`,
  display: "inline-block",
  fontSize: 10,
  fontWeight: "bold",
  padding: "0 5px",
  marginRight: "10px",
  textAlign: "center",
  width: "50px",
  lineHeight: "20px",
}));

// Empty array constant to avoid creating new references
const EMPTY_FIELDS: LayerFieldType[] = [];

const LayerFieldSelector = (props: SelectorProps) => {
  const theme = useTheme();
  const [searchText, setSearchText] = useState("");
  const { selectedField, fields, setSelectedField } = props;
  const [focused, setFocused] = useState(false);

  // Ensure fields is always an array (defensive check) - use stable reference
  const safeFields = fields && fields.length > 0 ? fields : EMPTY_FIELDS;

  const displayedfields = useMemo(() => {
    if (safeFields.length === 0) return EMPTY_FIELDS;
    const filtered = safeFields.filter((field) => {
      return containsText(field.name, searchText);
    });
    return filtered;
  }, [safeFields, searchText]);

  const { t } = useTranslation("common");
  const selectedValue = useMemo(() => {
    if (!props.multiple && !Array.isArray(selectedField)) {
      return selectedField ? JSON.stringify(selectedField) : "";
    } else {
      return selectedField && Array.isArray(selectedField) && selectedField.length > 0
        ? selectedField.map((field) => JSON.stringify(field))
        : EMPTY_FIELDS;
    }
  }, [props.multiple, selectedField]);

  return (
    <FormControl size="small" fullWidth>
      {props.label && (
        <FormLabelHelper
          label={props.label}
          color={
            props.disabled ? theme.palette.secondary.main : focused ? theme.palette.primary.main : "inherit"
          }
          tooltip={props.tooltip}
        />
      )}
      <Select
        fullWidth
        MenuProps={{
          autoFocus: false,
          sx: { width: "120px" },
          slotProps: {
            paper: {
              sx: {
                maxHeight: "350px",
                overflowY: "auto",
              },
            },
          },
        }}
        multiple={props.multiple ? true : false}
        disabled={props.disabled}
        IconComponent={() => null}
        sx={{ pr: 1 }}
        displayEmpty
        value={selectedValue as unknown}
        defaultValue={props.multiple ? EMPTY_FIELDS : ""}
        onChange={(e) => {
          try {
            if (!props.multiple) {
              const value = e.target.value as string;
              if (!value) return;
              const field = JSON.parse(value) as LayerFieldType;
              setSelectedField(field as LayerFieldType);
            } else if (props.multiple) {
              const fields = e.target.value as string[];
              const selectedFields = fields
                .filter((f) => f) // Filter out empty strings
                .map((field) => JSON.parse(field) as LayerFieldType);
              // eslint-disable-next-line @typescript-eslint/no-explicit-any
              setSelectedField(selectedFields as any);
            }
          } catch (error) {
            console.error("Failed to parse field selection:", error);
          }
        }}
        onClose={() => {
          setFocused(false);
          setSearchText("");
          if (props.onClose) {
            props.onClose();
          }
        }}
        onFocus={() => {
          setFocused(true);
          if (props.onFocus) {
            props.onFocus();
          }
        }}
        onBlur={() => setFocused(false)}
        startAdornment={
          <>
            {/* Only show field type tag for single select mode */}
            {!props.multiple &&
              selectedField &&
              !Array.isArray(selectedField) &&
              FieldTypeColors[selectedField.type] && (
                <FieldTypeTag fieldType={selectedField.type}>{selectedField.type}</FieldTypeTag>
              )}
          </>
        }
        endAdornment={
          <IconButton
            size="small"
            sx={{
              visibility:
                !selectedField ||
                (props.multiple && Array.isArray(selectedField) && selectedField.length === 0)
                  ? "hidden"
                  : "visible",
            }}
            onClick={() => {
              if (props.multiple) {
                // eslint-disable-next-line @typescript-eslint/no-explicit-any
                setSelectedField([] as any);
              } else {
                setSelectedField(undefined);
              }
            }}>
            <ClearIcon />
          </IconButton>
        }
        renderValue={() => {
          if (!selectedField && !props.multiple)
            return <Typography variant="body2">{t("select_field")}</Typography>;
          if (props.multiple && Array.isArray(selectedField) && selectedField.length === 0)
            return <Typography variant="body2">{t("select_fields")}</Typography>;
          return (
            <>
              {selectedField && (
                <Typography variant="body2" fontWeight="bold">
                  {props.multiple && Array.isArray(selectedField)
                    ? selectedField.map((f) => f.name).join(", ")
                    : selectedField.name}
                </Typography>
              )}
            </>
          );
        }}>
        <ListSubheader sx={{ px: 2, pt: 1 }}>
          <TextField
            size="small"
            autoFocus
            placeholder="Search"
            InputProps={{
              startAdornment: (
                <InputAdornment position="start">
                  <SearchIcon />
                </InputAdornment>
              ),
            }}
            onChange={(e) => setSearchText(e.target.value)}
            onKeyDown={(e) => {
              if (e.key !== "Escape") {
                e.stopPropagation();
              }
            }}
          />
        </ListSubheader>
        {displayedfields.map((field) => (
          <MenuItem sx={{ px: 2 }} key={field.name} value={JSON.stringify(field)}>
            {props.multiple && Array.isArray(selectedField) && (
              <Checkbox
                sx={{ mr: 2, p: 0 }}
                size="small"
                checked={selectedField ? selectedField.some((f) => f.name === field.name) : false}
              />
            )}

            {FieldTypeColors[field.type] && <FieldTypeTag fieldType={field.type}>{field.type}</FieldTypeTag>}
            <Typography variant="body2" fontWeight="bold">
              {field.name}
            </Typography>
          </MenuItem>
        ))}
      </Select>
    </FormControl>
  );
};

export default LayerFieldSelector;
