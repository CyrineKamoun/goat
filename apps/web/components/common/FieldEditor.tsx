import type { DragEndEvent } from "@dnd-kit/core";
import { DndContext, closestCenter } from "@dnd-kit/core";
import { restrictToVerticalAxis } from "@dnd-kit/modifiers";
import { SortableContext, useSortable, verticalListSortingStrategy } from "@dnd-kit/sortable";
import { CSS } from "@dnd-kit/utilities";
import { DragIndicator as DragIndicatorIcon } from "@mui/icons-material";
import {
  Box,
  Button,
  IconButton,
  Input,
  Stack,
  Typography,
  alpha,
  useTheme,
} from "@mui/material";
import { useMemo } from "react";
import { useTranslation } from "react-i18next";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import type { FieldDefinition } from "@/lib/validations/layer";
import { RESERVED_FIELD_NAMES } from "@/lib/validations/layer";

import type { SelectorItem } from "@/types/map/common";

import Selector from "@/components/map/panels/common/Selector";

interface FieldEditorProps {
  fields: FieldDefinition[];
  onChange: (fields: FieldDefinition[]) => void;
  selectedFieldId: string | null;
  onSelectField: (id: string | null) => void;
  /** Optional override for remove — e.g. to show a confirmation dialog for existing fields */
  onRemoveOverride?: (id: string) => void;
  /** IDs of fields whose type cannot be changed (existing DB columns) */
  lockedFieldIds?: Set<string>;
}

const FIELD_TYPE_ICON: Record<string, ICON_NAME> = {
  string: ICON_NAME.LETTER_T,
  number: ICON_NAME.HASHTAG,
};

const fieldTypeItems: SelectorItem[] = [
  { value: "string", label: "Text", icon: ICON_NAME.LETTER_T },
  { value: "number", label: "Number", icon: ICON_NAME.HASHTAG },
];

// --- Sortable field row ---

interface SortableFieldRowProps {
  field: FieldDefinition;
  isSelected: boolean;
  error?: string;
  warning?: string;
  onSelect: () => void;
  onRename: (newName: string) => void;
  onDuplicate: () => void;
  onRemove: () => void;
}

const SortableFieldRow = ({
  field,
  isSelected,
  error,
  warning,
  onSelect,
  onRename,
  onDuplicate,
  onRemove,
}: SortableFieldRowProps) => {
  const theme = useTheme();
  const { attributes, listeners, setNodeRef, transform, transition } = useSortable({ id: field.id });

  const style = {
    transform: CSS.Transform.toString(transform),
    transition,
  };

  return (
    <Box ref={setNodeRef} style={style}>
      <Stack
        direction="row"
        alignItems="center"
        onClick={onSelect}
        sx={{
          px: 0.5,
          py: 1.25,
          cursor: "pointer",
          borderLeft: isSelected
            ? `3px solid ${error ? theme.palette.error.main : theme.palette.primary.main}`
            : "3px solid transparent",
          backgroundColor: isSelected
            ? alpha(theme.palette.primary.main, 0.08)
            : "transparent",
          "&:hover": {
            backgroundColor: isSelected
              ? alpha(theme.palette.primary.main, 0.12)
              : theme.palette.action.hover,
            "& .field-actions": { opacity: 1 },
            "& .field-drag-handle": { opacity: 1 },
          },
        }}>
        {/* Drag handle */}
        <Box
          {...attributes}
          {...listeners}
          className="field-drag-handle"
          onClick={(e) => e.stopPropagation()}
          sx={{ cursor: "grab", display: "flex", alignItems: "center", mr: 0.5, opacity: 0 }}>
          <DragIndicatorIcon sx={{ fontSize: 16, color: theme.palette.text.secondary }} />
        </Box>

        {/* Type indicator */}
        <Box sx={{ width: 18, flexShrink: 0, display: "flex", justifyContent: "center" }}>
          <Icon
            iconName={FIELD_TYPE_ICON[field.type] || ICON_NAME.LETTER_T}
            style={{ fontSize: 12 }}
            htmlColor={error ? theme.palette.error.main : theme.palette.text.secondary}
          />
        </Box>

        {/* Always-visible input */}
        <Input
          value={field.name}
          onChange={(e) => onRename(e.target.value)}
          onFocus={onSelect}
          disableUnderline
          placeholder="Field name"
          autoFocus={isSelected}
          error={!!error}
          inputProps={{ maxLength: 128 }}
          sx={{
            flex: 1,
            ml: 0.5,
            "& .MuiInputBase-input": {
              py: 0.5,
              fontSize: "0.875rem",
            },
          }}
        />

        {/* Action buttons — visible on hover */}
        <Stack
          className="field-actions"
          direction="row"
          spacing={0}
          sx={{ flexShrink: 0, opacity: 0 }}>
          <IconButton
            size="small"
            onClick={(e) => {
              e.stopPropagation();
              onDuplicate();
            }}>
            <Icon
              iconName={ICON_NAME.COPY}
              style={{ fontSize: 14 }}
              htmlColor={theme.palette.text.secondary}
            />
          </IconButton>
          <IconButton
            size="small"
            onClick={(e) => {
              e.stopPropagation();
              onRemove();
            }}>
            <Icon
              iconName={ICON_NAME.MINUS}
              style={{ fontSize: 14 }}
              htmlColor={theme.palette.text.secondary}
            />
          </IconButton>
        </Stack>
      </Stack>
      {/* Error or warning message */}
      {error && (
        <Typography
          variant="caption"
          color="error"
          sx={{ pl: 6, pb: 0.5, display: "block" }}>
          {error}
        </Typography>
      )}
      {!error && warning && (
        <Typography
          variant="caption"
          color="warning.main"
          sx={{ pl: 6, pb: 0.5, display: "block" }}>
          {warning}
        </Typography>
      )}
    </Box>
  );
};

// --- Main FieldEditor ---

const FieldEditor: React.FC<FieldEditorProps> = ({
  fields,
  onChange,
  selectedFieldId,
  onSelectField,
  onRemoveOverride,
  lockedFieldIds,
}) => {
  const { t } = useTranslation("common");
  const theme = useTheme();

  const selectedField = fields.find((f) => f.id === selectedFieldId) ?? null;
  const hasFields = fields.length > 0;

  // Compute per-field validation errors
  // Simple identifier check — names that are safe for all export formats
  const SAFE_NAME_REGEX = /^[a-zA-Z_][a-zA-Z0-9_]*$/;

  const { fieldErrors, fieldWarnings } = useMemo(() => {
    const errors: Record<string, string> = {};
    const warnings: Record<string, string> = {};
    const nameCounts = new Map<string, number>();

    // Count occurrences of each name (case-insensitive)
    for (const field of fields) {
      const lower = field.name.toLowerCase();
      nameCounts.set(lower, (nameCounts.get(lower) || 0) + 1);
    }

    for (const field of fields) {
      const name = field.name;
      if (!name || name.trim().length === 0) {
        errors[field.id] = t("field_name_required");
      } else if (RESERVED_FIELD_NAMES.includes(name.toLowerCase())) {
        errors[field.id] = t("field_name_reserved");
      } else if ((nameCounts.get(name.toLowerCase()) || 0) > 1) {
        errors[field.id] = t("field_name_unique");
      } else if (!SAFE_NAME_REGEX.test(name)) {
        warnings[field.id] = t("field_name_special_chars_warning");
      }
    }
    return { fieldErrors: errors, fieldWarnings: warnings };
  }, [fields, t]);

  const handleAddField = () => {
    // Auto-increment name: new_field, new_field_2, new_field_3, ...
    const baseName = t("field_name_default");
    const existingNames = new Set(fields.map((f) => f.name.toLowerCase()));
    let fieldName = baseName;
    let counter = 2;
    while (existingNames.has(fieldName.toLowerCase())) {
      fieldName = `${baseName}_${counter}`;
      counter++;
    }
    const newField: FieldDefinition = {
      id: crypto.randomUUID(),
      name: fieldName,
      type: "string",
    };
    onChange([...fields, newField]);
    onSelectField(newField.id);
  };

  const handleDuplicate = (field: FieldDefinition) => {
    const copySuffix = t("field_name_copy_suffix");
    const existingNames = new Set(fields.map((f) => f.name.toLowerCase()));
    let copyName = `${field.name}_${copySuffix}`;
    let counter = 2;
    while (existingNames.has(copyName.toLowerCase())) {
      copyName = `${field.name}_${copySuffix}_${counter}`;
      counter++;
    }
    const copy: FieldDefinition = {
      id: crypto.randomUUID(),
      name: copyName,
      type: field.type,
    };
    const idx = fields.findIndex((f) => f.id === field.id);
    const next = [...fields];
    next.splice(idx + 1, 0, copy);
    onChange(next);
    onSelectField(copy.id);
  };

  const handleRemove = (id: string) => {
    if (onRemoveOverride) {
      onRemoveOverride(id);
      return;
    }
    const next = fields.filter((f) => f.id !== id);
    onChange(next);
    if (selectedFieldId === id) {
      onSelectField(next.length > 0 ? next[0].id : null);
    }
  };

  const handleRenameField = (id: string, newName: string) => {
    onChange(fields.map((f) => (f.id === id ? { ...f, name: newName } : f)));
  };

  const handleTypeChange = (id: string, item: SelectorItem | SelectorItem[] | undefined) => {
    if (!item || Array.isArray(item)) return;
    onChange(fields.map((f) => (f.id === id ? { ...f, type: item.value as "string" | "number" } : f)));
  };

  const handleDragEnd = (event: DragEndEvent) => {
    const { active, over } = event;
    if (over && active.id !== over.id) {
      const oldIndex = fields.findIndex((f) => f.id === active.id);
      const newIndex = fields.findIndex((f) => f.id === over.id);
      const reordered = [...fields];
      const [removed] = reordered.splice(oldIndex, 1);
      reordered.splice(newIndex, 0, removed);
      onChange(reordered);
    }
  };

  // Empty state — no fields defined yet
  if (!hasFields) {
    return (
      <Stack
        sx={{ height: 340 }}
        alignItems="center"
        justifyContent="center"
        spacing={2}>
        <Icon
          iconName={ICON_NAME.TABLE}
          fontSize="small"
          htmlColor={theme.palette.text.secondary}
        />
        <Typography variant="body2" color="text.secondary">
          {t("no_fields")}
        </Typography>
        <Button
          variant="text"
          size="small"
          startIcon={<Icon iconName={ICON_NAME.PLUS} style={{ fontSize: 11 }} />}
          onClick={handleAddField}
          sx={{ textTransform: "none" }}>
          {t("add_field")}
        </Button>
      </Stack>
    );
  }

  return (
    <Stack direction="row" sx={{ height: 340 }} spacing={0}>
      {/* Left column — field list */}
      <Box
        sx={{
          width: "55%",
          borderRight: `1px solid ${theme.palette.divider}`,
          display: "flex",
          flexDirection: "column",
          overflow: "hidden",
        }}>
        <Box sx={{ flexGrow: 1, overflowY: "auto" }}>
          <DndContext
            collisionDetection={closestCenter}
            modifiers={[restrictToVerticalAxis]}
            onDragEnd={handleDragEnd}>
            <SortableContext
              items={fields.map((f) => f.id)}
              strategy={verticalListSortingStrategy}>
              {fields.map((field) => (
                <SortableFieldRow
                  key={field.id}
                  field={field}
                  isSelected={field.id === selectedFieldId}
                  error={fieldErrors[field.id]}
                  warning={fieldWarnings[field.id]}
                  onSelect={() => onSelectField(field.id)}
                  onRename={(newName) => handleRenameField(field.id, newName)}
                  onDuplicate={() => handleDuplicate(field)}
                  onRemove={() => handleRemove(field.id)}
                />
              ))}
            </SortableContext>
          </DndContext>
        </Box>

        {/* Add field button */}
        <Box
          sx={{
            px: 1,
            py: 1.5,
            borderTop: `1px solid ${theme.palette.divider}`,
          }}>
          <Button
            variant="text"
            size="small"
            startIcon={<Icon iconName={ICON_NAME.PLUS} style={{ fontSize: 11 }} />}
            onClick={handleAddField}
            sx={{ textTransform: "none" }}>
            {t("add_field")}
          </Button>
        </Box>
      </Box>

      {/* Right column — settings */}
      <Box
        sx={{
          width: "45%",
          p: 2,
          display: "flex",
          flexDirection: "column",
        }}>
        {selectedField ? (
          <Stack spacing={2}>
            <Typography variant="subtitle2" fontWeight="bold">
              {selectedField.name}
            </Typography>
            <Selector
              label={t("field_type")}
              selectedItems={fieldTypeItems.find((i) => i.value === selectedField.type)}
              setSelectedItems={(item) => handleTypeChange(selectedField.id, item)}
              items={fieldTypeItems}
              disabled={lockedFieldIds?.has(selectedField.id)}
            />
          </Stack>
        ) : (
          <Box
            sx={{
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              height: "100%",
            }}>
            <Typography variant="body2" color="text.secondary">
              {t("select_field_to_edit")}
            </Typography>
          </Box>
        )}
      </Box>
    </Stack>
  );
};

export default FieldEditor;
