import {
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  Typography,
  useTheme,
} from "@mui/material";
import { useCallback, useEffect, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { toast } from "react-toastify";

import { COLLECTIONS_API_BASE_URL, addColumn, deleteColumn, renameColumn, useLayerQueryables } from "@/lib/api/layers";
import type { FieldDefinition } from "@/lib/validations/layer";
import { mutate as globalMutate } from "swr";

import FieldEditor from "@/components/common/FieldEditor";
import ConfirmModal from "@/components/modals/Confirm";

interface EditFieldsModalProps {
  open: boolean;
  onClose: () => void;
  layerId: string;
  /** If provided, this field will be pre-selected on open */
  initialFieldName?: string | null;
}

/** Type mapping from queryables schema types to our FieldDefinition types */
const mapQueryableType = (type: string): "string" | "number" => {
  if (type === "number" || type === "integer") return "number";
  return "string";
};

/** Type mapping from our FieldDefinition types to GeoAPI column types */
const mapToColumnType = (type: "string" | "number"): string => {
  if (type === "number") return "number";
  return "string";
};

/** Hidden system fields that should not appear in the editor */
const HIDDEN_FIELDS = ["layer_id", "id", "h3_3", "h3_6", "geom", "geometry"];

const EditFieldsModal: React.FC<EditFieldsModalProps> = ({
  open,
  onClose,
  layerId,
  initialFieldName,
}) => {
  const { t } = useTranslation("common");
  const theme = useTheme();
  const { queryables, mutate: mutateQueryables } = useLayerQueryables(layerId);

  const [fields, setFields] = useState<FieldDefinition[]>([]);
  const [selectedFieldId, setSelectedFieldId] = useState<string | null>(null);
  const [isSaving, setIsSaving] = useState(false);

  // Delete confirmation
  const [deleteConfirmOpen, setDeleteConfirmOpen] = useState(false);
  const [pendingDeleteId, setPendingDeleteId] = useState<string | null>(null);

  // Track original field state for diffing on save
  const [originalFields, setOriginalFields] = useState<FieldDefinition[]>([]);

  // Load fields from queryables when modal opens
  useEffect(() => {
    if (!open || !queryables?.properties) return;

    const loaded: FieldDefinition[] = Object.entries(queryables.properties)
      .filter(([name]) => !HIDDEN_FIELDS.includes(name))
      .filter(([, schema]) => {
        const t = (schema as { type?: string }).type;
        return t !== "object" && t !== "geometry";
      })
      .map(([name, schema]) => ({
        id: name, // Use the column name as the stable ID for existing fields
        name,
        type: mapQueryableType((schema as { type?: string }).type || "string"),
      }));

    setFields(loaded);
    setOriginalFields(loaded);

    // Pre-select the requested field
    if (initialFieldName) {
      const match = loaded.find((f) => f.name === initialFieldName);
      if (match) setSelectedFieldId(match.id);
    }
  }, [open, queryables, initialFieldName]);

  /** Revalidate all SWR keys for this layer (queryables, collection items, etc.) */
  const revalidateLayer = useCallback(() => {
    mutateQueryables();
    // Revalidate any SWR key that starts with the collection URL for this layer
    globalMutate(
      (key) => typeof key === "string" && key.startsWith(`${COLLECTIONS_API_BASE_URL}/${layerId}`),
      undefined,
      { revalidate: true },
    );
  }, [layerId, mutateQueryables]);

  // Build a map of original field names by ID for diffing
  const originalMap = useMemo(
    () => new Map(originalFields.map((f) => [f.id, f])),
    [originalFields]
  );

  // Existing fields whose type cannot be changed
  const lockedFieldIds = useMemo(
    () => new Set(originalFields.map((f) => f.id)),
    [originalFields]
  );

  const handleFieldsChange = useCallback((updated: FieldDefinition[]) => {
    setFields(updated);
  }, []);

  const handleRemoveRequest = useCallback((id: string) => {
    // For existing fields (id matches an original), show confirmation
    const isExisting = originalFields.some((f) => f.id === id);
    if (isExisting) {
      setPendingDeleteId(id);
      setDeleteConfirmOpen(true);
    } else {
      // New unsaved field — remove immediately
      setFields((prev) => prev.filter((f) => f.id !== id));
      setSelectedFieldId((prev) => (prev === id ? null : prev));
    }
  }, [originalFields]);

  const handleConfirmDelete = useCallback(async () => {
    if (!pendingDeleteId) return;
    const field = fields.find((f) => f.id === pendingDeleteId);
    if (!field) return;

    try {
      setIsSaving(true);
      await deleteColumn(layerId, field.name);
      setFields((prev) => prev.filter((f) => f.id !== pendingDeleteId));
      setOriginalFields((prev) => prev.filter((f) => f.id !== pendingDeleteId));
      if (selectedFieldId === pendingDeleteId) setSelectedFieldId(null);
      toast.success(t("field_deleted"));
      revalidateLayer();
    } catch (error) {
      console.error("Failed to delete field:", error);
      toast.error(t("error_deleting_field"));
    } finally {
      setIsSaving(false);
      setDeleteConfirmOpen(false);
      setPendingDeleteId(null);
    }
  }, [pendingDeleteId, fields, layerId, selectedFieldId, t, revalidateLayer]);

  const handleSave = async () => {
    try {
      setIsSaving(true);

      // Diff: find added, renamed, and type-changed fields
      const originalIds = new Set(originalFields.map((f) => f.id));

      for (const field of fields) {
        if (!originalIds.has(field.id)) {
          // New field — add column
          await addColumn(layerId, field.name, mapToColumnType(field.type));
        } else {
          const orig = originalMap.get(field.id);
          if (orig && orig.name !== field.name) {
            // Renamed field
            await renameColumn(layerId, orig.name, field.name);
          }
          // Note: type changes on existing columns are not supported by GeoAPI
        }
      }

      toast.success(t("fields_saved"));
      revalidateLayer();
      onClose();
    } catch (error) {
      console.error("Failed to save fields:", error);
      toast.error(t("error_saving_fields"));
    } finally {
      setIsSaving(false);
    }
  };

  // Check if there are unsaved changes
  const hasChanges = useMemo(() => {
    if (fields.length !== originalFields.length) return true;
    return fields.some((f) => {
      const orig = originalMap.get(f.id);
      if (!orig) return true; // new field
      return orig.name !== f.name;
    });
  }, [fields, originalFields, originalMap]);

  const pendingDeleteField = fields.find((f) => f.id === pendingDeleteId);

  return (
    <>
      <Dialog open={open} onClose={onClose} fullWidth maxWidth="md">
        <DialogTitle>{t("edit_fields")}</DialogTitle>
        <DialogContent sx={{ p: 0 }}>
          <FieldEditor
            fields={fields}
            onChange={handleFieldsChange}
            selectedFieldId={selectedFieldId}
            onSelectField={setSelectedFieldId}
            onRemoveOverride={handleRemoveRequest}
            lockedFieldIds={lockedFieldIds}
          />
        </DialogContent>
        <DialogActions
          sx={{
            "&.MuiDialogActions-root": {
              px: 3,
              py: 2,
              borderTop: `1px solid ${theme.palette.divider}`,
            },
            justifyContent: "flex-end",
          }}>
          <Button variant="text" onClick={onClose}>
            <Typography variant="body2" fontWeight="bold">
              {t("cancel")}
            </Typography>
          </Button>
          <Button
            variant="contained"
            color="primary"
            onClick={handleSave}
            disabled={!hasChanges || isSaving}>
            <Typography variant="body2" fontWeight="bold" color="inherit">
              {t("save")}
            </Typography>
          </Button>
        </DialogActions>
      </Dialog>

      <ConfirmModal
        open={deleteConfirmOpen}
        title={t("delete_field")}
        body={t("delete_field_confirmation", { name: pendingDeleteField?.name })}
        closeText={t("cancel")}
        confirmText={t("delete")}
        onClose={() => {
          setDeleteConfirmOpen(false);
          setPendingDeleteId(null);
        }}
        onConfirm={handleConfirmDelete}
      />
    </>
  );
};

export default EditFieldsModal;
