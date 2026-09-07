"use client";

import { Stack, TextField, Typography } from "@mui/material";
import React, { useCallback, useState } from "react";
import { useTranslation } from "react-i18next";

import { ICON_NAME } from "@p4b/ui/components/Icon";

import AppDialog, { AppDialogFooter } from "@/components/common/AppDialog";

interface SaveDatasetDialogProps {
  open: boolean;
  onClose: () => void;
  onSave: (name: string) => Promise<void>;
  defaultName?: string;
  isSaving?: boolean;
}

const SaveDatasetDialog: React.FC<SaveDatasetDialogProps> = ({
  open,
  onClose,
  onSave,
  defaultName = "",
  isSaving = false,
}) => {
  const { t } = useTranslation("common");
  const [name, setName] = useState(defaultName);
  const [error, setError] = useState<string | null>(null);

  // Reset name when dialog opens
  React.useEffect(() => {
    if (open) {
      setName(defaultName);
      setError(null);
    }
  }, [open, defaultName]);

  const handleSave = useCallback(async () => {
    const trimmedName = name.trim();
    if (!trimmedName) {
      setError(t("dataset_name_required"));
      return;
    }

    try {
      await onSave(trimmedName);
      onClose();
    } catch (err) {
      setError(err instanceof Error ? err.message : t("save_failed"));
    }
  }, [name, onSave, onClose, t]);

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      if (e.key === "Enter" && !isSaving) {
        handleSave();
      }
    },
    [handleSave, isSaving]
  );

  return (
    <AppDialog
      open={open}
      onClose={onClose}
      icon={ICON_NAME.SAVE}
      title={t("save_dataset")}
      maxWidth={600}
      closeDisabled={isSaving}
      footer={
        <AppDialogFooter
          onCancel={onClose}
          cancelDisabled={isSaving}
          primaryLabel={t("save")}
          onPrimary={() => void handleSave()}
          primaryDisabled={!name.trim()}
          primaryLoading={isSaving}
        />
      }>
      <Stack sx={{ pt: 1 }}>
        <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
          {t("save_dataset_description")}
        </Typography>
        <TextField
          autoFocus
          fullWidth
          size="small"
          inputProps={{
            style: {
              fontSize: "0.875rem",
              fontWeight: "bold",
            },
          }}
          placeholder={t("dataset_name")}
          value={name}
          onChange={(e) => {
            setName(e.target.value);
            setError(null);
          }}
          onKeyDown={handleKeyDown}
          error={!!error}
          helperText={error}
          disabled={isSaving}
        />
      </Stack>
    </AppDialog>
  );
};

export default SaveDatasetDialog;
