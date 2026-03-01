"use client";

import { LoadingButton } from "@mui/lab";
import {
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  Stack,
  TextField,
  Typography,
} from "@mui/material";
import React, { useCallback, useState } from "react";
import { useTranslation } from "react-i18next";

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
    <Dialog open={open} onClose={onClose} fullWidth maxWidth="sm">
      <DialogTitle>{t("save_dataset")}</DialogTitle>
      <DialogContent>
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
      </DialogContent>
      <DialogActions
        disableSpacing
        sx={{
          pb: 2,
        }}>
        <Button onClick={onClose} disabled={isSaving} variant="text" sx={{ borderRadius: 0 }}>
          <Typography variant="body2" fontWeight="bold">
            {t("cancel")}
          </Typography>
        </Button>
        <LoadingButton
          onClick={handleSave}
          loading={isSaving}
          variant="text"
          color="primary"
          disabled={!name.trim()}
          sx={{ borderRadius: 0 }}>
          <Typography variant="body2" fontWeight="bold" color="inherit">
            {t("save")}
          </Typography>
        </LoadingButton>
      </DialogActions>
    </Dialog>
  );
};

export default SaveDatasetDialog;
