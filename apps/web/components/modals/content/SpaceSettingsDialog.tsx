"use client";

import { FormControlLabel, Radio, RadioGroup, Typography } from "@mui/material";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { toast } from "react-toastify";

import { ICON_NAME } from "@p4b/ui/components/Icon";

import { updateSpaceDefaultRole, useSpaces } from "@/lib/api/content";
import type { Space, SpaceDefaultRole } from "@/lib/validations/content";

import AppDialog, { AppDialogFooter } from "@/components/common/AppDialog";

interface SpaceSettingsDialogProps {
  space: Space;
  onClose: () => void;
  /** Called once the space's `default_role` has actually been saved, after
   * the spaces list has been re-fetched — mirrors `MoveDialog`'s `onMoved`. */
  onSaved?: () => void;
}

/**
 * Team/organization space settings: the one thing an owner can configure
 * today is the default role new members get on the space's content. Only
 * rendered for a space the caller owns that isn't the personal space — the
 * page gates on both before opening this.
 */
const SpaceSettingsDialog = ({ space, onClose, onSaved }: SpaceSettingsDialogProps) => {
  const { t } = useTranslation("common");

  const { mutate } = useSpaces();
  const [role, setRole] = useState<SpaceDefaultRole>(space.default_role);
  const [saving, setSaving] = useState(false);

  const handleSave = async () => {
    setSaving(true);
    try {
      await updateSpaceDefaultRole(space.id, role);
      await mutate();
      toast.success(t("space_settings_saved"));
      onSaved?.();
      onClose();
    } catch {
      toast.error(t("error_updating_space"));
    } finally {
      setSaving(false);
    }
  };

  return (
    <AppDialog
      open
      onClose={onClose}
      icon={ICON_NAME.SETTINGS}
      title={t("space_settings")}
      maxWidth={444}
      fullScreenBelow="md"
      footer={
        <AppDialogFooter
          onCancel={onClose}
          primaryLabel={t("save")}
          onPrimary={() => void handleSave()}
          primaryLoading={saving}
          primaryDisabled={saving || role === space.default_role}
        />
      }>
      <Typography variant="subtitle2" sx={{ mb: 1 }}>
        {t("default_content_role")}
      </Typography>
      <RadioGroup value={role} onChange={(_event, value) => setRole(value as SpaceDefaultRole)}>
        <FormControlLabel value="editor" control={<Radio />} label={t("editor")} />
        <Typography variant="caption" color="text.secondary" sx={{ ml: 4, mt: -1, mb: 1 }}>
          {t("space_default_editor_hint")}
        </Typography>
        <FormControlLabel value="viewer" control={<Radio />} label={t("viewer")} />
        <Typography variant="caption" color="text.secondary" sx={{ ml: 4, mt: -1 }}>
          {t("space_default_viewer_hint")}
        </Typography>
      </RadioGroup>
    </AppDialog>
  );
};

export default SpaceSettingsDialog;
