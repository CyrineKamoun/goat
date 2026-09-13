"use client";

import { Box, FormControlLabel, Switch, Typography, useTheme } from "@mui/material";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { toast } from "react-toastify";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import { refreshContentFeed } from "@/lib/api/content";
import { updateDataset } from "@/lib/api/layers";
import type { ContentItem } from "@/lib/validations/content";

export interface ShareDatasetPublicTabProps {
  item: ContentItem;
  /** Only the owner may open a dataset to everyone — the share right. */
  canToggle: boolean;
}

/** The Share dialog's Public tab for a dataset: one switch that sets
 * `layer.public_read`. A public dataset is readable by every signed-in GOAT
 * user in any organization; it is not listed anywhere, people reach it
 * through the projects and templates that include it. Unlike a published
 * project there is no snapshot: what the owner edits is what everyone sees. */
const ShareDatasetPublicTab = ({ item, canToggle }: ShareDatasetPublicTabProps) => {
  const { t } = useTranslation("common");
  const theme = useTheme();
  const [isPublic, setIsPublic] = useState(item.is_public);
  const [saving, setSaving] = useState(false);

  const handleToggle = async (_event: unknown, next: boolean) => {
    setSaving(true);
    setIsPublic(next);
    try {
      await updateDataset(item.id, { public_read: next });
      refreshContentFeed();
      toast.success(t(next ? "dataset_now_public" : "dataset_now_private"));
    } catch {
      setIsPublic(!next);
      toast.error(t("error_updating_dataset"));
    } finally {
      setSaving(false);
    }
  };

  return (
    <Box sx={{ display: "grid", gap: "10px", pt: 1 }}>
      <FormControlLabel
        control={
          <Switch
            size="small"
            checked={isPublic}
            disabled={!canToggle || saving}
            onChange={handleToggle}
            inputProps={{ "aria-label": t("public_dataset_switch") }}
          />
        }
        label={
          <Typography sx={{ fontSize: 13, fontWeight: 700 }}>{t("public_dataset_switch")}</Typography>
        }
      />
      <Box sx={{ display: "flex", alignItems: "flex-start", gap: "9px" }}>
        <Icon
          iconName={ICON_NAME.GLOBE}
          style={{ fontSize: 13, marginTop: 2, color: theme.palette.text.secondary }}
        />
        <Typography sx={{ fontSize: 12.5, color: "text.secondary", lineHeight: 1.45 }}>
          {t("public_dataset_explainer")}
        </Typography>
      </Box>
    </Box>
  );
};

export default ShareDatasetPublicTab;
