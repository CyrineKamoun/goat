import { Box, Stack, Typography } from "@mui/material";
import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";

import { ICON_NAME } from "@p4b/ui/components/Icon";

import type { PopupPlacement, PopupSize, PopupType } from "@/lib/validations/widget";

import MarkdownContentEditor from "@/components/builder/widgets/common/MarkdownContentEditor";
import PopupSettingsControls from "@/components/builder/widgets/common/PopupSettingsControls";
import AppDialog, { AppDialogFooter } from "@/components/common/AppDialog";

export interface LinkPopupValues {
  popup_content?: string;
  popup_type?: PopupType;
  popup_placement?: PopupPlacement;
  popup_size?: PopupSize;
}

interface LinkPopupEditDialogProps {
  open: boolean;
  onClose: () => void;
  onSave: (values: LinkPopupValues) => void;
  initial: LinkPopupValues;
  /** Used as the dialog header context, e.g. the link's label. */
  contextLabel?: string;
}

/**
 * Per-link popup edit dialog. Visually identical to InfoChipEditDialog —
 * shares the same dialog frame, controls, and content editor.
 */
const LinkPopupEditDialog = ({
  open,
  onClose,
  onSave,
  initial,
  contextLabel: _contextLabel,
}: LinkPopupEditDialogProps) => {
  const { t } = useTranslation("common");
  const [popupType, setPopupType] = useState<PopupType>("popover");
  const [placement, setPlacement] = useState<PopupPlacement>("auto");
  const [size, setSize] = useState<PopupSize>("md");
  const [content, setContent] = useState("");

  useEffect(() => {
    if (!open) return;
    setPopupType(initial.popup_type ?? "popover");
    setPlacement(initial.popup_placement ?? "auto");
    setSize(initial.popup_size ?? "md");
    setContent(initial.popup_content ?? "");
    // Snapshot when the dialog opens; ignore prop churn while editing.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open]);

  const handleClose = () => {
    onSave({
      popup_type: popupType,
      popup_placement: placement,
      popup_size: size,
      popup_content: content,
    });
    onClose();
  };

  return (
    <AppDialog
      open={open}
      onClose={handleClose}
      icon={ICON_NAME.LINK}
      title={t("edit_popup_content")}
      maxWidth={600}
      bodySx={{ pt: 2 }}
      footer={<AppDialogFooter primaryLabel={t("done", { defaultValue: "Done" })} onPrimary={handleClose} />}>
      <Stack spacing={2}>
        <PopupSettingsControls
          popupType={popupType}
          placement={placement}
          size={size}
          onPopupTypeChange={setPopupType}
          onPlacementChange={setPlacement}
          onSizeChange={setSize}
        />

        <Box>
          <Typography
            variant="caption"
            color="text.secondary"
            fontWeight={600}
            sx={{ mb: 0.5, display: "block" }}>
            {t("info_text", { defaultValue: "Info text" })}
          </Typography>
          <MarkdownContentEditor value={content} onChange={setContent} plainText={popupType === "tooltip"} />
        </Box>
      </Stack>
    </AppDialog>
  );
};

export default LinkPopupEditDialog;
