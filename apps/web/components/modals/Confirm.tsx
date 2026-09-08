import { DialogContentText, TextField } from "@mui/material";
import { useState } from "react";

import { ICON_NAME } from "@p4b/ui/components/Icon";

import AppDialog, { AppDialogFooter } from "@/components/common/AppDialog";

interface ConfirmDialogProps {
  title: string;
  body;
  open: boolean;
  closeText?: string;
  onClose?: () => void;
  confirmText?: string;
  onConfirm?: () => void;
  matchText?: string;
}

const ConfirmModal: React.FC<ConfirmDialogProps> = ({
  open,
  title,
  body,
  closeText,
  onClose,
  confirmText,
  onConfirm,
  matchText,
}) => {
  const [matchTextValue, setMatchTextValue] = useState("");

  const close = () => {
    if (matchText) {
      setMatchTextValue("");
    }
    onClose?.();
  };

  return (
    <AppDialog
      open={open}
      onClose={close}
      // Every confirmation this dialog draws ends in a destructive button, so
      // it always wears the amber tile.
      icon={ICON_NAME.CIRCLEINFO}
      tone="warning"
      title={title}
      closeLabel={closeText || "Close"}
      footer={
        <AppDialogFooter
          cancelLabel={closeText || "Close"}
          onCancel={onClose}
          primaryLabel={confirmText || "Confirm"}
          onPrimary={() => onConfirm?.()}
          primaryColor="error"
          primaryDisabled={!!matchText && matchTextValue !== matchText}
        />
      }>
      <DialogContentText>{body}</DialogContentText>
      {matchText && (
        <TextField
          required
          fullWidth
          placeholder={matchText}
          id="matchText"
          onChange={(e) => setMatchTextValue(e.target.value)}
          value={matchTextValue}
          sx={{ my: 4 }}
        />
      )}
    </AppDialog>
  );
};

export default ConfirmModal;
