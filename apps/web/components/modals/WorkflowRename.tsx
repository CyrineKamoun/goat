"use client";

import { Stack, TextField } from "@mui/material";
import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";

import { ICON_NAME } from "@p4b/ui/components/Icon";

import AppDialog, { AppDialogFooter } from "@/components/common/AppDialog";

interface WorkflowRenameDialogProps {
  open: boolean;
  workflowName: string;
  onRename?: (newName: string) => Promise<void>;
  onClose?: () => void;
}

const WorkflowRenameModal: React.FC<WorkflowRenameDialogProps> = ({
  open,
  workflowName,
  onClose,
  onRename,
}) => {
  const { t } = useTranslation("common");
  const [isLoading, setIsLoading] = useState(false);
  const [name, setName] = useState(workflowName);

  // Reset name when modal opens with a new workflow
  useEffect(() => {
    setName(workflowName);
  }, [workflowName, open]);

  async function handleRename() {
    try {
      setIsLoading(true);
      await onRename?.(name);
      onClose?.();
    } catch (error) {
      console.error("Failed to rename workflow:", error);
    } finally {
      setIsLoading(false);
    }
  }

  return (
    <AppDialog
      open={open}
      onClose={() => onClose?.()}
      icon={ICON_NAME.EDITPEN}
      title={t("rename_workflow")}
      footer={
        <AppDialogFooter
          onCancel={onClose}
          primaryLabel={t("rename")}
          onPrimary={() => void handleRename()}
          primaryDisabled={!name.trim()}
          primaryLoading={isLoading}
        />
      }>
      <Stack sx={{ pt: 1 }}>
        <TextField
          autoFocus
          size="small"
          fullWidth
          inputProps={{
            style: {
              fontSize: "0.875rem",
              fontWeight: "bold",
            },
          }}
          value={name}
          onChange={(e) => {
            setName(e.target.value);
          }}
        />
      </Stack>
    </AppDialog>
  );
};

export default WorkflowRenameModal;
