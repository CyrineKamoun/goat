import { Alert, Checkbox, DialogContentText, FormControlLabel, Stack, Typography } from "@mui/material";
import { useParams } from "next/navigation";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { Trans } from "react-i18next";
import { toast } from "react-toastify";
import { mutate } from "swr";

import { ICON_NAME } from "@p4b/ui/components/Icon";

import { matchesContentListKey } from "@/lib/api/datasets";
import { deleteLayer, useDataset } from "@/lib/api/layers";
import { deleteProjectLayer, useProjectLayers } from "@/lib/api/projects";
import type { ProjectLayer } from "@/lib/validations/project";

import AppDialog, { AppDialogFooter } from "@/components/common/AppDialog";

interface ProjectLayerDeleteDialogProps {
  open: boolean;
  projectLayer: ProjectLayer;
  onDelete?: () => void;
  onClose?: () => void;
}

const ProjectLayerDeleteModal: React.FC<ProjectLayerDeleteDialogProps> = ({
  open,
  projectLayer,
  onClose,
  onDelete,
}) => {
  const { t } = useTranslation("common");
  const { projectId } = useParams() as { projectId: string };
  const [isLoading, setIsLoading] = useState(false);
  const { dataset } = useDataset(projectLayer?.layer_id);
  const { mutate: mutateProjectLayers } = useProjectLayers(projectId);
  const [deleteSourceLayer, setDeleteSourceLayer] = useState(false);

  async function handleDelete() {
    try {
      setIsLoading(true);
      if (!projectLayer) return;

      // Always remove the layer from the project immediately
      await deleteProjectLayer(projectId, projectLayer.id);
      mutateProjectLayers();

      // If the user also wants to delete the dataset, soft-delete it too
      if (deleteSourceLayer && dataset) {
        await deleteLayer(dataset.id);
        toast.success(t("delete_layer_success"));
        // Invalidate dataset layers cache
        mutate(matchesContentListKey);
      }

      onDelete?.();
    } catch (error) {
      toast.error(t("error_removing_layer_from_project"));
    } finally {
      setIsLoading(false);
    }
  }

  return (
    <AppDialog
      open={open}
      onClose={() => onClose?.()}
      icon={ICON_NAME.TRASH}
      tone="warning"
      title={t("delete_project_layer")}
      footer={
        <AppDialogFooter
          onCancel={onClose}
          primaryLabel={t("delete")}
          onPrimary={() => void handleDelete()}
          primaryColor="error"
          primaryLoading={isLoading}
        />
      }>
      <DialogContentText>
        <Trans
          i18nKey="common:are_you_sure_to_delete_layer"
          values={{ layer: projectLayer?.name }}
          components={{ b: <b /> }}
        />
      </DialogContentText>
      {!projectLayer.in_catalog && (
        <Stack sx={{ mt: 2 }}>
          <FormControlLabel
            control={
              <Checkbox
                size="small"
                color="warning"
                checked={deleteSourceLayer}
                onChange={(e) => {
                  setDeleteSourceLayer(e.target.checked);
                }}
              />
            }
            label={
              <Typography variant="body2" fontWeight="bold">
                {t("delete_dataset_source")}
              </Typography>
            }
          />
          {deleteSourceLayer && dataset && (
            <Alert severity="warning" sx={{ mt: 2 }}>
              <Trans
                i18nKey="common:dataset_delete_warning"
                values={{ datasetName: dataset?.name }}
                components={{ b: <b /> }}
              />
            </Alert>
          )}
        </Stack>
      )}
    </AppDialog>
  );
};

export default ProjectLayerDeleteModal;
