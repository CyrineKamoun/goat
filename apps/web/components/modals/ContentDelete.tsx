import { DialogContentText } from "@mui/material";
import { useTranslation } from "react-i18next";
import { Trans } from "react-i18next";
import { toast } from "react-toastify";
import { mutate } from "swr";

import { ICON_NAME } from "@p4b/ui/components/Icon";

import { deleteBundle, isBundleTile } from "@/lib/api/bundles";
import { matchesContentListKey } from "@/lib/api/datasets";
import { deleteLayer } from "@/lib/api/layers";
import { PROJECTS_API_BASE_URL, deleteProject } from "@/lib/api/projects";
import type { Layer } from "@/lib/validations/layer";

import type { ContentDialogBaseProps } from "@/types/dashboard/content";

import AppDialog, { AppDialogFooter } from "@/components/common/AppDialog";

interface ContentDeleteDialogProps extends ContentDialogBaseProps {
  disabled?: boolean;
  onDelete?: () => void;
}

const ContentDeleteModal: React.FC<ContentDeleteDialogProps> = ({
  open,
  disabled,
  onClose,
  onDelete,
  type,
  content,
}) => {
  const { t } = useTranslation("common");

  const isBundle = isBundleTile(content);

  const handleDelete = async () => {
    try {
      if (!content) return;
      if (isBundle) {
        // Bundle: one call removes the bundle + all its member layers
        // (backend cascades DuckLake cleanup). Refresh the bundles list.
        await deleteBundle(content.id);
        mutate(matchesContentListKey);
        toast.success(t("delete_bundle_success"));
      } else if (type === "layer") {
        // Optimistic update: immediately remove layer from cache
        mutate(
          matchesContentListKey,
          (currentData: { items: Layer[]; total: number; pages: number } | undefined) => {
            if (!currentData?.items) return currentData;
            return {
              ...currentData,
              items: currentData.items.filter((item) => item.id !== content.id),
              total: Math.max(0, currentData.total - 1),
            };
          },
          { revalidate: false }
        );

        await deleteLayer(content.id);
        toast.success(t("delete_layer_success"));
      } else if (type === "project") {
        await deleteProject(content?.id);
        mutate((key) => Array.isArray(key) && key[0] === PROJECTS_API_BASE_URL);
        toast.success(t("delete_project_success"));
      }
    } catch {
      // Revert optimistic update on error by revalidating
      if (isBundle || type === "layer") {
        mutate(matchesContentListKey);
      }
      toast.error(
        isBundle
          ? t("delete_bundle_error")
          : type === "layer"
            ? t("delete_layer_error")
            : t("delete_project_error")
      );
    }

    onDelete?.();
  };

  return (
    <AppDialog
      open={open}
      onClose={() => onClose?.()}
      icon={ICON_NAME.TRASH}
      tone="warning"
      title={isBundle ? t("delete_bundle") : type === "layer" ? t("delete_layer") : t("delete_project")}
      footer={
        <AppDialogFooter
          onCancel={onClose}
          primaryLabel={t("delete")}
          onPrimary={() => void handleDelete()}
          primaryColor="error"
          primaryDisabled={disabled}
        />
      }>
      <DialogContentText>
        {isBundle ? (
          <Trans
            i18nKey="common:are_you_sure_to_delete_bundle"
            values={{ name: content?.name }}
            components={{ b: <b /> }}
          />
        ) : type === "layer" ? (
          <Trans
            i18nKey="common:are_you_sure_to_delete_layer"
            values={{ layer: content?.name }}
            components={{ b: <b /> }}
          />
        ) : (
          <Trans
            i18nKey="common:are_you_sure_to_delete_project"
            values={{ project: content?.name }}
            components={{ b: <b /> }}
          />
        )}
      </DialogContentText>
    </AppDialog>
  );
};

export default ContentDeleteModal;
