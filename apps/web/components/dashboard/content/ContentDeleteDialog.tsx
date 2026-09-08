"use client";

import { DialogContentText } from "@mui/material";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { toast } from "react-toastify";
import { mutate } from "swr";

import { ICON_NAME } from "@p4b/ui/components/Icon";

import { deleteBundle } from "@/lib/api/bundles";
import { refreshContentFeed } from "@/lib/api/content";
import { matchesContentListKey } from "@/lib/api/datasets";
import { deleteFolder } from "@/lib/api/folders";
import { deleteLayer } from "@/lib/api/layers";
import { deleteProject } from "@/lib/api/projects";
import { deleteTemplate, refreshTemplates } from "@/lib/api/templates";
import type { ContentItem, ContentType } from "@/lib/validations/content";

import AppDialog, { AppDialogFooter } from "@/components/common/AppDialog";

interface ContentDeleteDialogProps {
  items: ContentItem[];
  onClose: () => void;
  onDeleted: () => void;
}

const DELETE_BY_TYPE: Record<ContentType, (id: string) => Promise<unknown>> = {
  folder: deleteFolder,
  project: deleteProject,
  layer: deleteLayer,
  bundle: deleteBundle,
  template: deleteTemplate,
};

/**
 * One confirmation for deleting 1..n items of mixed type — a kebab's single
 * item or the action bar's whole selection. Deletes run sequentially (not
 * `Promise.all`) so one item's failure never leaves another undeleted item
 * skipped; every item is attempted regardless of an earlier failure, and the
 * toast reports whether all of them made it.
 */
const ContentDeleteDialog = ({ items, onClose, onDeleted }: ContentDeleteDialogProps) => {
  const { t } = useTranslation("common");
  const [isBusy, setIsBusy] = useState(false);

  const handleDelete = async () => {
    setIsBusy(true);
    const failures: ContentItem[] = [];
    for (const item of items) {
      try {
        await DELETE_BY_TYPE[item.type](item.id);
      } catch {
        failures.push(item);
      }
    }

    refreshContentFeed();
    mutate(matchesContentListKey);
    if (items.some((item) => item.type === "template")) refreshTemplates();
    setIsBusy(false);

    if (failures.length === 0) {
      toast.success(t("deleted_moved_to_trash"));
      onDeleted();
    } else {
      toast.error(t("delete_partial_failed", { count: failures.length }));
    }
    onClose();
  };

  return (
    <AppDialog
      open
      onClose={onClose}
      icon={ICON_NAME.TRASH}
      tone="warning"
      title={t("delete_items_title", { count: items.length })}
      closeDisabled={isBusy}
      footer={
        <AppDialogFooter
          onCancel={onClose}
          cancelDisabled={isBusy}
          primaryLabel={t("delete")}
          onPrimary={() => void handleDelete()}
          primaryColor="error"
          primaryLoading={isBusy}
        />
      }>
      <DialogContentText>{t("delete_items_confirm")}</DialogContentText>
    </AppDialog>
  );
};

export default ContentDeleteDialog;
