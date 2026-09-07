import { Box, Stack } from "@mui/material";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { toast } from "react-toastify";
import { mutate } from "swr";

import { ICON_NAME } from "@p4b/ui/components/Icon";

import { isBundleTile, updateBundle } from "@/lib/api/bundles";
import { useSpaces } from "@/lib/api/content";
import { matchesContentListKey } from "@/lib/api/datasets";
import { getWritableFolders, useFolders } from "@/lib/api/folders";
import { updateDataset } from "@/lib/api/layers";
import { PROJECTS_API_BASE_URL, updateProject } from "@/lib/api/projects";
import { homeFolderOf } from "@/lib/utils/content";
import type { GetContentQueryParams } from "@/lib/validations/common";
import type { Layer, PostDataset } from "@/lib/validations/layer";
import type { PostProject, Project } from "@/lib/validations/project";

import AppDialog, { AppDialogFooter } from "@/components/common/AppDialog";
import FolderBrowser from "@/components/dashboard/common/FolderBrowser";

interface ContentMoveToFolderDialogProps {
  open: boolean;
  onClose?: () => void;
  disabled?: boolean;
  type: "project" | "layer";
  onContentMove?: () => void;
  content: Layer | Project;
}

const ContentMoveToFolderModal: React.FC<ContentMoveToFolderDialogProps> = ({
  open,
  onClose,
  disabled,
  type,
  onContentMove,
  content,
}) => {
  const { t } = useTranslation("common");
  const queryParams: GetContentQueryParams = {
    order: "descendent",
    order_by: "updated_at",
  };
  const { folders: allFolders } = useFolders(queryParams);
  const folders = getWritableFolders(allFolders);
  const { spaces } = useSpaces();

  const [isBusy, setIsBusy] = useState(false);
  /** `undefined` while the author has not browsed anywhere yet — the
   * browser then opens on the content's own folder, which is also the one
   * target the Move button refuses. `null` is the space root. */
  const [target, setTarget] = useState<string | null | undefined>(undefined);

  // The space the content already lives in: leaving it is a transfer, not a
  // move, so the browser only ever offers that space's own folders.
  const contentFolder = (allFolders ?? []).find((folder) => folder.id === content.folder_id);
  const space = spaces.find((candidate) => candidate.id === contentFolder?.space_id);
  const homeFolderId = space ? (homeFolderOf(allFolders ?? [], space.id)?.id ?? null) : null;

  const browsedFolderId = target === undefined ? (content.folder_id ?? null) : target;
  const targetFolderId = browsedFolderId ?? homeFolderId;

  const handleMoveToFolder = async () => {
    try {
      if (!content) return;
      setIsBusy(true);
      const payload = {
        folder_id: targetFolderId ?? undefined,
      };
      if (isBundleTile(content)) {
        await updateBundle(content.id, { folder_id: targetFolderId ?? undefined });
        mutate(matchesContentListKey);
      } else if (type === "layer") {
        payload["id"] = content.id;
        await updateDataset(content.id, payload as PostDataset);
        mutate(matchesContentListKey);
      } else if (type === "project") {
        await updateProject(content.id, payload as PostProject);
        mutate((key) => Array.isArray(key) && key[0] === PROJECTS_API_BASE_URL);
      }
    } catch {
      toast.error(`${t("error_moving_content")} ${content.name}`);
    } finally {
      setIsBusy(false);
      onClose?.();
    }

    onContentMove?.();
  };

  return (
    <AppDialog
      open={open}
      onClose={() => onClose?.()}
      icon={ICON_NAME.FOLDER}
      title={`${t("move_to")}`}
      maxWidth={444}
      footer={
        <AppDialogFooter
          onCancel={onClose}
          primaryLabel={t("move")}
          onPrimary={() => void handleMoveToFolder()}
          primaryLoading={isBusy}
          primaryDisabled={disabled || !targetFolderId || targetFolderId === content.folder_id}
        />
      }>
      <Stack spacing={2} sx={{ py: 2 }}>
        <Box>
          {space && (
            <FolderBrowser
              space={space}
              folders={folders}
              homeFolderId={homeFolderId}
              value={browsedFolderId}
              onChange={setTarget}
              label={t("folder")}
            />
          )}
        </Box>
      </Stack>
    </AppDialog>
  );
};

export default ContentMoveToFolderModal;
