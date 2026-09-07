import { Box, Button, Stack, Typography } from "@mui/material";
import { useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { toast } from "react-toastify";

import { ICON_NAME } from "@p4b/ui/components/Icon";

import { uploadAsset } from "@/lib/api/assets";
import { useSpaces } from "@/lib/api/content";
import { getWritableFolders, useFolders } from "@/lib/api/folders";
import { homeFolderOf } from "@/lib/utils/content";
import { DOCUMENTS_MAX_FILE_SIZE_MiB, DOCUMENT_ACCEPT } from "@/lib/validations/assets";

import AppDialog, { AppDialogFooter } from "@/components/common/AppDialog";
import FolderBrowser from "@/components/dashboard/common/FolderBrowser";

interface DocumentUploadProps {
  open: boolean;
  onClose: () => void;
  defaultFolderId?: string;
  onSuccess?: () => void;
}

export default function DocumentUploadModal({
  open,
  onClose,
  defaultFolderId,
  onSuccess,
}: DocumentUploadProps) {
  const { t } = useTranslation("common");
  const { folders: allFolders } = useFolders({});
  const writableFolders = getWritableFolders(allFolders);
  const { spaces } = useSpaces();

  /** `undefined` until the author browses somewhere — the browser opens on
   * the folder the upload was started from. `null` is the space root. */
  const [target, setTarget] = useState<string | null | undefined>(undefined);
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [isBusy, setIsBusy] = useState(false);
  const fileInputRef = useRef<HTMLInputElement>(null);

  // The space the upload was started in — the folder it came from names it,
  // and a caller that passed no folder uploads into the personal space.
  const defaultFolder = (allFolders ?? []).find((folder) => folder.id === defaultFolderId);
  const space =
    spaces.find((candidate) => candidate.id === defaultFolder?.space_id) ??
    spaces.find((candidate) => candidate.kind === "personal");
  const homeFolderId = space ? (homeFolderOf(allFolders ?? [], space.id)?.id ?? null) : null;

  const browsedFolderId = target === undefined ? (defaultFolderId ?? null) : target;
  // Uploading needs write access on the destination. The browser lists only
  // writable folders, but the folder the upload was started in and the space
  // root both arrive from outside that list — a read-only one leaves nothing
  // to upload into, so `Upload` stays refused until the author browses to a
  // folder they may write.
  const resolvedFolderId = browsedFolderId ?? homeFolderId;
  const targetFolderId =
    resolvedFolderId && writableFolders.some((folder) => folder.id === resolvedFolderId)
      ? resolvedFolderId
      : null;

  const acceptAttr = Object.values(DOCUMENT_ACCEPT).flat().join(",");

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0] ?? null;
    if (file && file.size > DOCUMENTS_MAX_FILE_SIZE_MiB * 1024 * 1024) {
      toast.error(`File too large. Maximum size is ${DOCUMENTS_MAX_FILE_SIZE_MiB} MiB.`);
      return;
    }
    setSelectedFile(file);
  };

  const handleUpload = async () => {
    if (!selectedFile || !targetFolderId) return;
    try {
      setIsBusy(true);
      await uploadAsset(selectedFile, "document", { folderId: targetFolderId });
      toast.success(t("document_upload_success"));
      onSuccess?.();
      handleClose();
    } catch {
      toast.error(t("error_uploading_document"));
    } finally {
      setIsBusy(false);
    }
  };

  const handleClose = () => {
    setSelectedFile(null);
    if (fileInputRef.current) fileInputRef.current.value = "";
    onClose();
  };

  return (
    <AppDialog
      open={open}
      onClose={handleClose}
      icon={ICON_NAME.UPLOAD}
      title={t("upload_document")}
      maxWidth={600}
      footer={
        <AppDialogFooter
          onCancel={handleClose}
          primaryLabel={t("upload")}
          onPrimary={() => void handleUpload()}
          primaryLoading={isBusy}
          primaryDisabled={!selectedFile || !targetFolderId}
        />
      }>
      <Stack spacing={3} sx={{ mt: 1 }}>
        {space && (
          <FolderBrowser
            space={space}
            folders={writableFolders}
            homeFolderId={homeFolderId}
            value={browsedFolderId}
            onChange={setTarget}
            label={t("folder")}
          />
        )}
        <Box>
          <input
            ref={fileInputRef}
            type="file"
            accept={acceptAttr}
            style={{ display: "none" }}
            id="document-file-input"
            onChange={handleFileChange}
          />
          <label htmlFor="document-file-input">
            <Button variant="outlined" component="span" fullWidth>
              {selectedFile ? selectedFile.name : t("select_file")}
            </Button>
          </label>
          <Typography variant="caption" color="text.secondary" sx={{ mt: 0.5, display: "block" }}>
            PDF, DOCX, DOC — max {DOCUMENTS_MAX_FILE_SIZE_MiB} MiB
          </Typography>
        </Box>
      </Stack>
    </AppDialog>
  );
}
