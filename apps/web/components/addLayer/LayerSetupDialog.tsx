"use client";

import { Box } from "@mui/material";
import { Trans, useTranslation } from "react-i18next";

import { ICON_NAME } from "@p4b/ui/components/Icon";

import AppDialog, { AppDialogFooter } from "@/components/common/AppDialog";

/**
 * The panel a file's own settings open in, on top of whatever opened it.
 *
 * Deliberately empty of subject knowledge: it owns the frame, the title, the close and
 * the footer, and takes the settings themselves as children. A workbook's header row is
 * the only thing that needs it today; a shapefile's encoding, a projection, a column
 * mapping would each be another `children` and no change here.
 *
 * Its own dialog rather than a section of the upload screen, because these settings are
 * a detour that most uploads never take, and a preview table needs more width than the
 * screen that sent you here.
 */
const LayerSetupDialog = ({
  open,
  fileName,
  onClose,
  onSave,
  children,
}: {
  open: boolean;
  /** The file being set up: named in the header's second line, and the dialog's
   * accessible name. */
  fileName: string;
  onClose: () => void;
  onSave: () => void;
  children: React.ReactNode;
}) => {
  const { t } = useTranslation("common");

  return (
    <AppDialog
      open={open}
      onClose={onClose}
      icon={ICON_NAME.TABLE}
      title={t("upload_set_up_columns")}
      // The file is named in the second line rather than in the title: that line wraps
      // where a heading would be pushed out of the frame by an export named after the
      // query that produced it.
      subtitle={
        <Box component="span" sx={{ display: "block", overflowWrap: "anywhere" }}>
          <Trans
            i18nKey="upload_set_up_intro"
            t={t}
            values={{ file: fileName }}
            components={{ file: <Box component="span" sx={{ fontWeight: 700 }} /> }}
          />
        </Box>
      }
      // Named after the file being set up, which is what tells two of these apart.
      ariaLabel={fileName}
      // Wider than the screen that opened it: a preview can run to dozens of columns, and
      // this is the one place they are meant to be read.
      maxWidth="min(1100px, 94vw)"
      bodySx={{ pt: 3 }}
      footer={<AppDialogFooter onCancel={onClose} primaryLabel={t("confirm")} onPrimary={onSave} />}>
      {children}
    </AppDialog>
  );
};

export default LayerSetupDialog;
