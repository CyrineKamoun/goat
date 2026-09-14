"use client";

import {
  Button,
  Dialog,
  DialogContent,
  IconButton,
  Skeleton,
  Stack,
  useMediaQuery,
  useTheme,
} from "@mui/material";
import { useTranslation } from "react-i18next";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import { useDataset } from "@/lib/api/layers";

import DatasetDetail from "@/components/dashboard/dataset/DatasetDetail";
import { contentDialogPaperSx } from "@/components/modals/content/ContentDialogChrome";

/** The template browser's height, so the two browsing dialogs match. */
const DIALOG_HEIGHT = 800;

interface ContentPreviewDialogProps {
  layerId: string;
  onClose: () => void;
  /** Each button renders only when a handler is passed. The Content page
   * passes them only for a layer the caller may act on (owner, and not a
   * shortcut), so a viewer sees neither. */
  onShare?: () => void;
  onMove?: () => void;
}

/**
 * Where a dataset is read: the `DatasetDetail` body in a dialog, so the feed
 * or list underneath it stays mounted and selected. A layer has no page of its
 * own — the Content page, Home search and a bundle's member list all open this.
 */
const ContentPreviewDialog = ({ layerId, onClose, onShare, onMove }: ContentPreviewDialogProps) => {
  const { t } = useTranslation("common");
  const theme = useTheme();
  const fullScreen = useMediaQuery(theme.breakpoints.down("md"));
  const { dataset, isLoading } = useDataset(layerId);

  return (
    <Dialog
      open
      onClose={onClose}
      fullWidth
      maxWidth="lg"
      fullScreen={fullScreen}
      // On the paper, which carries `role="dialog"`; the modal root would take
      // the label otherwise and leave the dialog itself unnamed.
      // A fixed height: the Summary and Data tabs measure differently, and the
      // Data tab's rows arrive after it opens, so a paper sized to its content
      // would shrink and grow with every switch.
      PaperProps={{
        "aria-label": dataset?.name,
        sx: contentDialogPaperSx(1200, fullScreen, { height: DIALOG_HEIGHT }),
      }}>
      {isLoading && (
        <DialogContent sx={{ p: { xs: 4, md: 6 }, display: "flex", flexDirection: "column" }}>
          {/* The header carries the close control once the dataset is there;
           * until then this is the only one. */}
          <Stack direction="row" justifyContent="flex-end" sx={{ mb: 2 }}>
            <IconButton size="small" onClick={onClose} aria-label={t("close")}>
              <Icon iconName={ICON_NAME.CLOSE} fontSize="small" />
            </IconButton>
          </Stack>
          {/* Fills the body, so the paper does not resize once the dataset is in. */}
          <Skeleton variant="rectangular" width="100%" sx={{ flex: "1 1 auto", height: "auto" }} />
        </DialogContent>
      )}
      {!isLoading && dataset && (
        <DatasetDetail
          dataset={dataset}
          actions={
            <>
              {onShare && (
                <Button
                  variant="outlined"
                  onClick={onShare}
                  startIcon={<Icon iconName={ICON_NAME.SHARE} style={{ fontSize: 13 }} />}>
                  {t("share")}
                </Button>
              )}
              {onMove && (
                <Button
                  variant="outlined"
                  onClick={onMove}
                  startIcon={<Icon iconName={ICON_NAME.FOLDER} style={{ fontSize: 13 }} />}>
                  {t("move_to")}
                </Button>
              )}
              {/* Last in the row, so it never sits over the buttons before it. */}
              <IconButton size="small" onClick={onClose} aria-label={t("close")} sx={{ ml: 1 }}>
                <Icon iconName={ICON_NAME.CLOSE} fontSize="small" />
              </IconButton>
            </>
          }
        />
      )}
    </Dialog>
  );
};

export default ContentPreviewDialog;
