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
import { useState } from "react";
import { useTranslation } from "react-i18next";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import { useBundle, useBundleDependencies, useBundleLayers } from "@/lib/api/bundles";

import BundleDetail from "@/components/dashboard/bundle/BundleDetail";
import BundleUpdateButton from "@/components/dashboard/bundle/BundleUpdateButton";
import { contentDialogPaperSx } from "@/components/modals/content/ContentDialogChrome";

interface BundlePreviewDialogProps {
  bundleId: string;
  onClose: () => void;
  /** Each button renders only when a handler is passed, as on the layer
   * dialog: a viewer sees neither. */
  onShare?: () => void;
  onMove?: () => void;
}

/**
 * Where a bundle is read: the `BundleDetail` body in a dialog, so the feed or
 * list underneath it stays mounted and selected — the same way a dataset is
 * read. A bundle has no page of its own.
 *
 * A dependency opens in place rather than stacking a second dialog: the one
 * being read is swapped, and closing returns to wherever the first was opened
 * from. Deeper nesting would leave a pile of modals over the feed.
 */
const BundlePreviewDialog = ({ bundleId, onClose, onShare, onMove }: BundlePreviewDialogProps) => {
  const { t } = useTranslation("common");
  const theme = useTheme();
  const fullScreen = useMediaQuery(theme.breakpoints.down("md"));
  const [openBundleId, setOpenBundleId] = useState(bundleId);
  const { bundle, isLoading } = useBundle(openBundleId);
  const { members, isLoading: areMembersLoading } = useBundleLayers(openBundleId);
  const { dependencies } = useBundleDependencies(openBundleId);

  // The actions act on the bundle the caller named; following a dependency
  // moves past what they were offered for.
  const isOriginal = openBundleId === bundleId;

  return (
    <Dialog
      open
      onClose={onClose}
      fullWidth
      maxWidth="lg"
      fullScreen={fullScreen}
      // On the paper, which carries `role="dialog"`; the modal root would take
      // the label otherwise and leave the dialog itself unnamed.
      PaperProps={{ "aria-label": bundle?.name, sx: contentDialogPaperSx(1200, fullScreen) }}>
      <DialogContent sx={{ p: { xs: 4, md: 6 } }}>
        {isLoading && (
          <>
            {/* The header carries the close control once the bundle is there;
             * until then this is the only one. */}
            <Stack direction="row" justifyContent="flex-end" sx={{ mb: 2 }}>
              <IconButton size="small" onClick={onClose} aria-label={t("close")}>
                <Icon iconName={ICON_NAME.CLOSE} fontSize="small" />
              </IconButton>
            </Stack>
            <Skeleton variant="rectangular" width="100%" height={400} />
          </>
        )}
        {!isLoading && bundle && (
          <BundleDetail
            bundle={bundle}
            members={members}
            areMembersLoading={areMembersLoading}
            dependencies={dependencies}
            onOpenBundle={setOpenBundleId}
            actions={
              <>
                {/* A failed artifact is what brings a user here; the remedy
                    belongs beside the status that prompted it. Offered for the
                    bundle being read, dependency or not — it is about that
                    bundle's own artifacts. */}
                <BundleUpdateButton bundle={bundle} />
                {onShare && isOriginal && (
                  <Button
                    variant="outlined"
                    onClick={onShare}
                    startIcon={<Icon iconName={ICON_NAME.SHARE} style={{ fontSize: 13 }} />}>
                    {t("share")}
                  </Button>
                )}
                {onMove && isOriginal && (
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
      </DialogContent>
    </Dialog>
  );
};

export default BundlePreviewDialog;
