"use client";

import { Stack, Typography } from "@mui/material";
import { useEffect, useRef } from "react";
import { useTranslation } from "react-i18next";
import { type Id, toast } from "react-toastify";

import { transferCleared } from "@/lib/store/uploads/slice";

import ToastCloseButton from "@/components/common/ToastCloseButton";

import { cancelTransfer } from "@/hooks/addLayer/useDatasetImport";
import { useAppDispatch, useAppSelector } from "@/hooks/store/ContextHooks";

const humanSize = (bytes: number): string => {
  if (bytes < 1024 * 1024) return `${Math.round(bytes / 1024)} kB`;
  const mb = bytes / (1024 * 1024);
  // Past a thousand megabytes it should read as gigabytes: "1949.6 MB" is a number to decode
  // rather than a size to recognise.
  return mb < 1024 ? `${mb.toFixed(1)} MB` : `${(mb / 1024).toFixed(2)} GB`;
};

/**
 * A filename that cannot push the toast open.
 *
 * Middle-truncated rather than clipped at the end, because the extension is the part worth
 * keeping — `project-export-Stadtklimadashboard-20260804_184850.zip` says most of what it
 * has to say in its first words and its last four. CSS ellipsis alone was not enough: the
 * toast body has no bounded width of its own, so the name simply ran past its edge.
 */
const shortName = (name: string): string => {
  if (name.length <= 34) return name;
  return `${name.slice(0, 20)}…${name.slice(-11)}`;
};

/**
 * The toast's two lines: what the file is, then what is happening to it.
 *
 * Two lines with different weights rather than one sentence, because a filename and a byte
 * count are different kinds of fact — run together at one size they wrap into a paragraph
 * that has to be read to be understood.
 */
const TransferBody = ({ fileName, detail }: { fileName: string; detail: string }) => (
  <Stack sx={{ minWidth: 0, maxWidth: "100%", overflow: "hidden" }}>
    <Typography
      variant="body2"
      fontWeight={700}
      title={fileName}
      // Primary ink: it is the heading of the toast, and inheriting the body's secondary
      // colour left the bold line looking weaker than the caption under it.
      sx={{
        color: "text.primary",
        overflow: "hidden",
        textOverflow: "ellipsis",
        whiteSpace: "nowrap",
      }}>
      {shortName(fileName)}
    </Typography>
    <Typography variant="caption" color="text.secondary" sx={{ lineHeight: 1.4 }}>
      {detail}
    </Typography>
  </Stack>
);

/**
 * A file's trip from the browser to S3, told through the app's own toasts.
 *
 * `react-toastify` rather than a panel of our own: every other message in the app arrives
 * this way, and a hand-built banner meant one notification that looked like nothing else and
 * sat wherever it was put. Toastify also takes a controlled `progress`, so the bar it already
 * draws becomes the real percentage.
 *
 * Renders nothing. It syncs the uploads slice into toasts and keeps the page asking before it
 * closes while bytes are still moving.
 */
const TransferToasts = () => {
  const { t } = useTranslation("common");
  const dispatch = useAppDispatch();
  const transfers = useAppSelector((state) => state.uploads.transfers);
  // Which toast belongs to which transfer, so progress updates the same one.
  const toastIds = useRef(new Map<string, Id>());

  const live = transfers.some(
    (transfer) => transfer.status === "starting" || transfer.status === "uploading"
  );

  useEffect(() => {
    if (!live) return;
    const ask = (event: BeforeUnloadEvent) => event.preventDefault();
    window.addEventListener("beforeunload", ask);
    // Removed as soon as nothing is in flight: left registered, it interrupts ordinary
    // navigation and teaches people to click straight through it.
    return () => window.removeEventListener("beforeunload", ask);
  }, [live]);

  useEffect(() => {
    /**
     * A toast whose transfer is gone must go with it.
     *
     * The loop below only visits transfers that still exist, so a row removed from the slice
     * — a cancelled upload — used to leave its toast spinning on "Preparing upload" for ever.
     * There is no `onClose` handler doing the cancelling any more either: it fired on toasts
     * the update cycle replaced, aborting transfers nobody had touched.
     */
    toastIds.current.forEach((toastId, transferId) => {
      if (transfers.some((transfer) => transfer.id === transferId)) return;
      toast.dismiss(toastId);
      toastIds.current.delete(transferId);
    });

    transfers.forEach((transfer) => {
      const existing = toastIds.current.get(transfer.id);
      const share = transfer.total > 0 ? transfer.sent / transfer.total : 0;
      /**
       * Everything handed to the socket, nothing acknowledged yet.
       *
       * `upload.onprogress` counts bytes written to the network stack, not bytes the server
       * has taken. Against a local MinIO that reaches 100% almost at once while the request
       * is still open, so holding "1.9 GB of 1.9 GB" on screen would read as stuck. Past this
       * point the count has nothing left to say and the state does.
       */
      const flushed = transfer.status === "uploading" && share >= 0.999;

      if (existing === undefined) {
        // Only a fresh transfer gets a toast. Without this, the terminal update below —
        // which deletes the id and clears the row — could be followed by one more pass that
        // saw no id and opened a second toast for a file already reported on.
        if (transfer.status !== "starting") return;
        toastIds.current.set(
          transfer.id,
          toast.loading(<TransferBody fileName={transfer.fileName} detail={t("upload_preparing")} />, {
            progress: 0,
            /**
             * Our own close button, because this one has to do something.
             *
             * Toastify's default merely dismisses the toast, and `onClose` alone proved too
             * indirect — options set at creation are not reliably carried through the
             * `toast.update` calls that follow. Cancelling is explicit here: abort the
             * transfer, drop the row, then let the toast close.
             */
            closeButton: ({ closeToast }: { closeToast: (event: React.MouseEvent) => void }) => (
              <ToastCloseButton
                closeToast={closeToast}
                onClose={() => {
                  cancelTransfer(transfer.id);
                  dispatch(transferCleared(transfer.id));
                }}
              />
            ),
            /**
             * Dismissing the toast cancels the upload.
             *
             * The close button used to hide the toast and leave the transfer running: bytes
             * kept moving with nothing on screen, and the layer appeared later as if from
             * nowhere. This is the only cancel there is, so it has to be the real one. Once a
             * transfer has finished there is no controller left and this does nothing.
             */
          })
        );
        return;
      }

      if (transfer.status === "uploading") {
        toast.update(existing, {
          render: (
            <TransferBody
              fileName={transfer.fileName}
              detail={
                flushed
                  ? t("upload_finishing")
                  : `${humanSize(transfer.sent)} ${t("of")} ${humanSize(transfer.total)} · ${Math.round(share * 100)}%`
              }
            />
          ),
          // Controlled: toastify draws this rather than its own timer.
          progress: Math.min(share, 0.99),
        });
        return;
      }

      if (transfer.status === "handed-off" || transfer.status === "failed") {
        const failed = transfer.status === "failed";
        toast.update(existing, {
          render: (
            <TransferBody
              fileName={transfer.fileName}
              detail={failed ? t("upload_failed_hint") : t("import_started")}
            />
          ),
          type: failed ? "error" : "success",
          isLoading: false,
          // Releases the controlled progress so the toast can time itself out again.
          progress: undefined,
          autoClose: 4000,
        });
        toastIds.current.delete(transfer.id);
        dispatch(transferCleared(transfer.id));
      }
    });
  }, [transfers, dispatch, t]);

  return null;
};

export default TransferToasts;
