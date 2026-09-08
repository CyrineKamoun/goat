"use client";

import { IconButton } from "@mui/material";
import type { MouseEvent } from "react";
import { useTranslation } from "react-i18next";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

/**
 * The × on every toast.
 *
 * Toastify's own is a bare SVG at a fixed opacity, which sat next to the MUI
 * icon button a composed toast used and read as two different controls for the
 * same action. This is the single representation: set once on `ToastContainer`
 * so plain-string toasts get it too, and reused by toasts that need the click
 * to do more than dismiss.
 */
const ToastCloseButton = ({
  closeToast,
  onClose,
}: {
  closeToast: (event: MouseEvent) => void;
  /** Runs before the toast closes, for a × that also cancels something. */
  onClose?: () => void;
}) => {
  const { t } = useTranslation("common");

  return (
    <IconButton
      size="small"
      aria-label={t("close")}
      // Against the first line rather than the middle, so it holds still while
      // a toast grows a second line.
      sx={{ alignSelf: "flex-start", color: "text.secondary" }}
      onClick={(event) => {
        onClose?.();
        closeToast(event);
      }}>
      <Icon iconName={ICON_NAME.XCLOSE} style={{ fontSize: 12 }} />
    </IconButton>
  );
};

export default ToastCloseButton;
