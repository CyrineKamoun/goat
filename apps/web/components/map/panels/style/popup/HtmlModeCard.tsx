import { Box, Button, Stack, Typography, useTheme } from "@mui/material";
import { type ReactNode, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import type { PopupProperties } from "@/lib/validations/layer";

import { HtmlEditorDialog } from "./HtmlEditorDialog";

interface Props {
  layerId: string;
  layerName: string;
  layerIcon?: ReactNode;
  popup: PopupProperties;
  onChange: (patch: Partial<PopupProperties>) => void;
}

export function HtmlModeCard({ layerId, layerName, layerIcon, popup, onChange }: Props) {
  const { t } = useTranslation("common");
  const theme = useTheme();
  const [open, setOpen] = useState(false);

  const stats = useMemo(() => {
    const html = popup.html ?? "";
    return { lines: html ? html.split("\n").length : 0, chars: html.length };
  }, [popup.html]);

  return (
    <>
      <Stack
        direction="row"
        alignItems="center"
        spacing={1.5}
        sx={{ p: 1.5, border: `1px solid ${theme.palette.divider}`, borderRadius: 1 }}>
        <Box
          sx={{
            width: 36,
            height: 36,
            borderRadius: 1,
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            bgcolor: theme.palette.action.selected,
          }}>
          <Icon iconName={ICON_NAME.CODE} style={{ fontSize: 16 }} />
        </Box>
        <Stack sx={{ flex: 1, minWidth: 0 }}>
          <Typography variant="body2" fontWeight="bold">
            {t("custom_html")}
          </Typography>
          <Typography variant="caption" color="text.secondary">
            {t("html_lines_chars", { lines: stats.lines, chars: stats.chars })}
          </Typography>
        </Stack>
        <Button variant="outlined" size="small" onClick={() => setOpen(true)}>
          {t("edit_html")}
        </Button>
      </Stack>

      {/* Conditionally mounted so HtmlEditorDialog's draft useState re-seeds
          from the current popup.html on every open (no stale draft). */}
      {open && (
        <HtmlEditorDialog
          open
          layerId={layerId}
          layerName={layerName}
          layerIcon={layerIcon}
          popup={popup}
          onChange={onChange}
          onClose={() => setOpen(false)}
        />
      )}
    </>
  );
}
