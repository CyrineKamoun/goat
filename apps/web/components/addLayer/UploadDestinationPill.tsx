"use client";

import { Box, Popover, Stack, Typography, useTheme } from "@mui/material";
import { useRef, useState } from "react";
import { useTranslation } from "react-i18next";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import type { Folder } from "@/lib/validations/folder";

import UploadPill from "@/components/addLayer/UploadPill";

/**
 * Where a file will land: a pill that opens the folders under it.
 *
 * A dropdown rather than a select on the screen, because the destination is already right
 * nearly every time — it is the project's own folder — and a control you rarely touch
 * should not occupy a row of the form.
 *
 * The list is flat because folders are: `Folder` has no parent and no team. When they gain
 * one, the team goes in this popover's header and the folders nest under it; nothing about
 * the pill changes.
 */
const UploadDestinationPill = ({
  folders,
  selected,
  onSelect,
}: {
  folders?: Folder[];
  selected?: Folder | null;
  onSelect: (folder: Folder) => void;
}) => {
  const { t } = useTranslation("common");
  const theme = useTheme();
  const anchor = useRef<HTMLDivElement | null>(null);
  const [open, setOpen] = useState(false);

  return (
    <>
      <Box ref={anchor} sx={{ display: "inline-flex", maxWidth: "100%" }}>
        <UploadPill
          icon={ICON_NAME.FOLDER}
          label={selected?.name ?? t("folder")}
          tone={selected ? "active" : "default"}
          trailingIcon={ICON_NAME.CHEVRON_DOWN}
          title={selected?.name}
          onClick={() => setOpen(true)}
        />
      </Box>

      <Popover
        open={open}
        anchorEl={anchor.current}
        onClose={() => setOpen(false)}
        anchorOrigin={{ vertical: "bottom", horizontal: "left" }}
        transformOrigin={{ vertical: "top", horizontal: "left" }}
        slotProps={{ paper: { sx: { mt: 1.5, width: 288, maxHeight: 320, overflow: "hidden" } } }}>
        <Typography
          variant="caption"
          fontWeight={700}
          sx={{
            display: "block",
            px: 4,
            py: 2.5,
            letterSpacing: 0.5,
            color: theme.palette.text.secondary,
            borderBottom: `1px solid ${theme.palette.divider}`,
            backgroundColor: theme.palette.action.hover,
          }}>
          {t("folder").toUpperCase()}
        </Typography>

        <Stack sx={{ p: 1.5, maxHeight: 264, overflowY: "auto" }}>
          {(folders ?? []).map((folder) => {
            const on = folder.id === selected?.id;
            return (
              <Stack
                key={folder.id}
                component="button"
                type="button"
                direction="row"
                alignItems="center"
                spacing={2.5}
                onClick={() => {
                  onSelect(folder);
                  setOpen(false);
                }}
                sx={{
                  px: 2.5,
                  py: 2,
                  border: "none",
                  borderRadius: 1.5,
                  font: "inherit",
                  textAlign: "left",
                  cursor: "pointer",
                  backgroundColor: on ? theme.palette.action.selected : "transparent",
                  "&:hover": { backgroundColor: theme.palette.action.hover },
                }}>
                <Icon
                  iconName={ICON_NAME.FOLDER}
                  style={{ fontSize: 14 }}
                  htmlColor={on ? theme.palette.primary.main : theme.palette.text.secondary}
                />
                <Typography
                  variant="body2"
                  noWrap
                  fontWeight={on ? 700 : 400}
                  sx={{ flex: 1, minWidth: 0 }}>
                  {folder.name}
                </Typography>
                {on && (
                  <Icon
                    iconName={ICON_NAME.CIRCLECHECK}
                    style={{ fontSize: 13 }}
                    htmlColor={theme.palette.primary.main}
                  />
                )}
              </Stack>
            );
          })}
        </Stack>
      </Popover>
    </>
  );
};

export default UploadDestinationPill;
