"use client";

import { Box, IconButton, Stack, TextField, Typography, useTheme } from "@mui/material";
import { useState } from "react";
import { useTranslation } from "react-i18next";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import type { Folder } from "@/lib/validations/folder";

import { MAX_NAME_LENGTH } from "@/hooks/addLayer/useUploadFlow";

import UploadDestinationPill from "@/components/addLayer/UploadDestinationPill";
import UploadPill from "@/components/addLayer/UploadPill";

/** `4.2 MB`, `840 kB` — a size someone can read. */
const humanSize = (bytes: number): string => {
  if (bytes < 1024) return `${bytes} B`;
  const units = ["kB", "MB", "GB"];
  let value = bytes / 1024;
  let unit = 0;
  while (value >= 1024 && unit < units.length - 1) {
    value /= 1024;
    unit += 1;
  }
  return `${value >= 10 ? Math.round(value) : value.toFixed(1)} ${units[unit]}`;
};

/**
 * The icon and format label a file gets, from its extension.
 *
 * By format, not by geometry: nothing has read the file yet, so a point layer and a
 * polygon layer are indistinguishable here. Claiming otherwise would be a guess drawn as
 * a fact.
 */
const FORMATS: Record<string, { label: string; icon: ICON_NAME; noteKey?: string }> = {
  // A GeoPackage holds layers by definition, so saying every one is imported is a fact.
  gpkg: { label: "GeoPackage", icon: ICON_NAME.LAYERS, noteKey: "upload_note_layers" },
  // A zip is not a format. It is usually one shapefile, sometimes an archive of many
  // things, and nothing has looked inside — so it is named for what it is and the note
  // says what holds either way.
  zip: { label: "ZIP", icon: ICON_NAME.LAYERS, noteKey: "upload_note_archive" },
  geojson: { label: "GeoJSON", icon: ICON_NAME.POLYGON_FEATURE },
  kml: { label: "KML", icon: ICON_NAME.POLYGON_FEATURE },
  parquet: { label: "Parquet", icon: ICON_NAME.TABLE },
  csv: { label: "CSV", icon: ICON_NAME.TABLE },
  xlsx: { label: "XLSX", icon: ICON_NAME.TABLE },
  xls: { label: "XLS", icon: ICON_NAME.TABLE },
};

const describe = (file: File) => {
  const ext = file.name.split(".").pop()?.toLowerCase() ?? "";
  return FORMATS[ext] ?? { label: ext.toUpperCase(), icon: ICON_NAME.FILE };
};

/**
 * One queued file: what it will be called, where it will land, and the settings it can be
 * given before it goes.
 *
 * A row rather than a form, so the screen looks the same whether one file is queued or
 * several — the only thing that changes is how many of these there are.
 */
const UploadFileRow = ({
  file,
  name,
  nameError,
  onRename,
  detail,
  folders,
  selectedFolder,
  onSelectFolder,
  description,
  onDescriptionChange,
  onSetUp,
  onRemove,
}: {
  file: File;
  name: string;
  nameError?: string;
  onRename: (value: string) => void;
  /** Extra facts the client actually knows, e.g. a workbook's rows and columns. */
  detail?: string;
  folders?: Folder[];
  selectedFolder?: Folder | null;
  onSelectFolder: (folder: Folder) => void;
  description: string;
  onDescriptionChange: (value: string) => void;
  /** Present only for a format that has settings — a workbook, today. */
  onSetUp?: () => void;
  onRemove: () => void;
}) => {
  const { t } = useTranslation("common");
  const theme = useTheme();
  const format = describe(file);
  const [showDescription, setShowDescription] = useState(false);

  return (
    <Stack
      spacing={2}
      sx={{
        p: 3,
        borderRadius: 1.5,
        border: `1px solid ${nameError ? theme.palette.error.main : theme.palette.divider}`,
        backgroundColor: theme.palette.background.paper,
      }}>
      <Stack direction="row" alignItems="flex-start" spacing={3}>
        <Box
          sx={{
            // The same height as the name field, so the glyph's centre and the field's
            // text sit on one line: at 36 against a 40px field they were 2px apart.
            width: 40,
            height: 40,
            borderRadius: 1.5,
            flexShrink: 0,
            display: "grid",
            placeItems: "center",
            backgroundColor: theme.palette.action.hover,
          }}>
          <Icon
            iconName={format.icon}
            style={{ fontSize: 15 }}
            htmlColor={theme.palette.text.secondary}
          />
        </Box>

        <Stack spacing={1} sx={{ flex: 1, minWidth: 0 }}>
          <TextField
            fullWidth
            value={name}
            onChange={(event) => onRename(event.target.value)}
            placeholder={t("upload_layer_name")}
            // On the input, not the wrapper: MUI spreads unrecognised props onto the root,
            // which left the field with no accessible name at all.
            inputProps={{ "aria-label": t("upload_layer_name"), maxLength: MAX_NAME_LENGTH }}
            error={!!nameError}
            helperText={nameError}
            variant="outlined"
            size="small"
            sx={{
              // Three states, in the order they must win. The previous version set the
              // outline transparent and only overrode it on hover, which beat MUI's own
              // focus rule while the pointer was over the field: the green then appeared
              // when the mouse *left*, which is the opposite of what focus means.
              "& .MuiOutlinedInput-root": { fontWeight: 700, fontSize: 14 },
              // Every one of these is scoped away from `.Mui-error`, so an invalid name
              // stays red at rest, on hover and while focused. Unscoped, they overrode
              // MUI's error colour and the outline turned green whenever the pointer left
              // — the field's colour told you where the mouse was, not whether the name
              // was usable.
              "& .MuiOutlinedInput-root:not(.Mui-error) .MuiOutlinedInput-notchedOutline": {
                borderColor: "transparent",
              },
              "& .MuiOutlinedInput-root:not(.Mui-error):hover .MuiOutlinedInput-notchedOutline":
                { borderColor: theme.palette.primary.main },
              "& .MuiOutlinedInput-root:not(.Mui-error).Mui-focused .MuiOutlinedInput-notchedOutline":
                { borderColor: theme.palette.primary.main, borderWidth: 2 },
              // Taller: at 8px of vertical padding the field read as a label with a
              // border round it rather than something to type in.
              // 40px, which is what `size="small"` is everywhere else in the app.
              "& .MuiOutlinedInput-input": { px: 2, py: 2.5 },
              ml: -1.5,
            }}
          />

          <Typography
            variant="caption"
            color="text.secondary"
            // A filename can be one unbroken word longer than the row: it breaks anywhere
            // rather than pushing the card open.
            sx={{ px: 1.5, overflowWrap: "anywhere" }}>
            {file.name} · {humanSize(file.size)} · {format.label}
            {detail ? ` · ${detail}` : ""}
            {format.noteKey ? ` · ${t(format.noteKey)}` : ""}
          </Typography>

          <Stack direction="row" spacing={2} useFlexGap flexWrap="wrap" sx={{ px: 1.5, pt: 1 }}>
            <UploadDestinationPill
              folders={folders}
              selected={selectedFolder}
              onSelect={onSelectFolder}
            />
            {onSetUp && (
              <UploadPill
                icon={ICON_NAME.TABLE}
                label={t("upload_set_up_columns")}
                onClick={onSetUp}
              />
            )}
            <UploadPill
              icon={ICON_NAME.FILE}
              label={t("upload_add_description")}
              tone={showDescription || description ? "active" : "default"}
              onClick={() => setShowDescription((open) => !open)}
            />
          </Stack>
        </Stack>

        {/* Centred on the card rather than on any one line of it, so it does not shift when
            a validation message appears under the name. */}
        <IconButton
          onClick={onRemove}
          aria-label={t("upload_remove_file")}
          sx={{ p: 2, alignSelf: "center", flexShrink: 0 }}>
          <Icon iconName={ICON_NAME.XCLOSE} style={{ fontSize: 14 }} />
        </IconButton>
      </Stack>

      {showDescription && (
        // Indented past the thumbnail, so it reads as belonging to this row rather than
        // to the list.
        <Box sx={{ pl: 12, pr: 1 }}>
          <TextField
            fullWidth
            multiline
            /**
             * It grows as it is typed into, between four lines and twelve.
             *
             * Not a drag handle: this TextField renders MUI's `TextareaAutosize` whichever
             * of `rows`/`minRows` is given (measured — two textareas in the DOM and an
             * inline `height`), and that component owns the height from JS. A `resize`
             * handle on top of it fights the next keystroke, which is what made dragging
             * feel broken. Growing on its own needs no handle.
             */
            minRows={4}
            maxRows={12}
            size="small"
            value={description}
            onChange={(event) => onDescriptionChange(event.target.value)}
            placeholder={t("upload_description_placeholder")}
            sx={{
              "& .MuiOutlinedInput-root": {
                fontSize: 13,
                alignItems: "flex-start",
                // No padding here, all of it on the textarea below: with the padding on
                // the root, the textarea sat inside it and its scrollbar floated a dozen
                // pixels off the field's edge. Filling the box puts the bar at the corner.
                p: 0,
                // `action.hover`, not `background.default`: the latter is the *page*
                // background, which inside this card reads as a hole punched in it on a
                // dark theme. `action.hover` is a low-alpha tint of the theme's own ink,
                // so it lifts off the card the same small amount in either theme.
                backgroundColor: theme.palette.action.hover,
              },
              "& textarea": {
                boxSizing: "border-box",
                px: 3,
                py: 2.5,
              },
            }}
          />
        </Box>
      )}
    </Stack>
  );
};

export default UploadFileRow;
