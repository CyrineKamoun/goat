"use client";

import {
  FormControlLabel,
  MenuItem,
  Stack,
  Switch,
  TextField,
  Typography,
} from "@mui/material";
import { useEffect, useMemo, useRef, useState } from "react";
import { useTranslation } from "react-i18next";

import { BUNDLE_TYPES } from "@/lib/api/bundles";

import type { UploadFlow } from "@/hooks/addLayer/useUploadFlow";

import LayerSetupDialog from "@/components/addLayer/LayerSetupDialog";
import UploadDropzone from "@/components/addLayer/UploadDropzone";
import UploadFileRow from "@/components/addLayer/UploadFileRow";
import CatalogFeatureTable from "@/components/dashboard/catalog/CatalogFeatureTable";

/**
 * The Upload tab: a drop zone, and what was dropped.
 *
 * One screen. The drop zone is the empty state: one file is taken at a time, so once one
 * is queued the zone gives way to the row describing it, and removing the file brings the
 * zone back. A file's own settings open in `LayerSetupDialog`, on top of this screen
 * rather than inside it.
 *
 * The row is written as one of a list, so taking several files later adds rows beside a
 * zone that stays, rather than a second design.
 */
const UploadBody = ({
  controller,
  autoOpenSetup,
}: {
  controller: UploadFlow;
  /** Set when the file arrived from elsewhere and its settings are the only thing left. */
  autoOpenSetup?: boolean;
}) => {
  const { t } = useTranslation("common");
  const { upload } = controller;
  const [setUpOpen, setSetUpOpen] = useState(false);

  /**
   * Open the settings as soon as there is something to show in them.
   *
   * Waits for the preview: opening on mount gives an empty dialog for as long as the file
   * takes to parse. Fires once, so closing it does not reopen it.
   */
  const opened = useRef(false);
  useEffect(() => {
    if (!autoOpenSetup || opened.current) return;
    if (!upload.isTabular || !upload.preview) return;
    opened.current = true;
    setSetUpOpen(true);
  }, [autoOpenSetup, upload.isTabular, upload.preview]);

  /**
   * The parsed head, in the shape the catalog's own preview table reads.
   *
   * `CatalogFeatureTable` rather than the `FeatureTable` under it: the header colour, the
   * band painted beside the sticky header and the max height all live in that wrapper, so
   * re-deriving them here would be the same table with a different header — which is
   * exactly the mismatch worth avoiding.
   */
  /**
   * Header labels made unique before anything keys off them.
   *
   * A CSV may repeat a column name — two "Billing Contact" columns is ordinary in an
   * export — and the table keys its columns by name. Left as they came, React warned about
   * duplicate keys and, worse, the row objects were built with `Object.fromEntries`, so the
   * second column's values silently overwrote the first and both columns showed the same
   * data. The suffix is for this preview only; what the import calls them is its own
   * business.
   */
  const previewLabels = useMemo(() => {
    const seen = new Map<string, number>();
    return (upload.preview?.headers ?? []).map((header, index) => {
      const base = header?.trim() || `column${index + 1}`;
      const count = (seen.get(base) ?? 0) + 1;
      seen.set(base, count);
      return count === 1 ? base : `${base} (${count})`;
    });
  }, [upload.preview]);

  const previewColumns = useMemo(
    () => previewLabels.map((name) => ({ name, type: "string" })),
    [previewLabels]
  );
  const previewFeatures = useMemo(
    () =>
      (upload.preview?.rows ?? []).map((row, index) => ({
        type: "Feature" as const,
        id: index,
        geometry: null,
        properties: Object.fromEntries(previewLabels.map((label, column) => [label, row[column]])),
      })),
    [upload.preview, previewLabels]
  );

  /** What the client genuinely knows about a workbook, from the head it already parsed. */
  const detail =
    upload.preview && upload.preview.headers.length > 0
      ? t("upload_rows_columns", {
          rows: upload.preview.totalRows,
          columns: upload.preview.headers.length,
        })
      : undefined;

  return (
    <Stack spacing={4}>
      <Typography variant="caption" color="text.secondary" sx={{ lineHeight: 1.6 }}>
        {t("supported")} <b>GeoPackage</b>, <b>GeoJSON</b>, <b>Shapefile (.zip)</b>, <b>KML</b>,{" "}
        <b>CSV</b>, <b>XLSX</b>, <b>Parquet</b>,{" "}
        {BUNDLE_TYPES.map((type, index) => (
          <span key={type.type}>
            <b>{type.uploadHint}</b>
            {index < BUNDLE_TYPES.length - 1 ? ", " : ""}
          </span>
        ))}
        . {t("upload_multi_dataset_hint")}
      </Typography>

      {/* One file at a time, so the zone is the empty state and nothing more: once a file
          is queued there is nothing to drop it next to, and a second zone would only
          invite something the upload cannot take. Removing the file brings it back. */}
      {upload.file ? (
        <Stack spacing={2}>
          <Typography variant="caption" color="text.secondary" fontWeight="bold">
            {t("file")}
          </Typography>
          <UploadFileRow
            file={upload.file}
            name={upload.values.name ?? upload.suggestedName}
            nameError={upload.errors.name?.message}
            onRename={upload.setName}
            detail={detail}
            folders={upload.folders}
            selectedFolder={upload.selectedFolder}
            onSelectFolder={upload.setSelectedFolder}
            description={upload.values.description ?? ""}
            onDescriptionChange={upload.setDescription}
            onSetUp={upload.isTabular ? () => setSetUpOpen(true) : undefined}
            onRemove={() => upload.setFile(null)}
          />
          {upload.bundleType && (
            <Typography variant="caption" color="text.secondary">
              {t("bundle_detected_note", { type: t(upload.bundleType.labelKey) })}
            </Typography>
          )}
        </Stack>
      ) : (
        <UploadDropzone
          accept={upload.acceptedFileTypes}
          error={upload.fileError}
          onChange={upload.setFile}
        />
      )}

      <LayerSetupDialog
        open={setUpOpen && upload.isTabular}
        fileName={upload.file?.name ?? ""}
        onClose={() => setSetUpOpen(false)}
        onSave={() => setSetUpOpen(false)}>
        <Stack direction="row" spacing={5} alignItems="flex-start">
          {/* The settings in a rail of their own, as a column of decisions, with the
              consequence stated under them rather than left to be inferred. */}
          <Stack spacing={4} sx={{ width: 280, flexShrink: 0 }}>
            {upload.preview && upload.preview.sheetNames.length > 1 && (
              <TextField
                select
                fullWidth
                size="small"
                label={t("worksheet")}
                value={upload.sheet}
                onChange={(event) => upload.setSheet(event.target.value)}>
                {upload.preview.sheetNames.map((name) => (
                  <MenuItem key={name} value={name}>
                    {name}
                  </MenuItem>
                ))}
              </TextField>
            )}

            <FormControlLabel
              control={
                <Switch
                  checked={upload.hasHeader}
                  onChange={(event) => upload.setHasHeader(event.target.checked)}
                  color="primary"
                  size="small"
                />
              }
              label={
                <Typography variant="body2" fontWeight="bold">
                  {t("first_row_is_header")}
                </Typography>
              }
            />

          </Stack>

          <Stack spacing={2} sx={{ flex: 1, minWidth: 0 }}>
            {upload.preview && upload.preview.headers.length > 0 && (
              <>
                <Typography variant="body2" fontWeight="bold">
                  {t("upload_preview_of_rows", { count: upload.preview.rows.length })}
                </Typography>
                <CatalogFeatureTable
                  features={previewFeatures as never}
                  columns={previewColumns as never}
                />
              </>
            )}
          </Stack>
        </Stack>
      </LayerSetupDialog>
    </Stack>
  );
};

export default UploadBody;
