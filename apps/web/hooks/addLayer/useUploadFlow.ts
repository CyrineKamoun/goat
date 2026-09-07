import { zodResolver } from "@hookform/resolvers/zod";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { type FieldErrors, type UseFormRegister, useForm } from "react-hook-form";
import { useTranslation } from "react-i18next";

import { type BundleTypeDef, detectBundleType } from "@/lib/api/bundles";
import { refreshContentFeed } from "@/lib/api/content";
import { getWritableFolders, useFolders } from "@/lib/api/folders";
import { useProject } from "@/lib/api/projects";
import {
  derivePreview,
  readTabularSource,
  type TabularPreview,
  type TabularSource,
} from "@/lib/utils/tabular-preview";
import type { GetContentQueryParams } from "@/lib/validations/common";
import type { Folder } from "@/lib/validations/folder";
import type { LayerMetadata } from "@/lib/validations/layer";
import { layerMetadataSchema } from "@/lib/validations/layer";

import { useDatasetImport } from "@/hooks/addLayer/useDatasetImport";

import type { FlowController } from "@/hooks/addLayer/flow";
import { useShareNotice } from "@/hooks/addLayer/useShareNotice";

/**
 * Uploading a file as a dataset: state, validation and submit — no UI.
 *
 * Headless so the flow is not tied to the modal that hosts it today; a side panel
 * or a page can mount this with a body of its own. Everything the host draws
 * (frame, footer) it draws from `FlowController`.
 *
 * The API sequence is unchanged from the dialog this replaces: presigned URL →
 * direct S3 upload → `createLayer`, whose OGC job id goes to the jobs store.
 */

const ACCEPTED_FILE_TYPES = [".gpkg", ".geojson", ".zip", ".kml", ".csv", ".xlsx", ".parquet"];
const TABULAR_EXTENSIONS = ["csv", "xlsx", "xls"];

/** As `contentMetadataSchema` enforces it — kept here so the field can stop at the same point. */
export const MAX_NAME_LENGTH = 100;

export type UploadFlowState = {
  file?: File;
  fileError?: string;
  setFile: (file: File | null) => void;
  acceptedFileTypes: string[];
  /** Writable folders, and the one this dataset lands in. */
  folders?: Folder[];
  selectedFolder?: Folder | null;
  setSelectedFolder: (folder: Folder | null) => void;
  /** Set when the file is a recognised bundle (e.g. GTFS) and will import as one. */
  bundleType: BundleTypeDef | null;
  /** CSV/XLSX only: the parsed head of the file plus how to read it. */
  isTabular: boolean;
  preview: TabularPreview | null;
  hasHeader: boolean;
  setHasHeader: (value: boolean) => void;
  sheet: string;
  setSheet: (value: string) => void;
  /** Metadata form, validated by `layerMetadataSchema`. */
  register: UseFormRegister<LayerMetadata>;
  errors: FieldErrors<LayerMetadata>;
  /** Defaults the name field to the file's own name, without its extension. */
  suggestedName: string;
  values: LayerMetadata;
  /** The row edits both directly — there is no form to register against. */
  setName: (value: string) => void;
  setDescription: (value: string) => void;
};

export type UploadFlow = FlowController & { upload: UploadFlowState };

export const useUploadFlow = ({
  projectId,
  defaultFolderId,
  initialFile,
  onDone,
}: {
  projectId?: string;
  defaultFolderId?: string;
  /** A file the host already has — dropped on the map, rather than chosen in here. */
  initialFile?: File;
  onDone?: () => void;
}): UploadFlow => {
  const { t } = useTranslation("common");

  const { project } = useProject(projectId);
  const notice = useShareNotice(projectId);
  const { importDataset } = useDatasetImport();
  const queryParams: GetContentQueryParams = { order: "descendent", order_by: "updated_at" };
  const { folders: allFolders } = useFolders(queryParams);
  const folders = getWritableFolders(allFolders);

  const [file, setFileValue] = useState<File>();
  const [fileError, setFileError] = useState<string>();
  const [selectedFolder, setSelectedFolder] = useState<Folder | null>();
  const folderInitialized = useRef(false);
  /**
   * Never busy: the dialog no longer waits for anything.
   *
   * It hands the file to the import and closes, so there is no window in which its control
   * should spin. What the transfer is doing is the banner's business.
   */
  const isBusy = false;
  const [source, setSource] = useState<TabularSource | null>(null);
  const [hasHeader, setHasHeader] = useState(true);
  const [sheet, setSheet] = useState("");

  const isTabular = useMemo(() => {
    const ext = file?.name.split(".").pop()?.toLowerCase();
    return !!ext && TABULAR_EXTENSIONS.includes(ext);
  }, [file]);

  const bundleType = useMemo(() => detectBundleType(file), [file]);


  // The project's own folder is the natural destination; otherwise whatever folder
  // the host was looking at.
  useEffect(() => {
    if (!folders || folderInitialized.current) return;
    const projectFolder = folders.find((folder) => folder.id === project?.folder_id);
    if (projectFolder) {
      setSelectedFolder(projectFolder);
      folderInitialized.current = true;
      return;
    }
    if (defaultFolderId) {
      const fallback = folders.find((folder) => folder.id === defaultFolderId);
      if (fallback) {
        setSelectedFolder(fallback);
        folderInitialized.current = true;
      }
    }
  }, [folders, project?.folder_id, defaultFolderId]);

  // Reading depends on the file and the worksheet only. `hasHeader` is not a dependency:
  // it changes how these rows are read, not which bytes are read, and having it here made
  // every flip of the switch re-read the file.
  useEffect(() => {
    if (!file || !isTabular) {
      setSource(null);
      return;
    }
    let cancelled = false;
    readTabularSource(file, { sheetName: sheet || undefined })
      .then((parsed) => {
        if (cancelled) return;
        setSource(parsed);
        if (!sheet && parsed.sheetNames.length > 0) setSheet(parsed.sheetNames[0]);
      })
      .catch((error) => {
        console.error("Preview parse error:", error);
        if (!cancelled) setSource(null);
      });
    return () => {
      cancelled = true;
    };
  }, [file, isTabular, sheet]);

  const preview = useMemo<TabularPreview | null>(
    () => (source ? derivePreview(source, hasHeader) : null),
    [source, hasHeader]
  );

  const {
    register,
    getValues,
    setValue,
    watch,
    reset: resetForm,
    formState: { errors, isValid },
  } = useForm<LayerMetadata>({ mode: "onChange", resolver: zodResolver(layerMetadataSchema) });
  // Watched, not read once: the footer's enabled state follows the fields.
  const values = watch();

  const suggestedName = useMemo(() => {
    if (!file) return "";
    const ext = file.name.split(".").pop();
    return ext && ACCEPTED_FILE_TYPES.includes(`.${ext}`)
      ? file.name.replace(`.${ext}`, "")
      : file.name;
  }, [file]);

  const setFile = useCallback(
    (next: File | null) => {
      setFileError(undefined);
      setFileValue(undefined);
      // Removing the file removes everything said about it: a name, a description and a
      // workbook's settings all described that file, and carrying them over to the next
      // one silently attaches them to something else.
      setValue("name", "", { shouldValidate: false });
      setValue("description", "", { shouldValidate: false });
      setSource(null);
      setHasHeader(true);
      setSheet("");
      if (!next?.name) return;
      // Case-insensitive, like the tabular reader that will open it — Windows
      // and most GIS exporters write `EXPORT.CSV`.
      if (!ACCEPTED_FILE_TYPES.some((type) => next.name.toLowerCase().endsWith(type))) {
        setFileError(`${t("invalid_file_type")}: ${ACCEPTED_FILE_TYPES.join(", ")}`);
        return;
      }
      setFileValue(next);
      // The suggested name goes into the form, not just onto the screen: with no field
      // registered for it, a name that only existed as display text left the form invalid
      // and the upload button dead.
      // Trimmed to the limit `contentMetadataSchema` enforces (100): an export named by a
      // SQL query is longer than that, and suggesting it verbatim opened the form invalid
      // with an error nobody had caused.
      setValue("name", next.name.replace(/\.[^/.]+$/, "").slice(0, MAX_NAME_LENGTH), {
        shouldValidate: true,
      });
    },
    [setValue, t]
  );

  const reset = useCallback(() => {
    setFileValue(undefined);
    setFileError(undefined);
    setSource(null);
    setHasHeader(true);
    setSheet("");
    setSelectedFolder(undefined);
    folderInitialized.current = false;
    resetForm();
  }, [resetForm]);

  /**
   * Hand the file to the import and close.
   *
   * Not awaited: the transfer can take minutes on a large file, and holding this dialog open
   * for it blocks the whole app for something the banner reports perfectly well on its own.
   * The dialog's job ends when the file and its settings have been handed over.
   */
  const submit = useCallback(() => {
    if (!file) return;
    void importDataset({
      file,
      name: getValues().name ?? suggestedName,
      description: getValues().description,
      folderId: selectedFolder?.id,
      projectId,
      ...(isTabular && { hasHeader }),
      ...(isTabular && sheet ? { sheetName: sheet } : {}),
    });
    reset();
    // Also refreshed on job completion (`useJobStatus`, wired by the Content
    // page) — this one is for anything the request already committed before
    // the dialog closed, so the feed is never behind what the server already
    // has by the time this call returns.
    refreshContentFeed();
    onDone?.();
  }, [
    file,
    getValues,
    suggestedName,
    selectedFolder?.id,
    projectId,
    isTabular,
    hasHeader,
    sheet,
    importDataset,
    reset,
    onDone,
  ]);

  /**
   * What the host's primary control says and does. It states its own
   * blocking condition, so no host has to know the rules.
   */
  const setName = useCallback(
    (value: string) => setValue("name", value, { shouldValidate: true }),
    [setValue]
  );
  const setDescription = useCallback(
    (value: string) => setValue("description", value, { shouldValidate: true }),
    [setValue]
  );

  /**
   * Take the file the host arrived with, once.
   *
   * Through `setFile` rather than around it, so a dropped file gets the same validation, the
   * same suggested name and the same preview as one chosen in the dialog. Guarded by a ref:
   * removing the file must not make it reappear.
   */
  const seeded = useRef(false);
  useEffect(() => {
    if (!initialFile || seeded.current) return;
    seeded.current = true;
    setFile(initialFile);
  }, [initialFile, setFile]);

  const action = useMemo(
    () => ({
      label: t("upload"),
      // One screen, so one condition: a file, a name that validates, somewhere to put it.
      disabled: !file || !isValid || !selectedFolder,
      notice,
      run: submit,
    }),
    [file, isValid, selectedFolder, submit, t, notice]
  );

  return {
    action,
    isBusy,
    reset,
    upload: {
      file,
      fileError,
      setFile,
      acceptedFileTypes: ACCEPTED_FILE_TYPES,
      folders,
      selectedFolder,
      setSelectedFolder,
      bundleType,
      isTabular,
      preview,
      hasHeader,
      setHasHeader,
      sheet,
      setSheet,
      register,
      errors,
      suggestedName,
      values,
      setName,
      setDescription,
    },
  };
};
