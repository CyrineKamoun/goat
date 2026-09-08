"use client";

import { useCallback, useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { toast } from "react-toastify";

import { useProject } from "@/lib/api/projects";

import { useDatasetImport } from "@/hooks/addLayer/useDatasetImport";

import AddLayerDialog from "@/components/addLayer/AddLayerDialog";

/**
 * Formats that need an answer before they can be imported: which row is the header, and
 * which worksheet. Guessing produces a plausible, wrong layer, so these open the dialog.
 */
const NEEDS_SETUP = ["csv", "xlsx", "xls"];

/** Formats that carry their own geometry and fields, so there is nothing to ask. */
const IMPORTS_DIRECTLY = ["gpkg", "geojson", "kml", "zip", "parquet"];

const extensionOf = (file: File): string => file.name.split(".").pop()?.toLowerCase() ?? "";

/**
 * Drop a file anywhere on the editor to import it.
 *
 * Nothing is drawn while dragging. The window takes the drop directly, and what tells you it
 * worked is the transfer banner appearing — an editor-sized panel lighting up green under the
 * cursor is a lot of screen for a message that arrives a moment later anyway.
 *
 * One file at a time, matching the upload dialog: several are refused rather than importing
 * whichever the browser happened to list first.
 */
const MapDropTarget = ({ projectId }: { projectId: string }) => {
  const { t } = useTranslation("common");
  const { importDataset } = useDatasetImport();
  const { project } = useProject(projectId);
  const [needsSetup, setNeedsSetup] = useState<File | null>(null);

  const take = useCallback(
    (files: FileList | null) => {
      const dropped = Array.from(files ?? []);
      if (dropped.length === 0) return;
      if (dropped.length > 1) {
        toast.warning(t("drop_one_file_only"));
        return;
      }

      const file = dropped[0];
      const extension = extensionOf(file);

      if (NEEDS_SETUP.includes(extension)) {
        setNeedsSetup(file);
        return;
      }
      if (!IMPORTS_DIRECTLY.includes(extension)) {
        toast.error(t("drop_unsupported_file"));
        return;
      }

      void importDataset({
        file,
        // The filename without its extension, as the dialog would have suggested.
        name: file.name.replace(/\.[^/.]+$/, ""),
        folderId: project?.folder_id,
        projectId,
      });
    },
    [importDataset, project?.folder_id, projectId, t]
  );

  useEffect(() => {
    const carriesFiles = (event: DragEvent) =>
      Array.from(event.dataTransfer?.types ?? []).includes("Files");

    // `dragover` must be prevented for a drop to be allowed at all, and only for files —
    // otherwise this swallows the panel's own drag-to-reorder.
    const onDragOver = (event: DragEvent) => {
      if (carriesFiles(event)) event.preventDefault();
    };
    const onDrop = (event: DragEvent) => {
      if (!carriesFiles(event)) return;
      // Without this the browser navigates to the file it was handed.
      event.preventDefault();
      take(event.dataTransfer?.files ?? null);
    };

    window.addEventListener("dragover", onDragOver);
    window.addEventListener("drop", onDrop);
    return () => {
      window.removeEventListener("dragover", onDragOver);
      window.removeEventListener("drop", onDrop);
    };
  }, [take]);

  // A spreadsheet has not been uploaded: it is waiting for an answer, with its column setup
  // already open, since there is one file and one thing to configure.
  if (!needsSetup) return null;
  return (
    <AddLayerDialog
      source="upload"
      projectId={projectId}
      defaultFolderId={project?.folder_id}
      initialFile={needsSetup}
      autoOpenSetup
      onClose={() => setNeedsSetup(null)}
    />
  );
};

export default MapDropTarget;
