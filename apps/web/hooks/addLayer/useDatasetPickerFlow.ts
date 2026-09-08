import { useCallback, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { toast } from "react-toastify";
import { mutate } from "swr";

import {
  PROJECTS_API_BASE_URL,
  addBundleToProject,
  addProjectLayers,
  projectLayersKey,
} from "@/lib/api/projects";
import type { ContentItem } from "@/lib/validations/content";

import type { FlowController } from "@/hooks/addLayer/flow";
import { useShareNotice } from "@/hooks/addLayer/useShareNotice";

import type { DatasetPickerSelection } from "@/components/addLayer/DatasetPickerBody";

export type DatasetPickerFlow = FlowController & { selection: DatasetPickerSelection };

/**
 * Adding the user's own datasets to a project. Layers link directly, in one
 * request; a bundle becomes a locked group of its members, one request per
 * bundle. The dialog closes on click and the result reports itself by toast,
 * as the catalog flow does.
 */
export const useDatasetPickerFlow = ({
  projectId,
  onDone,
}: {
  projectId?: string;
  onDone?: () => void;
}): DatasetPickerFlow => {
  const { t } = useTranslation("common");
  const notice = useShareNotice(projectId);

  /**
   * The selection, keyed by id and carrying the item itself, so it outlives
   * the page it was picked from: searching, entering a folder or moving to
   * another space keeps everything already selected. A `Map` because it
   * remembers insertion order, which is the order the datasets were picked in.
   */
  const [selected, setSelected] = useState<Map<string, ContentItem>>(new Map());

  const toggle = useCallback((item: ContentItem) => {
    setSelected((current) => {
      const next = new Map(current);
      if (!next.delete(item.id)) next.set(item.id, item);
      return next;
    });
  }, []);
  const clear = useCallback(() => setSelected(new Map()), []);

  const ids = useMemo(() => Array.from(selected.keys()), [selected]);
  const items = useMemo(() => Array.from(selected.values()), [selected]);

  const selection = useMemo<DatasetPickerSelection>(
    () => ({ ids, items, toggle, clear }),
    [ids, items, toggle, clear]
  );

  const addSelection = useCallback(async () => {
    if (!projectId || items.length === 0) return;
    const layerIds = items.filter((item) => item.type === "layer").map((item) => item.id);
    const bundleIds = items.filter((item) => item.type === "bundle").map((item) => item.id);
    /** Each write with what it stands for, so a partial failure can still say
     * how much landed. */
    const writes = [
      ...(layerIds.length ? [{ count: layerIds.length, run: addProjectLayers(projectId, layerIds) }] : []),
      ...bundleIds.map((bundleId) => ({ count: 1, run: addBundleToProject(projectId, bundleId) })),
    ];
    clear();
    onDone?.();

    const results = await Promise.allSettled(writes.map((write) => write.run));
    // Whatever landed has to become visible, so the project's keys are
    // revalidated whether every write succeeded or only some did. A refetch
    // that fails must not hide the toasts below: the writes are what the
    // user is told about, and the tree re-polls on its own.
    await Promise.allSettled([
      mutate(projectLayersKey(projectId)),
      mutate([`${PROJECTS_API_BASE_URL}/${projectId}/group`]),
      mutate([`${PROJECTS_API_BASE_URL}/${projectId}`]),
    ]);

    const added = results.reduce(
      (sum, result, index) => sum + (result.status === "fulfilled" ? writes[index].count : 0),
      0
    );
    if (added > 0) toast.success(t("catalog_layers_added", { count: added }));

    // The dialog is already gone, so a toast is the only place a failure can
    // surface. One is enough: they all read the same either way.
    const failure = results.find((result): result is PromiseRejectedResult => result.status === "rejected");
    if (failure) {
      console.error(failure.reason);
      const detail = failure.reason instanceof Error ? failure.reason.message : "";
      toast.error(detail || t("error_adding_layer"));
    }
  }, [projectId, items, clear, onDone, t]);

  const action = useMemo(
    () => ({
      label: ids.length > 1 ? t("catalog_add_n_layers", { count: ids.length }) : t("add_layer"),
      disabled: !projectId || ids.length === 0,
      reason: !projectId
        ? t("add_needs_project")
        : ids.length === 0
          ? t("catalog_select_datasets_first")
          : undefined,
      notice,
      run: addSelection,
    }),
    [ids.length, t, projectId, addSelection, notice]
  );

  return { action, isBusy: false, reset: clear, selection };
};
