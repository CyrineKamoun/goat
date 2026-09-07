import { useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { mutate } from "swr";

import { refreshContentFeed, useSpaces } from "@/lib/api/content";
import { useFolders } from "@/lib/api/folders";
import { useProjectLayers } from "@/lib/api/projects";
import { applyTemplate, refreshTemplates } from "@/lib/api/templates";
import { USERS_API_BASE_URL } from "@/lib/api/users";
import { homeFolderOf } from "@/lib/utils/content";
import type { Space } from "@/lib/validations/content";
import type { Folder } from "@/lib/validations/folder";
import type { ProjectLayer } from "@/lib/validations/project";
import type {
  TemplateInput,
  TemplateRead,
  TemplateUseRequest,
  TemplateUseResult,
} from "@/lib/validations/template";

/** Where "Use template" was opened from (T7): a panel already sitting inside
 * a project passes `in_project` and the payload is inserted there; every
 * other entry point (Home, Content, Catalog) passes `new_project`, which
 * asks for a location first. */
export type UseTemplateContext = { kind: "in_project"; projectId: string } | { kind: "new_project" };

/** The flow's own steps, in order — `location` only exists for
 * `new_project`, `bindings` only when the template has at least one `ask`
 * input to fill in. Either, both, or neither can apply to a given template
 * and context. */
export type UseTemplateStep = "location" | "bindings";

/** `POST /template/{id}/use`'s target, once resolved by this hook's state —
 * exactly one of `project_id`/`target_folder_id`, matching what
 * `crud_template.use` requires. */
const buildRequest = (
  template: TemplateRead,
  context: UseTemplateContext,
  name: string,
  targetFolderId: string | undefined,
  bindings: Record<string, string>
): TemplateUseRequest =>
  context.kind === "in_project"
    ? { project_id: context.projectId, bindings }
    : { target_folder_id: targetFolderId, name: name.trim() || template.name, bindings };

/** Same SWR key `useOnboardingFacts` reads (see `useHomeCreate`) — using a
 * template can complete "Run your first analysis"/"Build a workflow" (T10),
 * so the checklist should not wait for its own poll. */
const revalidateOnboardingFacts = () => mutate(`${USERS_API_BASE_URL}/me/onboarding`);

/**
 * Drives `UseTemplateFlow`'s state machine (T7, "Using a template"): the
 * location step's space/folder/name (new_project only), the bindings step's
 * per-input layer choice (in_project only — a fresh project has no layers of
 * its own yet, so its ask inputs can only be left for later), and the final
 * apply call. Steps that don't apply to this template/context (no location
 * needed, no ask inputs) simply aren't part of `steps`; the component reads
 * that to size its step indicator.
 */
export const useUseTemplate = (template: TemplateRead, context: UseTemplateContext) => {
  const { t } = useTranslation("common");
  const { spaces } = useSpaces();
  const { folders } = useFolders({});
  const { layers: projectLayers } = useProjectLayers(
    context.kind === "in_project" ? context.projectId : undefined
  );

  const [name, setName] = useState(template.name);
  const [spaceOverride, setSpaceOverride] = useState<Space | null>(null);
  const [folderOverride, setFolderOverride] = useState<Folder | null>(null);
  const [bindings, setBindings] = useState<Record<string, string>>({});
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const personalSpace = spaces.find((space) => space.kind === "personal") ?? null;
  const selectedSpace = spaceOverride ?? personalSpace;

  // Picking a different space invalidates any folder pick made under the
  // previous one.
  const setSelectedSpace = (space: Space | null) => {
    setSpaceOverride(space);
    setFolderOverride(null);
  };

  const spaceFolders = useMemo(
    () => (folders ?? []).filter((folder) => folder.space_id === selectedSpace?.id),
    [folders, selectedSpace]
  );
  const defaultFolder = selectedSpace ? (homeFolderOf(folders ?? [], selectedSpace.id) ?? null) : null;
  const selectedFolder = folderOverride ?? defaultFolder;
  const setSelectedFolder = (folder: Folder | null) => setFolderOverride(folder);

  const askInputs = useMemo(() => template.inputs.filter((input) => input.mode === "ask"), [template.inputs]);

  const setBinding = (key: string, layerId: string | null) => {
    setBindings((prev) => {
      if (layerId === null) {
        const next = { ...prev };
        delete next[key];
        return next;
      }
      return { ...prev, [key]: layerId };
    });
  };

  /** The project layers a given ask input can be bound to — filtered by the
   * input's own `layer_type`/`geometry_type` when it declares one. Empty in
   * `new_project` (there is no project yet), so the row offers only
   * "Decide later". */
  const candidatesFor = (input: TemplateInput): ProjectLayer[] => {
    if (context.kind !== "in_project") return [];
    return (projectLayers ?? []).filter(
      (layer) =>
        (!input.layer_type || layer.type === input.layer_type) &&
        (!input.geometry_type || layer.feature_layer_geometry_type === input.geometry_type)
    );
  };

  const steps = useMemo(() => {
    const list: UseTemplateStep[] = [];
    if (context.kind === "new_project") list.push("location");
    if (askInputs.length > 0) list.push("bindings");
    return list;
  }, [context.kind, askInputs.length]);

  const locationValid = context.kind !== "new_project" || (!!selectedFolder && name.trim().length > 0);

  const apply = async (): Promise<TemplateUseResult | null> => {
    setBusy(true);
    setError(null);
    try {
      const body = buildRequest(template, context, name, selectedFolder?.id, bindings);
      const result = await applyTemplate(template.id, body);
      refreshTemplates();
      refreshContentFeed();
      void revalidateOnboardingFacts();
      return result;
    } catch (applyError) {
      setError(applyError instanceof Error ? applyError.message : t("error_using_template"));
      return null;
    } finally {
      setBusy(false);
    }
  };

  return {
    steps,
    name,
    setName,
    spaces,
    selectedSpace,
    setSelectedSpace,
    folders: spaceFolders,
    selectedFolder,
    setSelectedFolder,
    askInputs,
    candidatesFor,
    bindings,
    setBinding,
    locationValid,
    busy,
    error,
    apply,
  };
};

/**
 * Where a caller should navigate once `onDone` fires. `map/[projectId]/page`
 * reads `?mode=`/`?workflow=`/`?layout=` on mount via `useMapUrlIntent`
 * (hooks/map/useMapUrlIntent.ts), which dispatches the matching mapMode and,
 * for a workflow, selects it once the project's workflow list contains the
 * id, then strips the params. A layout payload only gets the mode switch for
 * now — the Reports panel keeps its own selection in local component state,
 * not Redux, so there is no action yet for a caller outside it to select one
 * by id (see that hook's doc comment). A project payload has no workflow/
 * layout of its own to open, so it lands on the plain project route.
 */
export const templateResultHref = (_template: TemplateRead, result: TemplateUseResult): string => {
  if (result.workflow_id) {
    return `/map/${result.project_id}?mode=workflows&workflow=${result.workflow_id}`;
  }
  if (result.layout_id) {
    return `/map/${result.project_id}?mode=reports&layout=${result.layout_id}`;
  }
  return `/map/${result.project_id}`;
};
