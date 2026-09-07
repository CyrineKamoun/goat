"use client";

import { useSearchParams } from "next/navigation";
import { useEffect } from "react";

import { useReportLayouts } from "@/lib/api/reportLayouts";
import { useWorkflows } from "@/lib/api/workflows";
import type { MapMode } from "@/lib/store/map/slice";
import { setMapMode } from "@/lib/store/map/slice";
import { selectWorkflow, setWorkflows } from "@/lib/store/workflow/slice";

import { useAppDispatch } from "@/hooks/store/ContextHooks";

const URL_MAP_MODES: readonly MapMode[] = ["workflows", "reports", "builder"];

const isUrlMapMode = (value: string): value is MapMode =>
  (URL_MAP_MODES as readonly string[]).includes(value);

export interface MapUrlIntent {
  /** The `?layout=<id>` param, handed to the caller so it can thread it down
   * to the Reports panel (which owns its own selection — there is no Redux
   * action to dispatch here, see below). Read live from `useSearchParams`,
   * which Next feeds the native history methods back into, so it turns null
   * once this hook strips the param — by then the Reports panel's own effect
   * has already consumed it (a child effect runs before this parent one). */
  layoutId: string | null;
}

/**
 * Reads `?mode=workflows|reports|builder` plus `?workflow=<id>` / `?layout=<id>`
 * once on mount so a template's result (`templateResultHref` in
 * hooks/templates/useUseTemplate.ts) can land the map page on the right panel
 * and selection, then strips the handled params (via `history.replaceState`,
 * same as `useContentPageState` — no extra navigation/re-render) so a
 * refresh or back button doesn't replay them. Any other query params (e.g.
 * `?loc=`) are left untouched.
 *
 * Workflow selection dispatches the same Redux action the Workflows panel
 * uses (`selectWorkflow`), once this project's workflow list actually
 * contains the id. `selectWorkflow`'s reducer resolves nodes/edges from
 * `state.workflows` *synchronously at dispatch time* — it never re-derives
 * them later — so if `WorkflowsLayout`'s own `setWorkflows` dispatch (a
 * different, separately-mounted component) hasn't populated that array yet,
 * the canvas would load empty and stay that way. To make this deterministic
 * rather than relying on `WorkflowsLayout` happening to mount first, this
 * hook dispatches `setWorkflows(workflows)` with its own freshly-loaded list
 * immediately before `selectWorkflow` — `WorkflowsLayout`'s later dispatch of
 * the same data is then a harmless no-op replace.
 *
 * Layout selection has no such action — the Reports panel keeps its
 * selection in local component state, not Redux (see
 * components/reports/panels/ReportsConfigPanel.tsx's `selectedReportId`).
 * This hook only exposes the parsed `layoutId` (see `MapUrlIntent`) for the
 * caller to thread down as a prop, and waits for the layout list to load
 * before stripping `?layout=` (so it doesn't disappear before anyone had a
 * chance to use it) — it does not select anything itself.
 */
export const useMapUrlIntent = (projectId: string | undefined): MapUrlIntent => {
  const dispatch = useAppDispatch();
  const searchParams = useSearchParams();

  const mode = searchParams.get("mode");
  const workflowId = searchParams.get("workflow");
  const layoutId = searchParams.get("layout");

  // Only fetched when there is actually an id to resolve, so a plain map
  // load (the common case, no `?workflow=`/`?layout=`) triggers neither.
  const { workflows } = useWorkflows(workflowId ? projectId : undefined);
  const { reportLayouts } = useReportLayouts(layoutId ? projectId : undefined);

  useEffect(() => {
    if (!projectId || (!mode && !workflowId && !layoutId)) return;

    if (mode && isUrlMapMode(mode)) {
      dispatch(setMapMode(mode));
    }

    if (workflowId) {
      // The list hasn't loaded yet — bail without stripping so this effect
      // retries once it (or a later render with the same params) arrives.
      if (!workflows) return;
      if (workflows.some((workflow) => workflow.id === workflowId)) {
        // Populate state.workflows from this hook's own fresh list first —
        // selectWorkflow reads it synchronously, so this must happen before
        // it, not rely on WorkflowsLayout's separate mount to have done so.
        dispatch(setWorkflows(workflows));
        dispatch(selectWorkflow(workflowId));
      }
    }

    if (layoutId && !reportLayouts) return;

    const next = new URLSearchParams(searchParams.toString());
    next.delete("mode");
    next.delete("workflow");
    next.delete("layout");
    const query = next.toString();
    const path = window.location.pathname;
    window.history.replaceState(null, "", query ? `${path}?${query}` : path);
  }, [projectId, mode, workflowId, layoutId, workflows, reportLayouts, dispatch, searchParams]);

  return { layoutId };
};
