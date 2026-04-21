import { useEffect, useRef } from "react";

import type { InteractionRule } from "@/lib/validations/interaction";

import {
  clearPendingEvent,
  setActiveTabOverride,
  setSuppressEvents,
} from "@/lib/store/interaction/slice";

import { useAppDispatch, useAppSelector } from "@/hooks/store/ContextHooks";

interface UseInteractionDispatcherOptions {
  rules: InteractionRule[];
  onVisibilitySync?: (layerId: number, visible: boolean) => void;
}

export function useInteractionDispatcher({
  rules,
  onVisibilitySync,
}: UseInteractionDispatcherOptions) {
  const dispatch = useAppDispatch();
  const pendingEvent = useAppSelector((state) => state.interaction.pendingEvent);

  const onVisibilitySyncRef = useRef(onVisibilitySync);
  onVisibilitySyncRef.current = onVisibilitySync;

  useEffect(() => {
    if (!pendingEvent) return;

    const enabledRules = rules.filter((r) => r.enabled);

    for (const rule of enabledRules) {
      if (rule.trigger.type !== pendingEvent.type) continue;
      if (rule.trigger.sourceId && rule.trigger.sourceId !== pendingEvent.sourceId) continue;

      if (rule.action.type === "switch_tab" && rule.action.targetWidgetId) {
        const mapping = rule.mapping.find((m) => m.sourceId === pendingEvent.sourceId);
        const tabId = mapping?.actionParams?.tabId ?? rule.action.tabId;
        if (tabId) {
          dispatch(
            setActiveTabOverride({
              widgetId: rule.action.targetWidgetId,
              tabId,
            })
          );
        }
      }

      if (
        rule.action.type === "sync_visibility" &&
        pendingEvent.type === "visibility_changed" &&
        pendingEvent.value !== undefined
      ) {
        dispatch(setSuppressEvents(true));

        for (const mapping of rule.mapping) {
          if (mapping.sourceId !== pendingEvent.sourceId) {
            onVisibilitySyncRef.current?.(mapping.sourceId, pendingEvent.value);
          }
        }

        setTimeout(() => {
          dispatch(setSuppressEvents(false));
        }, 0);
      }
    }

    dispatch(clearPendingEvent());
  }, [pendingEvent, rules, dispatch]);
}
