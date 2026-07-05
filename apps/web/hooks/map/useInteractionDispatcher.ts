import { useEffect, useRef } from "react";

import type { InteractionRule } from "@/lib/validations/interaction";

import {
  clearPendingEvents,
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
  const pendingEvents = useAppSelector((state) => state.interaction.pendingEvents);

  const onVisibilitySyncRef = useRef(onVisibilitySync);
  onVisibilitySyncRef.current = onVisibilitySync;

  useEffect(() => {
    if (pendingEvents.length === 0) return;

    const enabledRules = rules.filter((r) => r.enabled);
    let didSync = false;

    // Process every queued event. The "Show all" toggle emits one event per
    // layer in a single tick, so all of them must be drained here — otherwise
    // linked (e.g. legend-hidden) layers stay out of sync with their source.
    for (const pendingEvent of pendingEvents) {
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
          // Suppress re-emission while we drive the synced layers so the
          // visibility writes below cannot feed events back into the queue.
          if (!didSync) {
            dispatch(setSuppressEvents(true));
            didSync = true;
          }

          for (const mapping of rule.mapping) {
            if (mapping.sourceId !== pendingEvent.sourceId) {
              onVisibilitySyncRef.current?.(mapping.sourceId, pendingEvent.value);
            }
          }
        }
      }
    }

    if (didSync) {
      setTimeout(() => {
        dispatch(setSuppressEvents(false));
      }, 0);
    }

    dispatch(clearPendingEvents());
  }, [pendingEvents, rules, dispatch]);
}
