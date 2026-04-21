import { z } from "zod";

export const interactionTriggerSchema = z.object({
  type: z.enum(["group_activated", "visibility_changed"]),
  sourceId: z.number().optional(),
});

export const interactionActionSchema = z.object({
  type: z.enum(["switch_tab", "sync_visibility"]),
  targetWidgetId: z.string().optional(),
  tabId: z.string().optional(),
});

export const interactionMappingSchema = z.object({
  sourceId: z.number(),
  actionParams: z.record(z.string()),
});

export const interactionRuleSchema = z.object({
  id: z.string(),
  name: z.string().default(""),
  enabled: z.boolean().default(true),
  trigger: interactionTriggerSchema,
  action: interactionActionSchema,
  mapping: z.array(interactionMappingSchema).default([]),
});

export type InteractionRule = z.infer<typeof interactionRuleSchema>;
export type InteractionMapping = z.infer<typeof interactionMappingSchema>;
export type InteractionTrigger = z.infer<typeof interactionTriggerSchema>;
export type InteractionAction = z.infer<typeof interactionActionSchema>;
