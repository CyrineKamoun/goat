import * as z from "zod";

export const spaceKind = z.enum(["personal", "team", "organization"]);
export const spaceDefaultRole = z.enum(["viewer", "editor"]);
export const contentRole = z.enum(["owner", "editor", "viewer"]);
export const contentType = z.enum(["layer", "project", "folder", "bundle", "template"]);
export const contentView = z.enum(["space", "shared_with_me", "shared_with_space", "recent"]);

export const spaceSchema = z.object({
  id: z.string().uuid(),
  kind: spaceKind,
  name: z.string(),
  default_role: spaceDefaultRole,
  my_role: contentRole.nullable().optional(),
  team_id: z.string().uuid().nullable().optional(),
  organization_id: z.string().uuid().nullable().optional(),
});

export const shareEntrySchema = z.object({
  role: z.string(),
  id: z.string().uuid(),
  name: z.string().nullable().optional(),
  avatar: z.string().nullable().optional(),
});

export const sharedWithSchema = z.object({
  teams: z.array(shareEntrySchema).default([]),
  organizations: z.array(shareEntrySchema).default([]),
  users: z.array(shareEntrySchema).default([]),
});

/** Who created a feed row — the content table's `user_id`, resolved to a
 * display name and picture. Null once the creating account is gone. */
export const contentCreatorSchema = z.object({
  id: z.string().uuid(),
  name: z.string(),
  avatar: z.string().nullable().optional(),
});

export const contentItemSchema = z.object({
  type: contentType,
  id: z.string().uuid(),
  name: z.string(),
  space_id: z.string().uuid(),
  folder_id: z.string().uuid().nullable().optional(),
  updated_at: z.string(),
  created_at: z.string(),
  my_role: contentRole,
  created_by: contentCreatorSchema.nullable().optional(),
  shared_with: sharedWithSchema.nullable().default(null),
  thumbnail_url: z.string().nullable().optional(),
  /** A published public snapshot exists — projects only. */
  is_public: z.boolean().default(false),
  layer_type: z.string().nullable().optional(),
  feature_layer_geometry_type: z.string().nullable().optional(),
  is_shortcut: z.boolean().default(false),
  restricted: z.boolean().default(false),
  restricted_inherited: z.boolean().default(false),
  /** `type: "template"` rows only: what the template's config carries (T1). */
  template_payload_kind: z.enum(["workflow", "layout", "project"]).nullable().optional(),
  /** `type: "template"` rows only: the kinds shown on the card (T1). Left
   * optional (rather than defaulted) so the countless existing `ContentItem`
   * fixtures across the codebase — folders, projects, layers, bundles, none
   * of which ever carry it — do not all need updating for a field that is
   * only ever populated on a template row. Read as `item.template_kinds ?? []`. */
  template_kinds: z.array(z.enum(["workflow", "dashboard", "layout"])).optional(),
  /** `type: "template"` rows only: the GOAT catalog shelf state (T4). */
  template_catalog_status: z.enum(["none", "proposed", "published", "declined"]).nullable().optional(),
  /** `type: "template"` rows only: true once any declared input is a
   * shipped, catalog-origin dataset (T5). Optional for the same reason as
   * `template_kinds` above — read as `item.template_ships_sample_data ?? false`. */
  template_ships_sample_data: z.boolean().optional(),
});

export const contentPageSchema = z.object({
  items: z.array(contentItemSchema),
  total: z.number().int(),
  page: z.number().int(),
  size: z.number().int(),
});

export const contentQueryParamsSchema = z.object({
  view: contentView,
  space_id: z.string().uuid().optional(),
  folder_id: z.string().uuid().optional(),
  types: z.string().optional(),
  search: z.string().optional(),
  order_by: z.enum(["updated_at", "created_at", "name", "last_opened_at"]).optional(),
  order: z.enum(["ascendent", "descendent"]).optional(),
  page: z.number().int().positive().optional(),
  size: z.number().int().positive().optional(),
});

export const trashItemSchema = z.object({
  type: contentType,
  id: z.string().uuid(),
  name: z.string(),
  deleted_at: z.string(),
  purge_after: z.string(),
});

export const contentRefSchema = z.object({ type: contentType, id: z.string() });

export const transferPreviewRequestSchema = z.object({
  items: z.array(contentRefSchema),
  target_space_id: z.string(),
});

export const transferRequestSchema = transferPreviewRequestSchema.extend({
  dataset_ids: z.array(z.string()),
  leave_shortcut: z.boolean(),
});

export const transferPreviewSchema = z.object({
  items: z.array(contentRefSchema.extend({ name: z.string() })),
  datasets: z.array(
    z.object({ id: z.string(), name: z.string(), owned: z.boolean(), used_elsewhere: z.boolean() })
  ),
  grants_to_drop: z.number().int(),
  folders_in_subtrees: z.number().int(),
  trashed_in_subtrees: z.number().int(),
  skipped_foreign: z.number().int().default(0),
  name_collisions: z.array(z.string()).default([]),
  warnings: z.array(z.string()),
});

export const transferResultSchema = z.object({
  moved: z.object({
    folder: z.number().int().default(0),
    project: z.number().int().default(0),
    layer: z.number().int().default(0),
    bundle: z.number().int().default(0),
  }),
  shortcuts: z.number().int(),
  trashed_moved: z.number().int(),
});

/** `GET /space/{space_id}/usage` (D13, members only): total size and count
 * of the space's live layers, and count of its live projects. */
export const spaceUsageSchema = z.object({
  space_id: z.string().uuid(),
  bytes: z.number().int(),
  layers: z.number().int(),
  projects: z.number().int(),
});

export type SpaceKind = z.infer<typeof spaceKind>;
export type SpaceDefaultRole = z.infer<typeof spaceDefaultRole>;
export type ContentRole = z.infer<typeof contentRole>;
export type ContentType = z.infer<typeof contentType>;
export type ContentView = z.infer<typeof contentView>;
export type Space = z.infer<typeof spaceSchema>;
export type ShareEntry = z.infer<typeof shareEntrySchema>;
export type SharedWith = z.infer<typeof sharedWithSchema>;
export type ContentCreator = z.infer<typeof contentCreatorSchema>;
export type ContentItem = z.infer<typeof contentItemSchema>;
export type ContentPage = z.infer<typeof contentPageSchema>;
export type ContentQueryParams = z.infer<typeof contentQueryParamsSchema>;
export type TrashItem = z.infer<typeof trashItemSchema>;
export type ContentRef = z.infer<typeof contentRefSchema>;
export type TransferPreviewRequest = z.infer<typeof transferPreviewRequestSchema>;
export type TransferRequest = z.infer<typeof transferRequestSchema>;
export type TransferPreview = z.infer<typeof transferPreviewSchema>;
export type TransferResult = z.infer<typeof transferResultSchema>;
export type SpaceUsage = z.infer<typeof spaceUsageSchema>;
