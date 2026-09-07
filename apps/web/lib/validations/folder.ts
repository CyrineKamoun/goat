import * as z from "zod";

export const folderSchema = z.object({
  name: z.string(),
  id: z.string().uuid(),
  user_id: z.string().uuid().nullable().optional(),
  parent_id: z.string().uuid().nullable().optional().default(null),
  space_id: z.string().uuid().optional(),
  depth: z.number().int().optional().default(0),
  is_owned: z.boolean().optional().default(true),
  role: z.string().optional().nullable(),
  shared_from_name: z.string().optional().nullable(),
  shared_with_ids: z.array(z.string().uuid()).optional().nullable(),
  restricted: z.boolean().optional().default(false),
});

export const folderResponse = z.array(folderSchema);

export const folderGrantSchema = z.object({
  grantee_type: z.enum(["team", "organization"]),
  grantee_id: z.string().uuid(),
  grantee_name: z.string(),
  role: z.enum(["folder-viewer", "folder-editor"]),
});

export const folderGrantsResponseSchema = z.object({
  grants: z.array(folderGrantSchema),
});

export const folderShareRoleEnum = z.enum(["folder-editor", "folder-viewer"]);

export type Folder = z.infer<typeof folderSchema>;
export type FolderResponse = z.infer<typeof folderResponse>;
export type FolderGrant = z.infer<typeof folderGrantSchema>;
export type FolderGrantsResponse = z.infer<typeof folderGrantsResponseSchema>;

export interface FolderSharePayload {
  grantee_type: "team" | "organization";
  grantee_id: string;
  role: "folder-viewer" | "folder-editor";
}
