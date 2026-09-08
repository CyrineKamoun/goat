import * as z from "zod";

import { contentMetadataSchema } from "@/lib/validations/common";

/**
 * Editable bundle metadata. The layer vocabulary restricted to fields that
 * describe a whole acquisition (a GTFS feed, an Overture extract) rather than
 * one member layer — no per-layer accuracy or geometry-specific fields.
 */
export const bundleMetadataSchema = contentMetadataSchema.extend({
  lineage: z.string().optional(),
  geographical_code: z.string().length(2).optional(),
  data_reference_year: z.coerce.number().optional(),
  distributor_name: z.string().optional(),
  distributor_email: z.string().email().optional(),
  distribution_url: z.string().url().optional(),
  // Free text: the licence as the source states it (`DL-DE-BY-2.0`), not a
  // code from a list of ours.
  license: z.string().optional(),
  attribution: z.string().optional(),
});

export type BundleMetadata = z.infer<typeof bundleMetadataSchema>;

/** The keys that live inside a bundle's `dataset_metadata`, for packing a flat form into it. */
export const BUNDLE_METADATA_KEYS = [
  "lineage",
  "geographical_code",
  "data_reference_year",
  "distributor_name",
  "distributor_email",
  "distribution_url",
  "license",
  "attribution",
] as const;
