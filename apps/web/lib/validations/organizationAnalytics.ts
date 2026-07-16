import * as z from "zod";

export const analyticsProviderSchema = z.enum(["matomo"]);

/** Mirrors the backend MatomoConfig validator. */
export const matomoConfigSchema = z.object({
  provider: z.literal("matomo").default("matomo"),
  url: z
    .string()
    .min(1)
    .transform((v) => v.trim())
    .pipe(z.string().url("Must be a valid URL"))
    .refine((v) => v.startsWith("https://"), "Must use https://")
    .refine((v) => {
      try {
        const u = new URL(v);
        return !u.search && !u.hash;
      } catch {
        return false;
      }
    }, "Must not contain a query or fragment")
    // A path is fine (self-hosted Matomo often lives under one, e.g.
    // https://host.de/matomo/); the tracker appends matomo.php/matomo.js,
    // so normalize to a trailing slash.
    .transform((v) => (v.endsWith("/") ? v : `${v}/`)),
  site_id: z
    .string()
    .min(1)
    .max(16)
    .regex(/^\d+$/, "Must be a numeric site ID"),
});

export type AnalyticsProvider = z.infer<typeof analyticsProviderSchema>;
export type MatomoConfig = z.infer<typeof matomoConfigSchema>;

/** Request body for POST/PUT /organizations/{org_id}/analytics. */
export const organizationAnalyticsCreateSchema = z.object({
  name: z.string().trim().min(1, "Name is required").max(120),
  provider: analyticsProviderSchema,
  config: matomoConfigSchema,
});

export type OrganizationAnalyticsCreate = z.infer<
  typeof organizationAnalyticsCreateSchema
>;

/** One item of GET /organizations/{org_id}/analytics. */
export const organizationAnalyticsSchema = z.object({
  id: z.string().uuid(),
  organization_id: z.string().uuid(),
  name: z.string(),
  provider: analyticsProviderSchema,
  config: matomoConfigSchema.partial({ provider: true }),
  usage_count: z.number().int().nonnegative().default(0),
  created_at: z.string(),
  updated_at: z.string(),
});

export type OrganizationAnalytics = z.infer<typeof organizationAnalyticsSchema>;

/** One published dashboard of the org with its analytics assignment. */
export const analyticsDashboardSchema = z.object({
  project_id: z.string().uuid(),
  name: z.string(),
  analytics_id: z.string().uuid().nullable(),
});

export type AnalyticsDashboard = z.infer<typeof analyticsDashboardSchema>;
