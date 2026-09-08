import * as z from "zod";

/** Home's cross-device UI state, carried on the existing per-user
 * `GET/PUT /api/v2/system/settings` row (H10) via `lib/api/preferences.ts`. */
export const userPreferencesSchema = z.object({
  onboarding_skipped_at: z.string().nullable(),
  releases_seen_at: z.string().nullable(),
  spotlight_seen: z.array(z.string()).default([]),
});
export type UserPreferences = z.infer<typeof userPreferencesSchema>;
export type UserPreferencesUpdate = Partial<UserPreferences>;

/** `GET /api/v2/users/me/onboarding` — which "getting started" steps are done. */
export const onboardingFactsSchema = z.object({
  has_project: z.boolean(),
  has_uploaded_layer: z.boolean(),
  has_catalog_layer: z.boolean(),
  has_team: z.boolean(),
  /** T10: any workflow exists in a project the caller created. */
  has_workflow: z.boolean(),
});
export type OnboardingFacts = z.infer<typeof onboardingFactsSchema>;

export const releaseTag = z.enum(["new", "improved", "fixed"]);
export const releaseEntrySchema = z.object({
  id: z.string(),
  date: z.string(),
  tag: releaseTag,
  title: z.string(),
  summary: z.string(),
  url: z.string().url(),
  spotlight: z
    .object({
      headline: z.string(),
      media: z.string().url().optional(),
      cta: z.object({ label: z.string(), url: z.string().url() }).optional(),
    })
    .optional(),
});
/** The feed carries both locales; the app picks its own. */
export const releasesFeedSchema = z.object({
  generatedAt: z.string(),
  entries: z.record(z.enum(["en", "de"]), z.array(releaseEntrySchema)),
});

export type ReleaseTag = z.infer<typeof releaseTag>;
export type ReleaseEntry = z.infer<typeof releaseEntrySchema>;
export type ReleasesFeed = z.infer<typeof releasesFeedSchema>;

export const statusLevel = z.enum(["operational", "notice", "maintenance", "disrupted", "outage"]);

/**
 * `title` and `latestUpdate.body` on an incident are either a plain string
 * or an `{en, de}` record. `localized` below picks the right half of
 * whichever shape arrives.
 */
export const localizedText = z.union([z.string(), z.record(z.string(), z.string())]);
export type LocalizedText = z.infer<typeof localizedText>;

export const statusFeedSchema = z.object({
  generatedAt: z.string(),
  overall: statusLevel,
  url: z.string().url(),
  systems: z.array(z.object({ id: z.string(), name: z.record(z.string(), z.string()), status: statusLevel })),
  incidents: z.array(
    z.object({
      id: z.string(),
      severity: z.string(),
      phase: z.string(),
      title: localizedText,
      startedAt: z.string(),
      affected: z.array(z.string()).default([]),
      latestUpdate: z.object({ at: z.string(), body: localizedText }),
    })
  ),
});
export type StatusLevel = z.infer<typeof statusLevel>;
export type StatusFeed = z.infer<typeof statusFeedSchema>;

/** Pick one locale out of a value that is either already a plain string or a `{en, de}` record. */
export const localized = (value: LocalizedText, locale: "en" | "de"): string =>
  typeof value === "string" ? value : (value[locale] ?? value.en ?? Object.values(value)[0] ?? "");
