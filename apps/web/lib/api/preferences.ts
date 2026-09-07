import { updateSystemSettings, useSystemSettings } from "@/lib/api/system/client";
import type { UserPreferences, UserPreferencesUpdate } from "@/lib/validations/home";

/** Home's three cross-device UI-state keys — onboarding dismissal, the
 * release-notes "seen" watermark, and which spotlight cards were dismissed —
 * carried on the existing per-user `GET/PUT /api/v2/system/settings` row
 * (Home H10) instead of a table of their own. */
const toPreferences = (settings: {
  onboarding_skipped_at?: string | null;
  releases_seen_at?: string | null;
  spotlight_seen?: string[];
}): UserPreferences => ({
  onboarding_skipped_at: settings.onboarding_skipped_at ?? null,
  releases_seen_at: settings.releases_seen_at ?? null,
  spotlight_seen: settings.spotlight_seen ?? [],
});

export const usePreferences = () => {
  const { systemSettings, isLoading, isError, mutate } = useSystemSettings();
  return {
    preferences: systemSettings ? toPreferences(systemSettings) : undefined,
    isLoading,
    isError,
    mutate,
  };
};

export const patchPreferences = async (update: UserPreferencesUpdate): Promise<UserPreferences> => {
  const settings = await updateSystemSettings(update);
  if (!settings) throw new Error("Failed to update preferences");
  return toPreferences(settings);
};
