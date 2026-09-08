import { beforeEach, describe, expect, it, vi } from "vitest";

import { patchPreferences, usePreferences } from "@/lib/api/preferences";

const { updateSystemSettingsMock, useSystemSettingsMock } = vi.hoisted(() => ({
  updateSystemSettingsMock: vi.fn(),
  useSystemSettingsMock: vi.fn(),
}));
vi.mock("@/lib/api/system/client", () => ({
  useSystemSettings: useSystemSettingsMock,
  updateSystemSettings: updateSystemSettingsMock,
}));

describe("preferences api", () => {
  beforeEach(() => {
    updateSystemSettingsMock.mockReset();
    useSystemSettingsMock.mockReset();
  });

  it("patchPreferences sends exactly the given keys to updateSystemSettings", async () => {
    updateSystemSettingsMock.mockResolvedValue({
      onboarding_skipped_at: null,
      releases_seen_at: null,
      spotlight_seen: ["x"],
    });

    await patchPreferences({ spotlight_seen: ["x"] });

    expect(updateSystemSettingsMock).toHaveBeenCalledTimes(1);
    expect(updateSystemSettingsMock).toHaveBeenCalledWith({ spotlight_seen: ["x"] });
  });

  it("usePreferences reads the three Home keys off the settings row", () => {
    useSystemSettingsMock.mockReturnValue({
      systemSettings: {
        client_theme: "dark",
        preferred_language: "de",
        unit: "metric",
        onboarding_skipped_at: "2026-09-04T10:00:00Z",
        releases_seen_at: null,
        spotlight_seen: ["a"],
      },
      isLoading: false,
      isError: undefined,
      mutate: vi.fn(),
    });

    const { preferences } = usePreferences();

    expect(preferences).toEqual({
      onboarding_skipped_at: "2026-09-04T10:00:00Z",
      releases_seen_at: null,
      spotlight_seen: ["a"],
    });
  });

  it("usePreferences reports undefined preferences while the settings row hasn't loaded", () => {
    useSystemSettingsMock.mockReturnValue({
      systemSettings: undefined,
      isLoading: true,
      isError: undefined,
      mutate: vi.fn(),
    });

    expect(usePreferences().preferences).toBeUndefined();
  });
});
