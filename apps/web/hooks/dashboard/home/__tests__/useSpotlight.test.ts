import { act, renderHook } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import type { UserPreferences } from "@/lib/validations/home";

import { useSpotlight } from "@/hooks/dashboard/home/useSpotlight";

const { useAppSelectorMock, usePreferencesMock, patchPreferencesMock, mutateMock } = vi.hoisted(() => ({
  useAppSelectorMock: vi.fn(),
  usePreferencesMock: vi.fn(),
  patchPreferencesMock: vi.fn(),
  mutateMock: vi.fn(),
}));

vi.mock("@/hooks/store/ContextHooks", () => ({ useAppSelector: useAppSelectorMock }));
vi.mock("@/lib/api/preferences", () => ({
  usePreferences: usePreferencesMock,
  patchPreferences: patchPreferencesMock,
}));

const preferences = (spotlight_seen: string[] = []): UserPreferences => ({
  onboarding_skipped_at: null,
  releases_seen_at: null,
  spotlight_seen,
});

const spotlightEntry = (overrides: Partial<Parameters<typeof useSpotlight>[0][number]> = {}) => ({
  id: "e1",
  date: "2026-09-04",
  tag: "new" as const,
  title: "Content Spaces",
  summary: "Spaces now own content.",
  url: "https://docs.plan4better.de/releases/2026-09-content-spaces",
  spotlight: { headline: "Content Spaces is here" },
  ...overrides,
});

beforeEach(() => {
  useAppSelectorMock.mockReset().mockReturnValue([]);
  usePreferencesMock.mockReset().mockReturnValue({ mutate: mutateMock });
  patchPreferencesMock.mockReset().mockResolvedValue({});
  mutateMock.mockReset();
  try {
    sessionStorage.clear();
  } catch {
    // sessionStorage not available in this environment; nothing to clear.
  }
});

afterEach(() => {
  vi.restoreAllMocks();
});

describe("useSpotlight", () => {
  it("returns no entry in the New stage even when a spotlight entry is unseen", () => {
    const { result } = renderHook(() => useSpotlight([spotlightEntry()], preferences(), "new"));

    expect(result.current.entry).toBeUndefined();
  });

  it("returns no entry while the stage is still loading (undefined)", () => {
    const { result } = renderHook(() => useSpotlight([spotlightEntry()], preferences(), undefined));

    expect(result.current.entry).toBeUndefined();
  });

  it("returns no entry once the entry's id is already in spotlight_seen", () => {
    const { result } = renderHook(() =>
      useSpotlight([spotlightEntry({ id: "e1" })], preferences(["e1"]), "getting_started")
    );

    expect(result.current.entry).toBeUndefined();
  });

  it("returns no entry while a job is running", () => {
    useAppSelectorMock.mockReturnValue(["job-1"]);

    const { result } = renderHook(() => useSpotlight([spotlightEntry()], preferences(), "getting_started"));

    expect(result.current.entry).toBeUndefined();
  });

  it("does not show while a file upload is still transferring", () => {
    useAppSelectorMock.mockImplementation((selector: (state: unknown) => unknown) =>
      selector({ jobs: { runningJobIds: [] }, uploads: { transfers: [{ id: "u1", status: "uploading" }] } })
    );
    const { result } = renderHook(() => useSpotlight([spotlightEntry()], preferences(), "established"));
    expect(result.current.entry).toBeUndefined();
  });

  it("returns the newest unseen spotlight entry once every rule clears", () => {
    const { result } = renderHook(() => useSpotlight([spotlightEntry()], preferences(), "getting_started"));

    expect(result.current.entry?.id).toBe("e1");
  });

  it("shows at most one entry per session: a second hook instance in the same session sees nothing", () => {
    renderHook(() => useSpotlight([spotlightEntry()], preferences(), "getting_started"));
    const { result } = renderHook(() => useSpotlight([spotlightEntry()], preferences(), "getting_started"));

    expect(result.current.entry).toBeUndefined();
  });

  it("dismiss patches spotlight_seen with the entry appended, then refreshes preferences", async () => {
    const { result } = renderHook(() =>
      useSpotlight([spotlightEntry()], preferences(["old-id"]), "getting_started")
    );
    expect(result.current.entry?.id).toBe("e1");

    await act(async () => {
      await result.current.dismiss();
    });

    expect(patchPreferencesMock).toHaveBeenCalledWith({ spotlight_seen: ["old-id", "e1"] });
    expect(mutateMock).toHaveBeenCalledTimes(1);
    expect(result.current.entry).toBeUndefined();
  });
});
