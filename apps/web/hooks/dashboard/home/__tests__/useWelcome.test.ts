import { act, renderHook } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type { UserPreferences } from "@/lib/validations/home";

import { WELCOME_ID, useWelcome } from "@/hooks/dashboard/home/useWelcome";

const { usePreferencesMock, patchPreferencesMock, mutateMock } = vi.hoisted(() => ({
  usePreferencesMock: vi.fn(),
  patchPreferencesMock: vi.fn(),
  mutateMock: vi.fn(),
}));

vi.mock("@/lib/api/preferences", () => ({
  usePreferences: usePreferencesMock,
  patchPreferences: patchPreferencesMock,
}));

const preferences = (spotlight_seen: string[] = []): UserPreferences => ({
  onboarding_skipped_at: null,
  releases_seen_at: null,
  spotlight_seen,
});

beforeEach(() => {
  usePreferencesMock.mockReset().mockReturnValue({ mutate: mutateMock });
  patchPreferencesMock.mockReset().mockResolvedValue({});
  mutateMock.mockReset();
});

describe("useWelcome", () => {
  it("opens in the New stage for a caller who has never seen it, and records it as seen at once", async () => {
    const { result } = renderHook(() => useWelcome("new", preferences(["2026-09-content-spaces"])));

    expect(result.current.open).toBe(true);
    expect(patchPreferencesMock).toHaveBeenCalledWith({
      spotlight_seen: ["2026-09-content-spaces", WELCOME_ID],
    });
    await act(async () => {});
    expect(mutateMock).toHaveBeenCalledTimes(1);
  });

  it("stays closed once the caller has seen it", () => {
    const { result } = renderHook(() => useWelcome("new", preferences([WELCOME_ID])));

    expect(result.current.open).toBe(false);
    expect(patchPreferencesMock).not.toHaveBeenCalled();
  });

  it("stays closed outside the New stage and while the stage or preferences are still loading", () => {
    expect(renderHook(() => useWelcome("getting_started", preferences())).result.current.open).toBe(false);
    expect(renderHook(() => useWelcome("established", preferences())).result.current.open).toBe(false);
    expect(renderHook(() => useWelcome(undefined, preferences())).result.current.open).toBe(false);
    expect(renderHook(() => useWelcome("new", undefined)).result.current.open).toBe(false);
    expect(patchPreferencesMock).not.toHaveBeenCalled();
  });

  it("dismiss closes it without writing anything more", () => {
    const { result } = renderHook(() => useWelcome("new", preferences()));
    expect(result.current.open).toBe(true);

    act(() => result.current.dismiss());

    expect(result.current.open).toBe(false);
    expect(patchPreferencesMock).toHaveBeenCalledTimes(1);
  });

  it("does not reopen when the preferences refresh after the write", () => {
    const { result, rerender } = renderHook(({ prefs }) => useWelcome("new", prefs), {
      initialProps: { prefs: preferences() },
    });
    act(() => result.current.dismiss());

    rerender({ prefs: preferences([WELCOME_ID]) });

    expect(result.current.open).toBe(false);
  });

  it("preview opens it regardless of stage and history and records nothing", () => {
    const { result } = renderHook(() =>
      useWelcome("established", preferences([WELCOME_ID]), { preview: true })
    );

    expect(result.current.open).toBe(true);
    expect(patchPreferencesMock).not.toHaveBeenCalled();
  });

  it("stays closed and records nothing while suppressed", () => {
    const { result } = renderHook(() => useWelcome("new", preferences(), { suppress: true }));

    expect(result.current.open).toBe(false);
    expect(patchPreferencesMock).not.toHaveBeenCalled();
  });
});
