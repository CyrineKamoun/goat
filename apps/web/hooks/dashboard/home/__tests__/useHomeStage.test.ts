import { renderHook } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { SETUP_STEPS, runSetupStep, useHomeStage } from "@/hooks/dashboard/home/useHomeStage";

const {
  useOnboardingFactsMock,
  usePreferencesMock,
  patchPreferencesMock,
  mutateFactsMock,
  mutatePreferencesMock,
} = vi.hoisted(() => ({
  useOnboardingFactsMock: vi.fn(),
  usePreferencesMock: vi.fn(),
  patchPreferencesMock: vi.fn(),
  mutateFactsMock: vi.fn(),
  mutatePreferencesMock: vi.fn(),
}));

vi.mock("@/lib/api/onboarding", () => ({ useOnboardingFacts: useOnboardingFactsMock }));
vi.mock("@/lib/api/preferences", () => ({
  usePreferences: usePreferencesMock,
  patchPreferences: patchPreferencesMock,
}));

const facts = (overrides: Partial<Record<string, boolean>> = {}) => ({
  has_project: false,
  has_uploaded_layer: false,
  has_catalog_layer: false,
  has_team: false,
  has_workflow: false,
  ...overrides,
});

const setup = (
  factsValue: ReturnType<typeof facts>,
  preferencesValue: { onboarding_skipped_at: string | null }
) => {
  useOnboardingFactsMock.mockReturnValue({ facts: factsValue, isLoading: false, mutate: mutateFactsMock });
  usePreferencesMock.mockReturnValue({
    preferences: preferencesValue,
    isLoading: false,
    mutate: mutatePreferencesMock,
  });
};

describe("useHomeStage", () => {
  beforeEach(() => {
    patchPreferencesMock.mockReset().mockResolvedValue({});
    mutateFactsMock.mockReset();
    mutatePreferencesMock.mockReset();
  });

  it("has no stage yet while either request is still loading, so a caller cannot flash the wrong hero", () => {
    useOnboardingFactsMock.mockReturnValue({ facts: undefined, isLoading: true, mutate: mutateFactsMock });
    usePreferencesMock.mockReturnValue({
      preferences: undefined,
      isLoading: true,
      mutate: mutatePreferencesMock,
    });

    const { result } = renderHook(() => useHomeStage());

    expect(result.current.isLoading).toBe(true);
    expect(result.current.stage).toBeUndefined();
  });

  it("is new when neither a project nor any reachable dataset exists", () => {
    setup(facts(), { onboarding_skipped_at: null });

    const { result } = renderHook(() => useHomeStage());

    expect(result.current.stage).toBe("new");
    expect(result.current.done).toEqual([]);
    expect(result.current.skipped).toBe(false);
  });

  it("is established when the onboarding facts request errors, rather than stranding the caller on new", () => {
    useOnboardingFactsMock.mockReturnValue({
      facts: undefined,
      isLoading: false,
      isError: new Error("network error"),
      mutate: mutateFactsMock,
    });
    usePreferencesMock.mockReturnValue({
      preferences: { onboarding_skipped_at: null },
      isLoading: false,
      mutate: mutatePreferencesMock,
    });

    const { result } = renderHook(() => useHomeStage());

    expect(result.current.stage).toBe("established");
  });

  it("is getting_started once one fact is true and onboarding is neither done nor skipped", () => {
    setup(facts({ has_project: true }), { onboarding_skipped_at: null });

    const { result } = renderHook(() => useHomeStage());

    expect(result.current.stage).toBe("getting_started");
    expect(result.current.done).toEqual(["project"]);
  });

  it("is established once onboarding is skipped, even with steps still open", () => {
    setup(facts({ has_project: true }), { onboarding_skipped_at: "2026-09-01T00:00:00.000Z" });

    const { result } = renderHook(() => useHomeStage());

    expect(result.current.stage).toBe("established");
    expect(result.current.skipped).toBe(true);
  });

  it("is established once every setup step is done", () => {
    setup(
      facts({
        has_project: true,
        has_uploaded_layer: true,
        has_catalog_layer: true,
        has_team: true,
        has_workflow: true,
      }),
      { onboarding_skipped_at: null }
    );

    const { result } = renderHook(() => useHomeStage());

    expect(result.current.stage).toBe("established");
    expect(result.current.done).toEqual(SETUP_STEPS);
  });

  it("maps both template steps to has_workflow", () => {
    setup(facts({ has_workflow: true }), { onboarding_skipped_at: null });

    const { result } = renderHook(() => useHomeStage());

    expect(result.current.done).toEqual(["analysis", "workflow"]);
  });

  it("runSetupStep opens the browser with no kind for the analysis step", () => {
    const openTemplates = vi.fn();
    runSetupStep("analysis", {
      newProject: vi.fn(),
      addDataset: vi.fn(),
      browseCatalog: vi.fn(),
      goToTeam: vi.fn(),
      openTemplates,
    });

    expect(openTemplates).toHaveBeenCalledWith();
  });

  it("runSetupStep opens the browser locked to workflow for the workflow step", () => {
    const openTemplates = vi.fn();
    runSetupStep("workflow", {
      newProject: vi.fn(),
      addDataset: vi.fn(),
      browseCatalog: vi.fn(),
      goToTeam: vi.fn(),
      openTemplates,
    });

    expect(openTemplates).toHaveBeenCalledWith("workflow");
  });

  it("skip() patches onboarding_skipped_at with the current time, then refreshes preferences", async () => {
    setup(facts(), { onboarding_skipped_at: null });
    const { result } = renderHook(() => useHomeStage());

    const before = Date.now();
    await result.current.skip();
    const after = Date.now();

    expect(patchPreferencesMock).toHaveBeenCalledTimes(1);
    const [update] = patchPreferencesMock.mock.calls[0] as [{ onboarding_skipped_at: string }];
    expect(Object.keys(update)).toEqual(["onboarding_skipped_at"]);
    const patchedAt = new Date(update.onboarding_skipped_at).getTime();
    expect(patchedAt).toBeGreaterThanOrEqual(before);
    expect(patchedAt).toBeLessThanOrEqual(after);
    expect(mutatePreferencesMock).toHaveBeenCalledTimes(1);
  });
});
