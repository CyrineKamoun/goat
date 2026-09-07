import { renderHook } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

// `t` echoes its key plus a JSON dump of its options, so an interpolation
// (the space name, the joined names) is assertable without a real i18n setup.
vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    t: (key: string, options?: Record<string, unknown>) =>
      options ? `${key}${JSON.stringify(options)}` : key,
  }),
}));

const useProject = vi.fn();
vi.mock("@/lib/api/projects", () => ({
  useProject: (...args: unknown[]) => useProject(...args),
}));

const useTeams = vi.fn();
vi.mock("@/lib/api/teams", () => ({
  useTeams: (...args: unknown[]) => useTeams(...args),
}));

import { useShareNotice } from "@/hooks/addLayer/useShareNotice";

const project = (overrides: Record<string, unknown>) => ({
  id: "project-1",
  space_kind: "personal",
  space_name: null,
  shared_with: null,
  ...overrides,
});

describe("useShareNotice", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    useTeams.mockReturnValue({ teams: [], isLoading: false });
  });

  it("returns the space notice for a team project", () => {
    useProject.mockReturnValue({
      project: project({ space_kind: "team", space_name: "Mobility Team" }),
      isLoading: false,
    });

    const { result } = renderHook(() => useShareNotice("project-1"));

    expect(result.current).toBe(
      `add_layer_space_notice${JSON.stringify({ name: "Mobility Team" })}`
    );
  });

  it("returns the shared notice listing both teams for a personal project shared with two teams", () => {
    useProject.mockReturnValue({
      project: project({
        shared_with: {
          teams: [
            { id: "t1", name: "Alpha", role: "project-viewer" },
            { id: "t2", name: "Beta", role: "project-viewer" },
          ],
          organizations: [],
          users: [],
        },
      }),
      isLoading: false,
    });

    const { result } = renderHook(() => useShareNotice("project-1"));

    expect(result.current).toBe(
      `add_layer_shared_notice${JSON.stringify({ names: "Alpha, Beta" })}`
    );
  });

  it("returns undefined for an unshared personal project", () => {
    useProject.mockReturnValue({ project: project({}), isLoading: false });

    const { result } = renderHook(() => useShareNotice("project-1"));

    expect(result.current).toBeUndefined();
  });

  it("returns undefined while loading", () => {
    useProject.mockReturnValue({ project: undefined, isLoading: true });

    const { result } = renderHook(() => useShareNotice("project-1"));

    expect(result.current).toBeUndefined();
  });

  it("resolves a team name via useTeams when the shared_with entry carries only an id", () => {
    useTeams.mockReturnValue({
      teams: [{ id: "t1", name: "Alpha", role: "team-member" }],
      isLoading: false,
    });
    useProject.mockReturnValue({
      project: project({
        shared_with: { teams: [{ id: "t1", role: "project-viewer" }], organizations: [], users: [] },
      }),
      isLoading: false,
    });

    const { result } = renderHook(() => useShareNotice("project-1"));

    expect(result.current).toBe(
      `add_layer_shared_notice${JSON.stringify({ names: "Alpha" })}`
    );
  });

  it("caps names at three and counts the rest into +N", () => {
    useProject.mockReturnValue({
      project: project({
        shared_with: {
          teams: [
            { id: "t1", name: "Alpha", role: "project-viewer" },
            { id: "t2", name: "Beta", role: "project-viewer" },
          ],
          organizations: [{ id: "o1", role: "project-viewer" }],
          users: [
            { id: "u1", name: "Casey", role: "project-viewer" },
            { id: "u2", role: "project-viewer" },
          ],
        },
      }),
      isLoading: false,
    });

    const { result } = renderHook(() => useShareNotice("project-1"));

    expect(result.current).toBe(
      `add_layer_shared_notice${JSON.stringify({ names: "Alpha, Beta, organization +2" })}`
    );
  });
});
