import { renderHook } from "@testing-library/react";
import type { ReactNode } from "react";
import { describe, expect, it } from "vitest";

import {
  PublicProjectProvider,
  useIsPublicProject,
  usePublicProjectId,
} from "@/lib/providers/PublicProjectProvider";

const PROJECT_ID = "3e870eb8-9633-44e9-8a43-36a7290f1ced";

const publicTree = ({ children }: { children: ReactNode }) => (
  <PublicProjectProvider projectId={PROJECT_ID}>{children}</PublicProjectProvider>
);

describe("PublicProjectProvider", () => {
  it("hands the public project's id to widgets inside the tree", () => {
    const { result } = renderHook(() => usePublicProjectId(), { wrapper: publicTree });
    expect(result.current).toBe(PROJECT_ID);
  });

  it("marks the tree as public", () => {
    const { result } = renderHook(() => useIsPublicProject(), { wrapper: publicTree });
    expect(result.current).toBe(true);
  });

  it("is neither public nor has a project id outside the provider", () => {
    expect(renderHook(() => usePublicProjectId()).result.current).toBeNull();
    expect(renderHook(() => useIsPublicProject()).result.current).toBe(false);
  });
});
