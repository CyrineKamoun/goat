import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type { Space } from "@/lib/validations/content";

import ContentBreadcrumb from "@/components/dashboard/content/ContentBreadcrumb";

vi.mock("react-i18next", () => ({
  useTranslation: () => ({ t: (key: string) => key }),
}));

const crumb = (props: Partial<React.ComponentProps<typeof ContentBreadcrumb>> = {}) => (
  <ContentBreadcrumb folders={[]} folderId={null} onNavigate={() => {}} {...props} />
);

describe("ContentBreadcrumb", () => {
  it("holds the space's place with a placeholder while it is unknown", () => {
    const { container } = render(crumb({ loading: true }));

    expect(container.querySelector(".MuiSkeleton-root")).not.toBeNull();
    expect(screen.queryByText("…")).not.toBeInTheDocument();
  });

  it("names the space once it is there", () => {
    const { container } = render(
      crumb({
        loading: true,
        space: {
          id: "00000000-0000-0000-0000-00000000000b",
          kind: "team",
          name: "Planning",
          default_role: "viewer",
        } as Space,
      })
    );

    expect(screen.getByText("Planning")).toBeInTheDocument();
    expect(container.querySelector(".MuiSkeleton-root")).toBeNull();
  });
});
