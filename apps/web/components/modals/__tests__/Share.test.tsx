import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import ShareModal from "@/components/modals/Share";

vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    t: (key: string, vars?: Record<string, unknown>) => (vars ? `${key}:${JSON.stringify(vars)}` : key),
  }),
}));
vi.mock("react-toastify", () => ({ toast: { success: vi.fn(), error: vi.fn() } }));
vi.mock("swr", () => ({ mutate: vi.fn() }));
vi.mock("@/lib/api/bundles", () => ({
  BUNDLES_API_BASE_URL: "/bundles",
  deleteBundleGrant: vi.fn(),
  isBundleTile: () => false,
  shareBundleGrant: vi.fn(),
  useBundleGrants: () => ({ data: undefined }),
}));
vi.mock("@/lib/api/folders", () => ({
  FOLDERS_API_BASE_URL: "/folders",
  deleteFolderGrant: vi.fn(),
  shareFolderGrant: vi.fn(),
  useFolderGrants: () => ({ data: undefined, error: undefined }),
}));
vi.mock("@/lib/api/datasets", () => ({ matchesContentListKey: vi.fn() }));
vi.mock("@/lib/api/projects", () => ({ PROJECTS_API_BASE_URL: "/projects" }));
vi.mock("@/lib/api/share", () => ({ shareLayer: vi.fn(), shareProject: vi.fn() }));
vi.mock("@/lib/api/teams", () => ({ useTeams: () => ({ teams: [] }) }));
vi.mock("@/lib/api/users", () => ({ useOrganization: () => ({ organization: undefined }) }));
vi.mock("@/components/modals/share/ShareWithPublicTab", () => ({ default: () => null }));

// eslint-disable-next-line @typescript-eslint/no-explicit-any
const layer = { id: "layer-1", name: "Roads", folder_id: "folder-1", shared_with: {} } as any;

describe("Share", () => {
  it("names what is being shared and offers the save", () => {
    render(<ShareModal open type="layer" content={layer} onClose={() => {}} />);

    expect(
      screen.getByText('manage_share_access_for_content:{"content_type":"layer","content_name":"Roads"}')
    ).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "save" })).toBeInTheDocument();
  });

  it("offers a public tab only for a project", () => {
    const { unmount } = render(<ShareModal open type="layer" content={layer} onClose={() => {}} />);
    expect(screen.queryByRole("tab", { name: "public" })).not.toBeInTheDocument();
    unmount();

    render(<ShareModal open type="project" content={layer} onClose={() => {}} />);
    expect(screen.getByRole("tab", { name: "public" })).toBeInTheDocument();
  });
});
