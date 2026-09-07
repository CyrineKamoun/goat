import { fireEvent, render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type { ContentItem, ContentPage, Space } from "@/lib/validations/content";

import JumpBackIn from "@/components/dashboard/home/JumpBackIn";

const { useContentMock, useSpacesMock, useFavoriteStarsMock, toggleStarMock, pushMock } = vi.hoisted(() => ({
  useContentMock: vi.fn(),
  useSpacesMock: vi.fn(),
  useFavoriteStarsMock: vi.fn(),
  toggleStarMock: vi.fn(),
  pushMock: vi.fn(),
}));

vi.mock("react-i18next", () => ({
  useTranslation: () => ({ t: (key: string) => key }),
}));
vi.mock("@/i18n/utils", () => ({ useDateFnsLocale: () => undefined }));
vi.mock("@/lib/api/users", () => ({
  useUserProfile: () => ({
    userProfile: { id: "u-me", firstname: "Marco", lastname: "Albrecht", avatar: "" },
  }),
}));
vi.mock("@/lib/api/content", () => ({
  useContent: (...args: unknown[]) => useContentMock(...args),
  useSpaces: () => useSpacesMock(),
}));
vi.mock("@/lib/api/favorites", () => ({
  useFavoriteStars: (...args: unknown[]) => useFavoriteStarsMock(...args),
}));
vi.mock("next/navigation", () => ({ useRouter: () => ({ push: pushMock }) }));

const space: Space = {
  id: "s1",
  kind: "personal",
  name: "My Content",
  default_role: "viewer",
  my_role: "owner",
  team_id: null,
  organization_id: null,
};

const projectItem = (overrides: Partial<ContentItem>): ContentItem => ({
  type: "project",
  id: "p1",
  name: "Project",
  space_id: "s1",
  folder_id: null,
  updated_at: new Date().toISOString(),
  created_at: new Date().toISOString(),
  my_role: "owner",
  created_by: null,
  shared_with: null,
  thumbnail_url: null,
  is_public: false,
  layer_type: null,
  feature_layer_geometry_type: null,
  is_shortcut: false,
  restricted: false,
  restricted_inherited: false,
  ...overrides,
});

const page = (items: ContentItem[]): ContentPage => ({ items, total: items.length, page: 1, size: 8 });

beforeEach(() => {
  useContentMock.mockReset();
  useSpacesMock.mockReset();
  useFavoriteStarsMock.mockReset();
  toggleStarMock.mockReset();
  pushMock.mockReset();
  useSpacesMock.mockReturnValue({ spaces: [space] });
  // jsdom has no matchMedia; JumpBackIn reads the mobile/desktop split with
  // MUI's useMediaQuery on every render.
  window.matchMedia =
    window.matchMedia ??
    ((query: string) =>
      ({
        matches: false,
        media: query,
        onchange: null,
        addListener: vi.fn(),
        removeListener: vi.fn(),
        addEventListener: vi.fn(),
        removeEventListener: vi.fn(),
        dispatchEvent: vi.fn(),
      }) as unknown as MediaQueryList);
});

describe("JumpBackIn", () => {
  it("renders the header with skeleton cards on first load, no cached page yet", () => {
    useContentMock.mockReturnValue({ page: undefined, isLoading: true });
    useFavoriteStarsMock.mockReturnValue({ starred: {}, toggleStar: toggleStarMock });

    const { container } = render(<JumpBackIn />);

    expect(screen.getByText("jump_back_in")).toBeInTheDocument();
    expect(container.querySelectorAll(".MuiSkeleton-root").length).toBeGreaterThan(0);
  });

  it("does not flash skeletons on a background refetch once a page is cached", () => {
    const item = projectItem({ id: "a", name: "Only Project" });
    useContentMock.mockReturnValue({ page: page([item]), isLoading: true });
    useFavoriteStarsMock.mockReturnValue({ starred: {}, toggleStar: toggleStarMock });

    const { container } = render(<JumpBackIn />);

    expect(container.querySelectorAll(".MuiSkeleton-root")).toHaveLength(0);
    expect(screen.getByText("Only Project")).toBeInTheDocument();
  });

  it("renders nothing once loaded when the feed has no projects", () => {
    useContentMock.mockReturnValue({ page: page([]), isLoading: false });
    useFavoriteStarsMock.mockReturnValue({ starred: {}, toggleStar: toggleStarMock });

    const { container } = render(<JumpBackIn />);

    expect(container).toBeEmptyDOMElement();
  });

  it("puts a pinned project first though it is the older, less recently opened one", () => {
    const older = projectItem({
      id: "a",
      name: "Older Pinned",
      updated_at: new Date(Date.now() - 10 * 24 * 60 * 60 * 1000).toISOString(),
    });
    const newer = projectItem({ id: "b", name: "Newer Recent" });
    // The feed already orders by last-opened, so the newer/more-recently
    // opened project ("b") leads it — pinning must still bring "a" first.
    useContentMock.mockReturnValue({ page: page([newer, older]), isLoading: false });
    useFavoriteStarsMock.mockReturnValue({ starred: { a: true }, toggleStar: toggleStarMock });

    render(<JumpBackIn />);

    const titles = screen.getAllByText(/Older Pinned|Newer Recent/).map((el) => el.textContent);
    expect(titles).toEqual(["Older Pinned", "Newer Recent"]);
  });

  it("gives each card's kebab exactly three actions: open, pin/unpin, show in content", () => {
    const item = projectItem({ id: "a", name: "Only Project" });
    useContentMock.mockReturnValue({ page: page([item]), isLoading: false });
    useFavoriteStarsMock.mockReturnValue({ starred: {}, toggleStar: toggleStarMock });

    render(<JumpBackIn />);
    fireEvent.click(screen.getByRole("button", { name: "more" }));

    expect(screen.getByText("open")).toBeInTheDocument();
    expect(screen.getByText("pin_to_home")).toBeInTheDocument();
    expect(screen.getByText("show_in_content")).toBeInTheDocument();
    expect(screen.queryByText("unpin_from_home")).not.toBeInTheDocument();
  });

  it("shows at most four cards", () => {
    const items = Array.from({ length: 8 }, (_, i) => projectItem({ id: `p${i}`, name: `Project ${i}` }));
    useContentMock.mockReturnValue({ page: page(items), isLoading: false });
    useFavoriteStarsMock.mockReturnValue({ starred: {}, toggleStar: toggleStarMock });

    render(<JumpBackIn />);

    expect(screen.getAllByRole("button", { name: "more" })).toHaveLength(4);
  });

  it("opens a card's project on click", () => {
    const item = projectItem({ id: "a", name: "Only Project" });
    useContentMock.mockReturnValue({ page: page([item]), isLoading: false });
    useFavoriteStarsMock.mockReturnValue({ starred: {}, toggleStar: toggleStarMock });

    render(<JumpBackIn />);
    fireEvent.click(screen.getByText("Only Project"));

    expect(pushMock).toHaveBeenCalledWith("/map/a");
  });

  it("routes all_projects to the Content page's recent view", () => {
    const item = projectItem({ id: "a", name: "Only Project" });
    useContentMock.mockReturnValue({ page: page([item]), isLoading: false });
    useFavoriteStarsMock.mockReturnValue({ starred: {}, toggleStar: toggleStarMock });

    render(<JumpBackIn />);
    fireEvent.click(screen.getByText("all_projects"));

    expect(pushMock).toHaveBeenCalledWith("/content/recent");
  });
});
