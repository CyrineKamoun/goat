import { fireEvent, render, screen, within } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type { Space } from "@/lib/validations/content";
import type { TemplatePage, TemplateRead, TemplateUseResult } from "@/lib/validations/template";

import TemplateBand from "@/components/dashboard/home/TemplateBand";

const { useTemplatesMock, useTemplateMock, useSpacesMock, useFavoriteStarsMock, toggleStarMock, pushMock } =
  vi.hoisted(() => ({
    useTemplatesMock: vi.fn(),
    useTemplateMock: vi.fn(),
    useSpacesMock: vi.fn(),
    useFavoriteStarsMock: vi.fn(),
    toggleStarMock: vi.fn(),
    pushMock: vi.fn(),
  }));

vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    t: (key: string, opts?: Record<string, unknown>) => (opts ? `${key}:${JSON.stringify(opts)}` : key),
  }),
}));
vi.mock("next/navigation", () => ({ useRouter: () => ({ push: pushMock }) }));
vi.mock("@/lib/api/templates", () => ({
  useTemplates: (...args: unknown[]) => useTemplatesMock(...args),
  // The preview dialog reads the picked template's frozen payload.
  useTemplate: (...args: unknown[]) => useTemplateMock(...args),
}));
vi.mock("@/lib/api/content", () => ({
  useSpaces: () => useSpacesMock(),
}));
vi.mock("@/lib/api/favorites", () => ({
  useFavoriteStars: (...args: unknown[]) => useFavoriteStarsMock(...args),
}));
// A card's meta row names its creator, and reads the caller's own account to
// say "You" — mocked so no card fetches a profile from this file.
vi.mock("@/lib/api/users", () => ({ useUserProfile: () => ({ userProfile: { id: "u-me" } }) }));
// `TemplateBrowser` and `UseTemplateFlow` are heavy components with their own
// data hooks — stubbed here so this file tests only TemplateBand's own
// wiring (which props each receives, and what happens when they call back).
vi.mock("@/components/templates/TemplateBrowser", () => ({
  default: ({ onUse, onClose }: { onUse: (t: TemplateRead) => void; onClose: () => void }) => (
    <div data-testid="template-browser">
      <button onClick={() => onUse(template({ id: "from-browser", name: "From browser" }))}>
        browser-use
      </button>
      <button onClick={onClose}>browser-close</button>
    </div>
  ),
}));
vi.mock("@/components/templates/UseTemplateFlow", () => ({
  default: ({
    template: t,
    onDone,
  }: {
    template: TemplateRead;
    onDone: (result: TemplateUseResult) => void;
  }) => (
    <div data-testid="use-template-flow">
      {t.name}
      <button
        onClick={() =>
          onDone({ project_id: "new-project", added_layer_project_ids: [], unresolved_inputs: [] })
        }>
        finish-use
      </button>
    </div>
  ),
}));

const personalSpace: Space = {
  id: "s1",
  kind: "personal",
  name: "My Content",
  default_role: "viewer",
  my_role: "owner",
  team_id: null,
  organization_id: null,
};

const template = (overrides: Partial<TemplateRead>): TemplateRead => ({
  id: "t1",
  name: "Bus network analysis",
  description: null,
  categories: [],
  thumbnail_url: null,
  space_id: "s1",
  folder_id: "f1",
  created_by: null,
  payload_kind: "workflow",
  kinds: ["workflow"],
  inputs: [],
  ships_sample_data: false,
  catalog_status: "published",
  source_ref: {},
  datasets_needing_share: [],
  my_role: "viewer",
  created_at: new Date().toISOString(),
  updated_at: new Date().toISOString(),
  ...overrides,
});

const page = (items: TemplateRead[], total?: number): TemplatePage => ({
  items,
  total: total ?? items.length,
});

beforeEach(() => {
  useTemplatesMock.mockReset();
  useTemplateMock.mockReset().mockReturnValue({ template: undefined, isLoading: false, isError: undefined });
  useSpacesMock.mockReset();
  useFavoriteStarsMock.mockReset();
  toggleStarMock.mockReset();
  pushMock.mockReset();
  useSpacesMock.mockReturnValue({ spaces: [personalSpace] });
  useFavoriteStarsMock.mockReturnValue({ starred: {}, toggleStar: toggleStarMock });
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

describe("TemplateBand", () => {
  it("renders a card per loaded template", () => {
    useTemplatesMock.mockReturnValue({
      page: page([template({ id: "a", name: "Bus network" }), template({ id: "b", name: "Cycling access" })]),
      isLoading: false,
      isError: undefined,
    });

    render(<TemplateBand />);

    expect(screen.getByText("Bus network")).toBeInTheDocument();
    expect(screen.getByText("Cycling access")).toBeInTheDocument();
  });

  it("puts a pinned template first", () => {
    useTemplatesMock.mockReturnValue({
      page: page([template({ id: "a", name: "Unpinned" }), template({ id: "b", name: "Pinned" })]),
      isLoading: false,
      isError: undefined,
    });
    useFavoriteStarsMock.mockReturnValue({ starred: { b: true }, toggleStar: toggleStarMock });

    render(<TemplateBand />);

    const titles = screen.getAllByText(/Unpinned|Pinned/).map((el) => el.textContent);
    expect(titles).toEqual(["Pinned", "Unpinned"]);
  });

  it("caps the grid at six cards on desktop", () => {
    const items = Array.from({ length: 9 }, (_, i) => template({ id: `t${i}`, name: `Template ${i}` }));
    useTemplatesMock.mockReturnValue({ page: page(items, 40), isLoading: false, isError: undefined });

    render(<TemplateBand />);

    expect(screen.getAllByText(/^Template \d$/)).toHaveLength(6);
  });

  it("shows the visible/total count in the footer", () => {
    const items = [template({ id: "a" }), template({ id: "b" })];
    useTemplatesMock.mockReturnValue({ page: page(items, 40), isLoading: false, isError: undefined });

    render(<TemplateBand />);

    expect(screen.getByText('showing_n_of_m_templates:{"n":2,"m":40}')).toBeInTheDocument();
  });

  it("renders the empty state with an action once loaded with no templates", () => {
    useTemplatesMock.mockReturnValue({ page: page([]), isLoading: false, isError: undefined });

    render(<TemplateBand />);

    expect(screen.getByText("empty_templates_all")).toBeInTheDocument();
    expect(screen.getAllByText("all_templates").length).toBeGreaterThan(0);
  });

  it("does not show the empty state while still loading", () => {
    useTemplatesMock.mockReturnValue({ page: undefined, isLoading: true, isError: undefined });

    render(<TemplateBand />);

    expect(screen.queryByText("empty_templates_all")).not.toBeInTheDocument();
  });

  it("renders the header with skeleton cards on first load, no cached page yet, filters hidden", () => {
    useTemplatesMock.mockReturnValue({ page: undefined, isLoading: true, isError: undefined });

    const { container } = render(<TemplateBand />);

    expect(screen.getByText("start_from_a_template")).toBeInTheDocument();
    expect(container.querySelectorAll(".MuiSkeleton-root").length).toBeGreaterThan(0);
    expect(screen.queryByRole("button", { name: "source_goat" })).not.toBeInTheDocument();
  });

  it("does not flash skeletons on a background refetch once a page is cached", () => {
    const found = template({ id: "a", name: "Bus network" });
    useTemplatesMock.mockReturnValue({ page: page([found]), isLoading: true, isError: undefined });

    const { container } = render(<TemplateBand />);

    expect(container.querySelectorAll(".MuiSkeleton-root")).toHaveLength(0);
    expect(screen.getByText("Bus network")).toBeInTheDocument();
  });

  it("renders nothing when the feed errors", () => {
    useTemplatesMock.mockReturnValue({ page: undefined, isLoading: false, isError: new Error("boom") });

    const { container } = render(<TemplateBand />);

    expect(container).toBeEmptyDOMElement();
  });

  it("tags the preview dialog with the shelf the template came from", () => {
    const found = template({ id: "a", name: "Bus network", catalog_status: "none" });
    useTemplatesMock.mockReturnValue({ page: page([found]), isLoading: false, isError: undefined });

    render(<TemplateBand />);
    fireEvent.click(screen.getByText("Bus network"));

    // Scoped to the dialog: the band's own source segments carry the same
    // label behind it.
    expect(within(screen.getByRole("dialog")).getByText("source_mine")).toBeInTheDocument();
  });

  it("opens the preview dialog on card click, then hands off to UseTemplateFlow on Use", () => {
    const found = template({ id: "a", name: "Bus network" });
    useTemplatesMock.mockReturnValue({ page: page([found]), isLoading: false, isError: undefined });

    render(<TemplateBand />);
    fireEvent.click(screen.getByText("Bus network"));
    fireEvent.click(screen.getByText("use_template"));

    expect(screen.getByTestId("use-template-flow")).toBeInTheDocument();
    expect(screen.getByTestId("use-template-flow")).toHaveTextContent("Bus network");
  });

  it("routes to the created project once UseTemplateFlow reports it is done", () => {
    const found = template({ id: "a", name: "Bus network" });
    useTemplatesMock.mockReturnValue({ page: page([found]), isLoading: false, isError: undefined });

    render(<TemplateBand />);
    fireEvent.click(screen.getByText("Bus network"));
    fireEvent.click(screen.getByText("use_template"));
    fireEvent.click(screen.getByText("finish-use"));

    expect(pushMock).toHaveBeenCalledWith("/map/new-project");
  });

  it("opens the browser dialog from the header action and hands its selection to UseTemplateFlow", () => {
    useTemplatesMock.mockReturnValue({ page: page([]), isLoading: false, isError: undefined });

    render(<TemplateBand />);
    fireEvent.click(screen.getAllByText("all_templates")[0]);
    expect(screen.getByTestId("template-browser")).toBeInTheDocument();

    fireEvent.click(screen.getByText("browser-use"));

    expect(screen.queryByTestId("template-browser")).not.toBeInTheDocument();
    expect(screen.getByTestId("use-template-flow")).toHaveTextContent("From browser");
  });

  it("links the GOAT source to the Catalog Templates tab", () => {
    useTemplatesMock.mockReturnValue({ page: page([]), isLoading: false, isError: undefined });

    render(<TemplateBand />);
    fireEvent.click(screen.getByRole("button", { name: "source_goat" }));
    fireEvent.click(screen.getByText("browse_goat_templates"));

    expect(pushMock).toHaveBeenCalledWith("/catalog?tab=templates");
  });

  it("links the Mine source to Content filtered to templates", () => {
    useTemplatesMock.mockReturnValue({ page: page([]), isLoading: false, isError: undefined });

    render(<TemplateBand />);
    fireEvent.click(screen.getByRole("button", { name: "source_mine" }));
    fireEvent.click(screen.getByText("show_in_content"));

    expect(pushMock).toHaveBeenCalledWith("/content/s1?types=template");
  });
});
