import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { tagColor } from "@/lib/utils/tagColor";
import type { Space } from "@/lib/validations/content";
import type { TemplatePage, TemplateRead } from "@/lib/validations/template";

import TemplateBrowser from "@/components/templates/TemplateBrowser";

const {
  useTemplatesMock,
  useTemplateCategoriesMock,
  useTemplateMock,
  useSpacesMock,
  useFavoriteStarsMock,
  toggleStarMock,
  useUserProfileMock,
} = vi.hoisted(() => ({
  useTemplatesMock: vi.fn(),
  useTemplateCategoriesMock: vi.fn(),
  useTemplateMock: vi.fn(),
  useSpacesMock: vi.fn(),
  useFavoriteStarsMock: vi.fn(),
  toggleStarMock: vi.fn(),
  useUserProfileMock: vi.fn(),
}));

vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    t: (key: string, opts?: Record<string, unknown>) => (opts ? `${key}:${JSON.stringify(opts)}` : key),
  }),
}));
vi.mock("@/lib/api/templates", () => ({
  useTemplates: (...args: unknown[]) => useTemplatesMock(...args),
  useTemplateCategories: (...args: unknown[]) => useTemplateCategoriesMock(...args),
  useTemplate: (...args: unknown[]) => useTemplateMock(...args),
}));
vi.mock("@/lib/api/content", () => ({
  useSpaces: () => useSpacesMock(),
}));
vi.mock("@/lib/api/favorites", () => ({
  useFavoriteStars: () => useFavoriteStarsMock(),
}));
vi.mock("@/lib/api/users", () => ({
  useUserProfile: () => useUserProfileMock(),
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
const teamSpace: Space = { ...personalSpace, id: "s2", kind: "team", name: "Marketing", team_id: "team-1" };
const otherTeamSpace: Space = {
  ...personalSpace,
  id: "s4",
  kind: "team",
  name: "Planning",
  team_id: "team-2",
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

const page = (items: TemplateRead[]): TemplatePage => ({ items, total: items.length });

/** The facet `GET /template/categories` would answer with for a page: the
 * categories its templates carry, most used first. The endpoint counts every
 * readable template rather than one page, so a test that wants a tag the
 * loaded page does not carry passes its own rows in. */
const facetsFor = (loaded: TemplatePage) => {
  const counts = new Map<string, number>();
  for (const item of loaded.items) {
    for (const category of item.categories) counts.set(category, (counts.get(category) ?? 0) + 1);
  }
  return [...counts.entries()]
    .map(([name, count]) => ({ name, count }))
    .sort((a, b) => b.count - a.count || a.name.localeCompare(b.name));
};

/** The page the list shows, and the tag facet that goes with it. */
const mockShelf = (loaded: TemplatePage, facets = facetsFor(loaded)) => {
  useTemplatesMock.mockReturnValue({ page: loaded, isLoading: false });
  useTemplateCategoriesMock.mockReturnValue({ categories: facets, isLoading: false });
};

/** The dialog's own list, so a query cannot pick up the preview column. */
const list = () => screen.getByRole("listbox");

beforeEach(() => {
  useTemplatesMock.mockReset();
  useTemplateCategoriesMock.mockReset().mockReturnValue({ categories: [], isLoading: false });
  useTemplateMock.mockReset().mockReturnValue({ template: undefined, isLoading: false });
  useSpacesMock.mockReset();
  useFavoriteStarsMock.mockReset();
  toggleStarMock.mockReset();
  useUserProfileMock.mockReset().mockReturnValue({ userProfile: { id: "me" } });
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

describe("TemplateBrowser (inline)", () => {
  it("renders a card per loaded template", () => {
    useTemplatesMock.mockReturnValue({
      page: page([template({ id: "a", name: "Bus network" }), template({ id: "b", name: "Cycling access" })]),
      isLoading: false,
    });

    render(<TemplateBrowser mode="inline" onUse={vi.fn()} />);

    expect(screen.getByText("Bus network")).toBeInTheDocument();
    expect(screen.getByText("Cycling access")).toBeInTheDocument();
  });

  it("keeps every kind pill selectable, with no counts on them", () => {
    useTemplatesMock.mockReturnValue({
      page: page([template({ id: "a", kinds: ["workflow"] })]),
      isLoading: false,
    });

    render(<TemplateBrowser mode="inline" onUse={vi.fn()} />);

    // Read off the DOM property rather than a matcher: the repo's ESLint
    // setup reads jest-dom's `toBeDisabled`/`toBeEnabled` as Playwright's
    // (async) matchers of the same name.
    for (const name of [/template_kind_workflow/, /template_kind_dashboard/, /template_kind_layout/]) {
      const pill = screen.getByRole("button", { name }) as HTMLButtonElement;
      expect(pill.disabled).toBe(false);
    }
    expect(screen.getByRole("button", { name: "template_kind_dashboard" })).toBeInTheDocument();
    // One request, not a second kind-unfiltered one for the pill counts.
    expect(useTemplatesMock).toHaveBeenCalledTimes(1);
  });

  it("previews nothing until a card is clicked, then fires onUse from the dialog", () => {
    const onUse = vi.fn();
    const found = template({ id: "a", name: "Bus network" });
    useTemplatesMock.mockReturnValue({ page: page([found]), isLoading: false });

    render(<TemplateBrowser mode="inline" onUse={onUse} />);
    expect(screen.queryByText("use_template")).not.toBeInTheDocument();

    fireEvent.click(screen.getByText("Bus network"));
    fireEvent.click(screen.getByText("use_template"));

    expect(onUse).toHaveBeenCalledWith(found);
  });

  it("keeps the preview open across a filter change", () => {
    useTemplatesMock.mockReturnValue({
      page: page([template({ id: "a", name: "Bus network" })]),
      isLoading: false,
    });

    render(<TemplateBrowser mode="inline" onUse={vi.fn()} />);
    fireEvent.click(screen.getByText("Bus network"));
    // The open preview dialog marks the page behind it aria-hidden, so the
    // pill has to be reached explicitly.
    fireEvent.click(screen.getByRole("button", { name: "template_kind_workflow", hidden: true }));

    expect(screen.getByText("use_template")).toBeInTheDocument();
  });

  it("reads no template of its own for a preview — the descriptor came with the row", () => {
    useTemplatesMock.mockReturnValue({
      page: page([template({ id: "a", name: "Bus network", my_role: "owner", catalog_status: "none" })]),
      isLoading: false,
    });

    render(<TemplateBrowser mode="inline" onUse={vi.fn()} />);
    fireEvent.click(screen.getByText("Bus network"));

    // Only the preselected-template read, which nothing asked for here.
    expect(useTemplateMock).toHaveBeenCalledWith(null);
    expect(useTemplateMock).not.toHaveBeenCalledWith("a", true);
  });

  it("reads no single template when none is preselected", () => {
    useTemplatesMock.mockReturnValue({ page: page([]), isLoading: false });

    render(<TemplateBrowser mode="inline" onUse={vi.fn()} />);

    expect(useTemplateMock).toHaveBeenCalledWith(null);
  });

  it("browses the GOAT shelf, whatever source it is handed", () => {
    useTemplatesMock.mockReturnValue({ page: page([]), isLoading: false });

    const { unmount } = render(<TemplateBrowser mode="inline" onUse={vi.fn()} />);
    expect(useTemplatesMock).toHaveBeenLastCalledWith(expect.objectContaining({ source: "goat" }));
    expect(useTemplateCategoriesMock).toHaveBeenLastCalledWith({ source: "goat", kind: undefined });
    unmount();

    // The Catalog tab is GOAT-only: a source handed in cannot move it.
    render(<TemplateBrowser mode="inline" initialSource="mine" onUse={vi.fn()} />);

    expect(useTemplatesMock).toHaveBeenLastCalledWith(expect.objectContaining({ source: "goat" }));
  });

  it("offers nothing that would change the source", () => {
    useTemplatesMock.mockReturnValue({ page: page([]), isLoading: false });
    useSpacesMock.mockReturnValue({
      spaces: [
        personalSpace,
        teamSpace,
        { ...personalSpace, id: "s3", kind: "organization", organization_id: "org-1" },
      ],
    });

    render(<TemplateBrowser mode="inline" onUse={vi.fn()} />);

    // No segments, no filter pill holding a source section, no source chip.
    for (const name of [
      "source_everyone",
      "source_goat",
      "source_mine",
      "source_team",
      "source_organization",
    ]) {
      expect(screen.queryByRole("button", { name })).not.toBeInTheDocument();
      expect(screen.queryByRole("radio", { name })).not.toBeInTheDocument();
    }
    expect(screen.queryByText("source")).not.toBeInTheDocument();
    expect(screen.queryByTestId("template-filter-chip-source")).not.toBeInTheDocument();
  });

  it("keeps the source on GOAT across a kind change", () => {
    useTemplatesMock.mockReturnValue({ page: page([]), isLoading: false });

    render(<TemplateBrowser mode="inline" onUse={vi.fn()} />);
    fireEvent.click(screen.getByRole("button", { name: "template_kind_layout" }));

    expect(useTemplatesMock).toHaveBeenLastCalledWith(
      expect.objectContaining({ source: "goat", kind: "layout" })
    );
  });

  it("says the GOAT shelf is empty without nudging the caller to another source", () => {
    useTemplatesMock.mockReturnValue({ page: page([]), isLoading: false });

    render(<TemplateBrowser mode="inline" onUse={vi.fn()} />);

    expect(screen.getByText('no_templates_in_scope_title:{"scope":"source_goat"}')).toBeInTheDocument();
    expect(screen.getByText("templates_empty_goat_hint")).toBeInTheDocument();
    expect(screen.queryByText("browse_goat_templates")).not.toBeInTheDocument();
    expect(screen.queryByText("show_all_sources")).not.toBeInTheDocument();
  });

  it("says a search matched nothing rather than that the shelf is empty", () => {
    vi.useFakeTimers();
    useTemplatesMock.mockImplementation((params: { search?: string }) => ({
      page: params?.search ? page([]) : page([template({ id: "a", name: "Bus network" })]),
      isLoading: false,
    }));

    render(<TemplateBrowser mode="inline" onUse={vi.fn()} />);
    fireEvent.change(screen.getByLabelText("search"), { target: { value: "nothing" } });
    act(() => {
      vi.advanceTimersByTime(300);
    });

    expect(screen.getByText("templates_empty_filter_title")).toBeInTheDocument();
    expect(screen.getByText("templates_empty_filter_hint")).toBeInTheDocument();
    vi.useRealTimers();
  });
});

describe("TemplateBrowser (dialog)", () => {
  const openDialog = (props: Partial<React.ComponentProps<typeof TemplateBrowser>> = {}) =>
    render(<TemplateBrowser mode="dialog" open onClose={vi.fn()} onUse={vi.fn()} {...props} />);

  const openFilters = () => fireEvent.click(screen.getByRole("button", { name: "filter" }));

  it("previews the first row without being asked", () => {
    useTemplatesMock.mockReturnValue({
      page: page([template({ id: "a", name: "Bus network" }), template({ id: "b", name: "Cycling access" })]),
      isLoading: false,
    });

    openDialog();

    // Read the attribute off the DOM rather than through `toHaveAttribute`:
    // the repo's ESLint setup reads that jest-dom matcher as Playwright's
    // (async) matcher of the same name.
    const options = within(list()).getAllByRole("option");
    expect(options[0].getAttribute("aria-selected")).toBe("true");
    // The name is in the list AND, as the preview's title, on the right.
    expect(screen.getAllByText("Bus network")).toHaveLength(2);
    expect(screen.getAllByText("Cycling access")).toHaveLength(1);
  });

  it("swaps the preview when another row is picked", () => {
    useTemplatesMock.mockReturnValue({
      page: page([template({ id: "a", name: "Bus network" }), template({ id: "b", name: "Cycling access" })]),
      isLoading: false,
    });

    openDialog();
    fireEvent.click(within(list()).getByText("Cycling access"));

    expect(screen.getAllByText("Cycling access")).toHaveLength(2);
    expect(screen.getAllByText("Bus network")).toHaveLength(1);
    expect(within(list()).getAllByRole("option")[1].getAttribute("aria-selected")).toBe("true");
  });

  it("groups the rows by shelf under Everyone", () => {
    useSpacesMock.mockReturnValue({ spaces: [personalSpace, teamSpace] });
    useTemplatesMock.mockReturnValue({
      page: page([
        template({ id: "a", name: "GOAT starter", catalog_status: "published" }),
        template({
          id: "b",
          name: "My draft",
          catalog_status: "none",
          created_by: { id: "me", name: "Majk" },
        }),
        template({
          id: "c",
          name: "Team draft",
          catalog_status: "none",
          space_id: "s2",
          created_by: { id: "other", name: "Ada" },
        }),
      ]),
      isLoading: false,
    });

    openDialog();

    expect(within(list()).getByText("source_goat · 1")).toBeInTheDocument();
    expect(within(list()).getByText("source_mine · 1")).toBeInTheDocument();
    expect(within(list()).getByText("Marketing · 1")).toBeInTheDocument();
    // Grouped rows carry no shelf tag of their own.
    expect(within(list()).queryByText("source_goat")).not.toBeInTheDocument();
  });

  it("tags each row with its shelf when a single source is shown", () => {
    useTemplatesMock.mockReturnValue({
      page: page([template({ id: "a", name: "GOAT starter" })]),
      isLoading: false,
    });

    openDialog({ initialSource: "goat" });

    expect(within(list()).getByText("source_goat")).toBeInTheDocument();
    expect(within(list()).queryByText(/·/)).not.toBeInTheDocument();
  });

  it("previews the first row as rendered, not the first the API returned", () => {
    useTemplatesMock.mockReturnValue({
      page: page([
        template({ id: "b", name: "My draft", catalog_status: "none" }),
        template({ id: "a", name: "GOAT starter", catalog_status: "published" }),
      ]),
      isLoading: false,
    });

    openDialog();

    // The GOAT group renders above Mine, so the GOAT row is the first on
    // screen even though the API listed the personal one first.
    const options = within(list()).getAllByRole("option");
    expect(options[0].textContent).toContain("GOAT starter");
    expect(options[0].getAttribute("aria-selected")).toBe("true");
    expect(screen.getAllByText("GOAT starter")).toHaveLength(2);
  });

  it("gives each team space its own group, named after that space", () => {
    useSpacesMock.mockReturnValue({ spaces: [personalSpace, teamSpace, otherTeamSpace] });
    useTemplatesMock.mockReturnValue({
      page: page([
        template({ id: "c", name: "Planning draft", catalog_status: "none", space_id: "s4" }),
        template({ id: "b", name: "Marketing draft", catalog_status: "none", space_id: "s2" }),
      ]),
      isLoading: false,
    });

    openDialog();

    expect(within(list()).getByText("Marketing · 1")).toBeInTheDocument();
    expect(within(list()).getByText("Planning · 1")).toBeInTheDocument();
    // Teams stack alphabetically, each labelled after its own space.
    expect(
      within(list())
        .getAllByRole("group")
        .map((group) => group.getAttribute("aria-label"))
    ).toEqual(["Marketing", "Planning"]);
  });

  it("re-selects the first row of the new list when a filter changes", () => {
    useTemplatesMock.mockReturnValue({
      page: page([
        template({ id: "a", name: "GOAT starter", catalog_status: "published" }),
        template({ id: "b", name: "Cycling access", catalog_status: "published" }),
      ]),
      isLoading: false,
    });

    openDialog();
    fireEvent.click(within(list()).getByText("Cycling access"));
    expect(within(list()).getAllByRole("option")[1].getAttribute("aria-selected")).toBe("true");

    openFilters();
    fireEvent.click(screen.getByRole("radio", { name: "source_goat" }));

    const options = within(list()).getAllByRole("option");
    expect(options[0].getAttribute("aria-selected")).toBe("true");
    expect(options[1].getAttribute("aria-selected")).toBe("false");
  });

  it("titles the dialog with the locked kind and drops the kind pills", () => {
    useTemplatesMock.mockReturnValue({ page: page([template({})]), isLoading: false });

    openDialog({ lockedKind: "workflow" });

    expect(screen.getByText("new_workflow_from_template")).toBeInTheDocument();
    expect(screen.getByText("template_browser_locked_hint")).toBeInTheDocument();
    expect(screen.queryByText("all_templates")).not.toBeInTheDocument();
  });

  it("titles the unlocked dialog with the shelf", () => {
    useTemplatesMock.mockReturnValue({ page: page([template({})]), isLoading: false });

    openDialog();

    expect(screen.getByText("templates")).toBeInTheDocument();
    expect(screen.getByText("template_browser_hint")).toBeInTheDocument();
  });

  it("empties the list column and the preview together", () => {
    useTemplatesMock.mockReturnValue({ page: page([]), isLoading: false });

    openDialog({ initialSource: "mine" });

    expect(screen.getByText('templates_empty_source_title:{"source":"source_mine"}')).toBeInTheDocument();
    expect(screen.getByText("templates_empty_source_hint")).toBeInTheDocument();
    // Nothing to press in the dialog's empty state.
    expect(screen.queryByText("browse_goat_templates")).not.toBeInTheDocument();
    expect(screen.queryByText("show_all_sources")).not.toBeInTheDocument();
    expect(screen.getByText("select_template_to_preview")).toBeInTheDocument();
    const use = screen.getByRole("button", { name: "use_template" }) as HTMLButtonElement;
    expect(use.disabled).toBe(true);
  });

  it("uses the selected template from the footer", () => {
    const onUse = vi.fn();
    const found = template({ id: "a", name: "Bus network" });
    useTemplatesMock.mockReturnValue({ page: page([found]), isLoading: false });

    openDialog({ onUse });
    fireEvent.click(screen.getByRole("button", { name: "use_template" }));

    expect(onUse).toHaveBeenCalledWith(found);
  });

  it("previews a preselected template rather than the first row", () => {
    const starter = template({ id: "starter", name: "Bus network starter" });
    useTemplatesMock.mockReturnValue({
      page: page([template({ id: "a", name: "Cycling access" })]),
      isLoading: false,
    });
    useTemplateMock.mockImplementation((id: string | null) =>
      id === "starter" ? { template: starter, isLoading: false } : { template: undefined, isLoading: false }
    );

    openDialog({ initialTemplateId: "starter" });

    expect(useTemplateMock).toHaveBeenCalledWith("starter");
    expect(screen.getByText("Bus network starter")).toBeInTheDocument();
  });

  it("stands rows in while the first page loads", () => {
    useTemplatesMock.mockReturnValue({ page: undefined, isLoading: true });

    openDialog();

    expect(screen.queryByRole("listbox")).not.toBeInTheDocument();
    expect(screen.queryByText("no_templates_yet_title")).not.toBeInTheDocument();
  });
});

describe("TemplateBrowser (dialog) — empty shelf", () => {
  const openDialog = (props: Partial<React.ComponentProps<typeof TemplateBrowser>> = {}) =>
    render(<TemplateBrowser mode="dialog" open onClose={vi.fn()} onUse={vi.fn()} {...props} />);

  it("fills the body with one empty state when the shelf itself is empty", () => {
    useTemplatesMock.mockReturnValue({ page: page([]), isLoading: false });

    openDialog();

    expect(screen.getByText("no_templates_yet_title")).toBeInTheDocument();
    expect(screen.getByText("empty_templates_mine")).toBeInTheDocument();
    // Nothing to press: the shelf empty state is a sentence, not a nudge.
    expect(screen.queryByText("browse_goat_templates")).not.toBeInTheDocument();
    // No split: neither the list column nor the preview placeholder is there.
    expect(screen.queryByRole("listbox")).not.toBeInTheDocument();
    expect(screen.queryByText("select_template_to_preview")).not.toBeInTheDocument();
    expect(screen.queryByLabelText("search_templates")).not.toBeInTheDocument();
    // The dialog's own actions stay under it.
    expect(screen.getByRole("button", { name: "use_template" })).toBeInTheDocument();
  });

  it("keeps the split layout when a search is what emptied the list", () => {
    // The search debounces before it reaches the API, so the timer has to be
    // run out for the empty page to arrive.
    vi.useFakeTimers();
    useTemplatesMock.mockImplementation((params: { search?: string }) => ({
      page: params?.search ? page([]) : page([template({ id: "a", name: "Bus network" })]),
      isLoading: false,
    }));

    openDialog();
    fireEvent.change(screen.getByLabelText("search_templates"), { target: { value: "nothing" } });
    act(() => {
      vi.advanceTimersByTime(300);
    });

    expect(screen.getByText("templates_empty_filter_title")).toBeInTheDocument();
    expect(screen.getByText("templates_empty_filter_hint")).toBeInTheDocument();
    expect(screen.getByText("select_template_to_preview")).toBeInTheDocument();
    expect(screen.getByLabelText("search_templates")).toBeInTheDocument();
    vi.useRealTimers();
  });

  it("keeps the split layout when a source filter is what emptied the list", () => {
    useTemplatesMock.mockReturnValue({ page: page([]), isLoading: false });

    openDialog({ initialSource: "mine" });

    expect(screen.getByText('templates_empty_source_title:{"source":"source_mine"}')).toBeInTheDocument();
    expect(screen.getByText("select_template_to_preview")).toBeInTheDocument();
  });
});

describe("TemplateBrowser (dialog) — filter pill", () => {
  const openDialog = (props: Partial<React.ComponentProps<typeof TemplateBrowser>> = {}) =>
    render(<TemplateBrowser mode="dialog" open onClose={vi.fn()} onUse={vi.fn()} {...props} />);

  const filterPill = () => screen.getByRole("button", { name: "filter" });
  const openFilters = () => fireEvent.click(filterPill());
  const categoriesChip = () => screen.getByTestId("template-filter-chip-categories");
  const removeChip = (element: HTMLElement) =>
    fireEvent.click(element.querySelector(".MuiChip-deleteIcon") as Element);

  const tagged = () =>
    page([
      template({ id: "a", name: "Bus network", categories: ["Transport"] }),
      template({ id: "b", name: "Green space", categories: ["Environment"] }),
    ]);

  it("replaces the source segments with a pill in the dialog", () => {
    mockShelf(tagged());

    openDialog();

    expect(filterPill()).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "source_everyone" })).not.toBeInTheDocument();
  });

  it("keeps the source section and its chip in the dialog, where the shelf is the caller's to pick", () => {
    mockShelf(tagged());

    openDialog();
    openFilters();

    expect(screen.getByText("source")).toBeInTheDocument();
    expect(screen.getByRole("radio", { name: "source_mine" })).toBeInTheDocument();

    fireEvent.click(screen.getByRole("radio", { name: "source_mine" }));

    expect(screen.getByTestId("template-filter-chip-source")).toBeInTheDocument();
  });

  it("counts the active filters on the pill badge", () => {
    mockShelf(tagged());

    openDialog();
    expect(filterPill().textContent).not.toContain("2");

    openFilters();
    fireEvent.click(screen.getByRole("radio", { name: "source_goat" }));
    fireEvent.click(screen.getByRole("checkbox", { name: "Transport" }));

    expect(filterPill().textContent).toContain("2");
  }, 15000);

  it("asks the API for the source picked in the popover, tag counts included", () => {
    mockShelf(tagged());

    openDialog();
    openFilters();
    fireEvent.click(screen.getByRole("radio", { name: "source_mine" }));

    expect(useTemplatesMock).toHaveBeenLastCalledWith(expect.objectContaining({ source: "mine" }));
    // The facet is scoped the same way, so the counts describe the shelf the
    // list is showing.
    expect(useTemplateCategoriesMock).toHaveBeenLastCalledWith({ source: "mine", kind: undefined });
    expect(screen.getByText("source: source_mine")).toBeInTheDocument();
  });

  it("offers a tag the loaded page does not carry, since the facet counts the whole shelf", () => {
    mockShelf(tagged(), [
      { name: "Transport", count: 12 },
      { name: "Cycling", count: 4 },
    ]);

    openDialog();
    openFilters();

    expect(screen.getByRole("checkbox", { name: "Cycling" })).toBeInTheDocument();
  });

  it("asks the API to filter by the ticked tag, and shows it as a removable chip", () => {
    mockShelf(tagged());

    openDialog();
    openFilters();
    fireEvent.click(screen.getByRole("checkbox", { name: "Transport" }));

    // Server-side: the tag goes out as a query parameter rather than
    // narrowing the page already in hand.
    expect(useTemplatesMock).toHaveBeenLastCalledWith(expect.objectContaining({ categories: "Transport" }));
    expect(within(categoriesChip()).getByText("Transport")).toBeInTheDocument();

    removeChip(categoriesChip());

    expect(useTemplatesMock).toHaveBeenLastCalledWith(expect.objectContaining({ categories: undefined }));
    expect(screen.queryByTestId("template-filter-chip-categories")).not.toBeInTheDocument();
  });

  it("sends every ticked tag as one comma-separated value", () => {
    mockShelf(tagged());

    openDialog();
    openFilters();
    fireEvent.click(screen.getByRole("checkbox", { name: "Transport" }));
    fireEvent.click(screen.getByRole("checkbox", { name: "Environment" }));

    expect(useTemplatesMock).toHaveBeenLastCalledWith(
      expect.objectContaining({ categories: "Transport,Environment" })
    );
  });

  it("clears every filter at once", () => {
    mockShelf(tagged());

    openDialog();
    openFilters();
    fireEvent.click(screen.getByRole("radio", { name: "source_goat" }));
    fireEvent.click(screen.getByRole("checkbox", { name: "Transport" }));
    expect(within(categoriesChip()).getByText("Transport")).toBeInTheDocument();

    fireEvent.click(screen.getByText("clear_all"));

    expect(screen.queryByTestId("template-filter-chip-categories")).not.toBeInTheDocument();
    expect(screen.queryByText("source: source_goat")).not.toBeInTheDocument();
    expect(filterPill().textContent).not.toContain("1");
  });

  it("closes just the popover on Escape and leaves a second press for the dialog", async () => {
    mockShelf(tagged());
    const onClose = vi.fn();

    openDialog({ onClose });
    openFilters();
    // Focus lands inside the popover, not on the pill — the case that also
    // needs the pill refocused once Escape closes the popover.
    const radio = screen.getByRole("radio", { name: "source_goat" });
    radio.focus();

    fireEvent.keyDown(radio, { key: "Escape" });

    // The popover exits via a Grow transition, so its removal is async.
    await waitFor(() => expect(screen.queryByText("source")).not.toBeInTheDocument());
    expect(onClose).not.toHaveBeenCalled();
    // Read off `document.activeElement` rather than through `toHaveFocus`:
    // the repo's ESLint setup reads that jest-dom matcher as Playwright's
    // (async) matcher of the same name.
    expect(document.activeElement).toBe(filterPill());

    fireEvent.keyDown(filterPill(), { key: "Escape" });

    expect(onClose).toHaveBeenCalledTimes(1);
  });
});

describe("TemplateBrowser (dialog) — categories at scale", () => {
  const openDialog = () => render(<TemplateBrowser mode="dialog" open onClose={vi.fn()} onUse={vi.fn()} />);
  const openFilters = () => fireEvent.click(screen.getByRole("button", { name: "filter" }));
  /** Each tag row as it reads: the name, then its count. */
  const tagRows = () =>
    screen.getAllByRole("checkbox").map((box) => box.closest("label")?.parentElement?.textContent ?? "");

  /** `count` templates, each carrying one of `categories` in turn, so the
   * category counts differ. */
  const withCategories = (categories: string[][]) =>
    page(categories.map((cats, index) => template({ id: `t${index}`, name: `T${index}`, categories: cats })));

  it("orders the tags by how many templates carry them, selected first", () => {
    mockShelf(withCategories([["Rare"], ["Common"], ["Common"], ["Common"], ["Middling"], ["Middling"]]));

    openDialog();
    openFilters();

    expect(tagRows()).toEqual(["Common3", "Middling2", "Rare1"]);

    fireEvent.click(screen.getByRole("checkbox", { name: "Rare" }));

    // A ticked tag sorts to the top and keeps the count it had.
    expect(tagRows()).toEqual(["Rare1", "Common3", "Middling2"]);
  });

  it("shows the first eight tags, with the rest behind Show all", () => {
    mockShelf(withCategories(Array.from({ length: 10 }, (_, index) => [`Cat${index}`])));

    openDialog();
    openFilters();

    expect(screen.getAllByRole("checkbox")).toHaveLength(8);

    fireEvent.click(screen.getByText("show_all"));
    expect(screen.getAllByRole("checkbox")).toHaveLength(10);

    fireEvent.click(screen.getByText("show_less"));
    expect(screen.getAllByRole("checkbox")).toHaveLength(8);
  });

  it("narrows the tag list, not the templates, from the section's own search", () => {
    mockShelf(withCategories([...Array.from({ length: 13 }, (_, index) => [`Cat${index}`]), ["Transport"]]));

    openDialog();
    openFilters();
    fireEvent.change(screen.getByLabelText("catalog_filter_values"), { target: { value: "transp" } });

    expect(tagRows()).toEqual(["Transport1"]);
    // The list behind the popover is untouched by a tag search.
    expect(within(list()).getAllByRole("option")).toHaveLength(14);
  });

  it("offers no tag search while there are few tags", () => {
    mockShelf(withCategories(Array.from({ length: 10 }, (_, index) => [`Cat${index}`])));

    openDialog();
    openFilters();

    expect(screen.queryByLabelText("catalog_filter_values")).not.toBeInTheDocument();
  });
});

describe("TemplateBrowser (dialog) — categories chip", () => {
  const openDialog = () => render(<TemplateBrowser mode="dialog" open onClose={vi.fn()} onUse={vi.fn()} />);
  const openFilters = () => fireEvent.click(screen.getByRole("button", { name: "filter" }));
  const categoriesChip = () => screen.getByTestId("template-filter-chip-categories");
  const tick = (name: string) => fireEvent.click(screen.getByRole("checkbox", { name }));

  beforeEach(() => {
    mockShelf(
      page([
        template({ id: "a", name: "Bus network", categories: ["Alpha", "Beta", "Gamma"] }),
        template({ id: "b", name: "Green space", categories: ["Alpha"] }),
      ])
    );
  });

  it("collapses three or more tags into two names and a count", () => {
    openDialog();
    openFilters();
    tick("Alpha");
    expect(categoriesChip()).toHaveTextContent("categories:");
    expect(within(categoriesChip()).getByText("Alpha")).toBeInTheDocument();

    tick("Beta");
    expect(within(categoriesChip()).getByText("Beta")).toBeInTheDocument();

    tick("Gamma");
    expect(within(categoriesChip()).queryByText("Gamma")).not.toBeInTheDocument();
    expect(within(categoriesChip()).getByText("+1")).toBeInTheDocument();
  });

  it("names each tag in the chip in its own colour", () => {
    openDialog();
    openFilters();
    tick("Alpha");
    tick("Beta");

    const alpha = within(categoriesChip()).getByText("Alpha");
    const beta = within(categoriesChip()).getByText("Beta");
    expect(alpha).toHaveStyle({ color: tagColor("Alpha").fg });
    expect(beta).toHaveStyle({ color: tagColor("Beta").fg });
  });

  it("names the icon that drops the filter", () => {
    openDialog();
    openFilters();
    tick("Alpha");

    expect(within(categoriesChip()).getByTitle("clear_filter")).toBeInTheDocument();
  });

  it("clears every tag when the chip is removed", () => {
    openDialog();
    openFilters();
    tick("Alpha");
    tick("Beta");

    fireEvent.click(categoriesChip().querySelector(".MuiChip-deleteIcon") as Element);

    expect(screen.queryByTestId("template-filter-chip-categories")).not.toBeInTheDocument();
    expect(screen.getAllByRole("checkbox").every((box) => !(box as HTMLInputElement).checked)).toBe(true);
  });
});

describe("TemplateBrowser (dialog) — categories without the facet", () => {
  const openDialog = () => render(<TemplateBrowser mode="dialog" open onClose={vi.fn()} onUse={vi.fn()} />);
  const openFilters = () => fireEvent.click(screen.getByRole("button", { name: "filter" }));

  /** The facet request failed, so the tag list and the filtering both fall
   * back to the page in hand. */
  const facetDown = (loaded: TemplatePage) => {
    useTemplatesMock.mockReturnValue({ page: loaded, isLoading: false });
    useTemplateCategoriesMock.mockReturnValue({
      categories: undefined,
      isLoading: false,
      isError: new Error("boom"),
    });
  };

  const tagged = () =>
    page([
      template({ id: "a", name: "Bus network", categories: ["Transport"] }),
      template({ id: "b", name: "Green space", categories: ["Environment"] }),
    ]);

  it("offers the loaded page's own tags", () => {
    facetDown(tagged());

    openDialog();
    openFilters();

    expect(screen.getByRole("checkbox", { name: "Transport" })).toBeInTheDocument();
    expect(screen.getByRole("checkbox", { name: "Environment" })).toBeInTheDocument();
  });

  it("filters the loaded page itself, and asks the API for no categories", () => {
    facetDown(tagged());

    openDialog();
    openFilters();
    fireEvent.click(screen.getByRole("checkbox", { name: "Transport" }));

    expect(useTemplatesMock).toHaveBeenLastCalledWith(expect.objectContaining({ categories: undefined }));
    expect(within(list()).getByText("Bus network")).toBeInTheDocument();
    expect(within(list()).queryByText("Green space")).not.toBeInTheDocument();
  });
});

describe("TemplateBrowser (dialog) — while the tag list loads", () => {
  it("stands the categories section in with skeletons rather than popping it in", () => {
    useTemplatesMock.mockReturnValue({ page: page([template({ id: "a" })]), isLoading: false });
    useTemplateCategoriesMock.mockReturnValue({ categories: undefined, isLoading: true });

    render(<TemplateBrowser mode="dialog" open onClose={vi.fn()} onUse={vi.fn()} />);
    fireEvent.click(screen.getByRole("button", { name: "filter" }));

    expect(screen.getByText("categories")).toBeInTheDocument();
    expect(document.querySelectorAll(".MuiSkeleton-root")).toHaveLength(3);
    expect(screen.queryByRole("checkbox")).not.toBeInTheDocument();
  });
});

describe("TemplateBrowser (dialog) — stacked below md", () => {
  // Read right before it is overwritten below, so afterEach puts back
  // whatever the outer beforeEach had just set up for this test, not
  // whatever `window.matchMedia` happened to be when the file loaded.
  let original: typeof window.matchMedia;

  beforeEach(() => {
    original = window.matchMedia;
    window.matchMedia = ((query: string) =>
      ({
        matches: true,
        media: query,
        onchange: null,
        addListener: vi.fn(),
        removeListener: vi.fn(),
        addEventListener: vi.fn(),
        removeEventListener: vi.fn(),
        dispatchEvent: vi.fn(),
      }) as unknown as MediaQueryList) as typeof window.matchMedia;
  });

  afterEach(() => {
    window.matchMedia = original;
  });

  it("carries the dialog's actions under the list, before anything is picked", () => {
    mockShelf(page([template({ id: "a", name: "Bus network" })]));

    render(<TemplateBrowser mode="dialog" open onClose={vi.fn()} onUse={vi.fn()} />);

    // The columns are stacked, so the list is on screen and the preview is
    // not — and the way out still has to be there.
    expect(within(list()).getByText("Bus network")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "cancel" })).toBeInTheDocument();
  });
});
