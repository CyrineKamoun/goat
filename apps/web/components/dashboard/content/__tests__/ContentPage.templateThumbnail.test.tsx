import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type { ContentItem } from "@/lib/validations/content";

import { ContentActions } from "@/types/common";

// The "Regenerate thumbnail" kebab pick, through the one seam the page has
// for it: a stubbed card that renders a button per menu item and calls the
// page's own `onMenuSelect`. Everything else the page pulls in is mocked,
// the way `ContentPage.jobStatus.test.tsx` already does — the redraw itself
// is covered against its own API mocks in
// `lib/templates/__tests__/thumbnailSnapshot.test.ts`.
import type { PopperMenuItem } from "@/components/common/PopperMenu";
import ContentPage from "@/components/dashboard/content/ContentPage";

const { regenerateTemplateThumbnailResultMock, refreshTemplatesMock, refreshContentFeedMock, toastMock } =
  vi.hoisted(() => ({
    regenerateTemplateThumbnailResultMock: vi.fn(),
    refreshTemplatesMock: vi.fn(),
    refreshContentFeedMock: vi.fn(),
    toastMock: { success: vi.fn(), error: vi.fn(), info: vi.fn() },
  }));

const templateItem = {
  type: "template",
  id: "tmpl-1",
  name: "Bus network analysis",
  space_id: "space-1",
  folder_id: "folder-1",
  updated_at: "2026-09-01T00:00:00Z",
  created_at: "2026-09-01T00:00:00Z",
  my_role: "owner",
  shared_with: null,
  is_public: false,
  is_shortcut: false,
  restricted: false,
  restricted_inherited: false,
  template_payload_kind: "workflow",
} as unknown as ContentItem;

vi.mock("react-i18next", () => ({
  useTranslation: () => ({ t: (key: string) => key }),
  Trans: () => null,
}));
vi.mock("react-toastify", () => ({ toast: toastMock }));

vi.mock("@/lib/templates/thumbnailSnapshot", () => ({
  regenerateTemplateThumbnail: vi.fn(),
  regenerateTemplateThumbnailResult: regenerateTemplateThumbnailResultMock,
}));
vi.mock("@/lib/api/templates", () => ({
  publishTemplateWithDetail: vi.fn(),
  refreshTemplate: vi.fn(),
  refreshTemplates: refreshTemplatesMock,
  unpublishTemplate: vi.fn(),
  useTemplate: () => ({ template: undefined }),
}));

vi.mock("@/lib/api/assets", () => ({ useDocuments: () => ({ documents: [] }) }));
vi.mock("@/lib/api/content", () => ({
  refreshContentFeed: refreshContentFeedMock,
  useContent: () => ({ page: { items: [templateItem], total: 1, page: 1, size: 50 }, isLoading: false }),
  useSharedWithSpace: () => ({ page: undefined }),
  useSpaces: () => ({ spaces: [] }),
}));
vi.mock("@/lib/api/folders", () => ({ useFolders: () => ({ folders: [] }) }));
vi.mock("@/lib/providers/ContentUiStateProvider", () => ({
  useContentUiState: () => ({
    openSections: {
      folders: true,
      shortcuts: true,
      projects: true,
      templates: true,
      datasets: true,
      documents: true,
      shared_with_space: true,
    },
    toggleSection: vi.fn(),
    detailsOpen: false,
    setDetailsOpen: vi.fn(),
    toggleDetails: vi.fn(),
  }),
}));

vi.mock("@/hooks/dashboard/content/useContentActions", () => ({
  canActOn: () => true,
  canMoveAll: (items: unknown[]) => items.length > 0,
  useContentActions: () => ({
    getMenuItems: () => [
      { id: ContentActions.REGENERATE_THUMBNAIL, label: "regenerate_thumbnail" },
      { id: ContentActions.RENAME, label: "rename" },
    ],
  }),
}));
vi.mock("@/hooks/dashboard/content/useContentDrag", () => ({
  useContentDrag: () => ({
    dragIds: [],
    dropTarget: null,
    startDrag: vi.fn(),
    endDrag: vi.fn(),
    setDropTarget: vi.fn(),
    canDropOnSpace: vi.fn(),
  }),
}));
vi.mock("@/hooks/dashboard/content/useContentPageState", () => ({
  readStoredPersonalSpaceId: () => null,
  writeStoredPersonalSpaceId: vi.fn(),
  useContentPageState: () => ({
    active: { kind: "space", spaceId: null },
    folderId: undefined,
    search: "",
    setSearch: vi.fn(),
    types: [],
    setTypes: vi.fn(),
    orderBy: "updated_at",
    order: "descendent",
    setSort: vi.fn(),
    layout: "grid",
    setLayout: vi.fn(),
    goSpace: vi.fn(),
    goView: vi.fn(),
    goFolder: vi.fn(),
    goFolderIn: vi.fn(),
    feedParams: { view: "space" },
  }),
}));
vi.mock("@/hooks/dashboard/content/useContentSelection", () => ({
  useContentSelection: () => ({
    selected: new Set(),
    selectedItems: [],
    anySelected: false,
    toggle: vi.fn(),
    selectOnly: vi.fn(),
    clear: vi.fn(),
  }),
}));
vi.mock("@/hooks/jobs/JobStatus", () => ({ useJobStatus: vi.fn() }));

// The card is the seam this test drives; every other card/dialog/panel the
// page can render is a no-op stub, and the section wrapper renders its
// children so the template card is on screen.
vi.mock("@/components/dashboard/content/ContentCard", () => ({
  default: ({
    item,
    menuItems,
    onMenuSelect,
  }: {
    item: ContentItem;
    menuItems: PopperMenuItem[];
    onMenuSelect: (menuItem: PopperMenuItem) => void;
  }) => (
    <div>
      {menuItems.map((menuItem) => (
        <button key={menuItem.id} onClick={() => onMenuSelect(menuItem)}>
          {`${menuItem.id}-${item.id}`}
        </button>
      ))}
    </div>
  ),
}));
vi.mock("@/components/dashboard/content/ContentSection", () => ({
  default: ({ children }: { children: React.ReactNode }) => <div>{children}</div>,
}));
vi.mock("@/components/dashboard/common/DocumentCard", () => ({ default: () => null }));
vi.mock("@/components/dashboard/common/EmptyState", () => ({ default: () => null }));
vi.mock("@/components/dashboard/content/ContentActionBar", () => ({ default: () => null }));
vi.mock("@/components/dashboard/content/ContentActionDialogs", () => ({ default: () => null }));
vi.mock("@/components/dashboard/content/ContentAddMenu", () => ({ default: () => null }));
vi.mock("@/components/dashboard/content/ContentBreadcrumb", () => ({ default: () => null }));
vi.mock("@/components/dashboard/content/ContentDeleteDialog", () => ({ default: () => null }));
vi.mock("@/components/dashboard/content/ContentDetailsPanel", () => ({ default: () => null }));
vi.mock("@/components/dashboard/content/ContentFolderCard", () => ({ default: () => null }));
vi.mock("@/components/dashboard/content/ContentKebab", () => ({ default: () => null }));
vi.mock("@/components/dashboard/content/ContentPreviewDialog", () => ({ default: () => null }));
vi.mock("@/components/dashboard/content/ContentRow", () => ({ default: () => null }));
vi.mock("@/components/dashboard/content/ContentSpacesPanel", () => ({ default: () => null }));
vi.mock("@/components/dashboard/content/ContentToolbar", () => ({ default: () => null }));
vi.mock("@/components/dashboard/content/ShortcutTile", () => ({ default: () => null }));
vi.mock("@/components/modals/content/ShareDialog", () => ({ default: () => null }));
vi.mock("@/components/modals/content/SpaceSettingsDialog", () => ({ default: () => null }));
vi.mock("@/components/modals/content/TrashDialog", () => ({ default: () => null }));
vi.mock("@/components/modals/content/TransferDialog", () => ({ default: () => null }));
vi.mock("@/components/modals/content/MoveDialog", () => ({
  default: () => null,
  folderDepthViolation: () => false,
  moveContentItems: vi.fn(),
  withDescendants: () => [],
}));
vi.mock("@/lib/utils/content", () => ({
  folderLocationLabel: () => undefined,
  homeFolderOf: () => undefined,
  iconFor: () => "folder",
  lastRoleSegment: () => undefined,
  sectionOf: () => "templates",
  spaceDisplayName: () => "",
  spaceIconFor: () => "folder",
}));

const pick = async () => {
  render(<ContentPage />);
  await userEvent.click(await screen.findByRole("button", { name: "regenerateThumbnail-tmpl-1" }));
};

describe("ContentPage — regenerate a template's thumbnail", () => {
  beforeEach(() => {
    regenerateTemplateThumbnailResultMock.mockReset();
    refreshTemplatesMock.mockReset();
    refreshContentFeedMock.mockReset();
    toastMock.success.mockReset();
    toastMock.error.mockReset();
    toastMock.info.mockReset();
    // jsdom has no matchMedia; MUI's useMediaQuery (the mobile/desktop split
    // this page reads throughout) calls it on every render.
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

  it("redraws the picked template and refreshes both listings", async () => {
    regenerateTemplateThumbnailResultMock.mockResolvedValue({
      status: "updated",
      template: { id: "tmpl-1" },
    });

    await pick();

    await waitFor(() => expect(toastMock.success).toHaveBeenCalledWith("template_thumbnail_updated"));
    expect(regenerateTemplateThumbnailResultMock).toHaveBeenCalledWith(
      expect.objectContaining({ id: "tmpl-1", name: "Bus network analysis" }),
      expect.any(Function)
    );
    expect(refreshTemplatesMock).toHaveBeenCalled();
    expect(refreshContentFeedMock).toHaveBeenCalled();
  });

  it("says so and changes nothing when the config is not answered", async () => {
    regenerateTemplateThumbnailResultMock.mockResolvedValue({ status: "no_config" });

    await pick();

    await waitFor(() => expect(toastMock.info).toHaveBeenCalledWith("template_thumbnail_no_config"));
    expect(refreshTemplatesMock).not.toHaveBeenCalled();
    expect(toastMock.success).not.toHaveBeenCalled();
  });

  it("says so and changes nothing when there is no picture in the config", async () => {
    regenerateTemplateThumbnailResultMock.mockResolvedValue({ status: "nothing_to_draw" });

    await pick();

    await waitFor(() => expect(toastMock.info).toHaveBeenCalledWith("template_thumbnail_nothing_to_draw"));
    expect(refreshTemplatesMock).not.toHaveBeenCalled();
  });

  it("says so and changes nothing when the upload fails", async () => {
    regenerateTemplateThumbnailResultMock.mockResolvedValue({
      status: "failed",
      error: new Error("507"),
    });

    await pick();

    await waitFor(() => expect(toastMock.error).toHaveBeenCalledWith("error_updating_template"));
    expect(refreshTemplatesMock).not.toHaveBeenCalled();
    expect(toastMock.success).not.toHaveBeenCalled();
  });
});
