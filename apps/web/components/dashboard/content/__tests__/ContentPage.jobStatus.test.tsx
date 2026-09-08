import { render } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import ContentPage from "@/components/dashboard/content/ContentPage";

// ContentPage wires `useJobStatus` to `refreshContentFeed` so that a background
// import job finishing after its dialog has already closed (see `useUploadFlow`)
// still lands in an open Content page — this is the fix for the "new dataset
// never shows without a reload" bug. Everything else the page pulls in (feed
// data, folders, spaces, selection/drag state, every card/dialog it can render)
// is mocked to isolate that one wiring: the page has no other seams narrow
// enough to assert this through, and there is no existing precedent in this
// repo for exercising a dashboard page shell end to end in a unit test.

const { useJobStatusMock, refreshContentFeedMock } = vi.hoisted(() => ({
  useJobStatusMock: vi.fn(),
  refreshContentFeedMock: vi.fn(),
}));

vi.mock("react-i18next", () => ({
  useTranslation: () => ({ t: (key: string) => key }),
  Trans: () => null,
}));
vi.mock("react-toastify", () => ({ toast: { success: vi.fn(), error: vi.fn(), info: vi.fn() } }));

vi.mock("@/lib/api/assets", () => ({ useDocuments: () => ({ documents: [] }) }));
vi.mock("@/lib/api/content", () => ({
  refreshContentFeed: refreshContentFeedMock,
  useContent: () => ({ page: undefined, isLoading: false }),
  useSharedWithSpace: () => ({ page: undefined }),
  useSpaces: () => ({ spaces: [] }),
}));
vi.mock("@/lib/api/folders", () => ({ useFolders: () => ({ folders: [] }) }));
vi.mock("@/lib/providers/ContentUiStateProvider", () => ({
  useContentUiState: () => ({
    openSections: {
      folders: false,
      shortcuts: false,
      projects: false,
      datasets: false,
      documents: false,
      shared_with_space: false,
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
  useContentActions: () => ({ getMenuItems: () => [] }),
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
    feedParams: null,
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
vi.mock("@/hooks/jobs/JobStatus", () => ({ useJobStatus: useJobStatusMock }));

// Every card/dialog/panel the page can render — none of it is exercised here,
// so each is a no-op stub. `vi.mock`'s hoisting only recognises a literal
// path, so these cannot be generated from a loop.
vi.mock("@/components/dashboard/common/DocumentCard", () => ({ default: () => null }));
vi.mock("@/components/dashboard/common/EmptyState", () => ({ default: () => null }));
vi.mock("@/components/dashboard/content/ContentActionBar", () => ({ default: () => null }));
vi.mock("@/components/dashboard/content/ContentActionDialogs", () => ({ default: () => null }));
vi.mock("@/components/dashboard/content/ContentAddMenu", () => ({ default: () => null }));
vi.mock("@/components/dashboard/content/ContentBreadcrumb", () => ({ default: () => null }));
vi.mock("@/components/dashboard/content/ContentCard", () => ({ default: () => null }));
vi.mock("@/components/dashboard/content/ContentDeleteDialog", () => ({ default: () => null }));
vi.mock("@/components/dashboard/content/ContentDetailsPanel", () => ({ default: () => null }));
vi.mock("@/components/dashboard/content/ContentFolderCard", () => ({ default: () => null }));
vi.mock("@/components/dashboard/content/ContentKebab", () => ({ default: () => null }));
vi.mock("@/components/dashboard/content/ContentPreviewDialog", () => ({ default: () => null }));
vi.mock("@/components/dashboard/content/ContentRow", () => ({ default: () => null }));
vi.mock("@/components/dashboard/content/ContentSection", () => ({ default: () => null }));
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
  homeFolderOf: () => undefined,
  iconFor: () => "folder",
  lastRoleSegment: () => undefined,
  sectionOf: () => "datasets",
  spaceDisplayName: () => "",
  spaceIconFor: () => "folder",
}));

describe("ContentPage job-status wiring", () => {
  beforeEach(() => {
    useJobStatusMock.mockReset();
    refreshContentFeedMock.mockReset();
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

  it("revalidates the feed on both job success and job failure", () => {
    render(<ContentPage />);

    expect(useJobStatusMock).toHaveBeenCalledTimes(1);
    expect(useJobStatusMock).toHaveBeenCalledWith(refreshContentFeedMock, refreshContentFeedMock);
  });
});
