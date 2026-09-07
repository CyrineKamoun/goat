import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type { ContentItem } from "@/lib/validations/content";

const { deleteFolderMock, deleteProjectMock, deleteLayerMock, deleteBundleMock, refreshContentFeedMock, mutateMock, toastSuccessMock, toastErrorMock } =
  vi.hoisted(() => ({
    deleteFolderMock: vi.fn(),
    deleteProjectMock: vi.fn(),
    deleteLayerMock: vi.fn(),
    deleteBundleMock: vi.fn(),
    refreshContentFeedMock: vi.fn(),
    mutateMock: vi.fn(),
    toastSuccessMock: vi.fn(),
    toastErrorMock: vi.fn(),
  }));

vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    t: (key: string, vars?: Record<string, unknown>) => (vars ? `${key}:${JSON.stringify(vars)}` : key),
  }),
}));
vi.mock("react-toastify", () => ({ toast: { success: toastSuccessMock, error: toastErrorMock } }));
vi.mock("swr", () => ({ mutate: mutateMock }));
vi.mock("@/lib/api/content", () => ({ refreshContentFeed: refreshContentFeedMock }));
vi.mock("@/lib/api/datasets", () => ({ matchesContentListKey: vi.fn() }));
vi.mock("@/lib/api/folders", () => ({ deleteFolder: deleteFolderMock }));
vi.mock("@/lib/api/projects", () => ({ deleteProject: deleteProjectMock }));
vi.mock("@/lib/api/layers", () => ({ deleteLayer: deleteLayerMock }));
vi.mock("@/lib/api/bundles", () => ({ deleteBundle: deleteBundleMock }));

import ContentDeleteDialog from "@/components/dashboard/content/ContentDeleteDialog";

const layerItem: ContentItem = {
  type: "layer",
  id: "layer-1",
  name: "A layer",
  space_id: "space-1",
  updated_at: "2026-01-01T00:00:00Z",
  created_at: "2026-01-01T00:00:00Z",
  my_role: "owner",
  shared_with: null,
  is_shortcut: false,
  is_public: false,
  restricted: false,
  restricted_inherited: false,
};

const folderItem: ContentItem = {
  type: "folder",
  id: "folder-1",
  name: "A folder",
  space_id: "space-1",
  updated_at: "2026-01-01T00:00:00Z",
  created_at: "2026-01-01T00:00:00Z",
  my_role: "owner",
  shared_with: null,
  is_shortcut: false,
  is_public: false,
  restricted: false,
  restricted_inherited: false,
};

describe("ContentDeleteDialog", () => {
  beforeEach(() => {
    deleteFolderMock.mockReset().mockResolvedValue(undefined);
    deleteProjectMock.mockReset();
    deleteLayerMock.mockReset().mockResolvedValue(undefined);
    deleteBundleMock.mockReset();
    refreshContentFeedMock.mockReset();
    mutateMock.mockReset();
    toastSuccessMock.mockReset();
    toastErrorMock.mockReset();
  });

  it("deletes every item by its own type and reports success", async () => {
    const onClose = vi.fn();
    const onDeleted = vi.fn();
    render(<ContentDeleteDialog items={[layerItem, folderItem]} onClose={onClose} onDeleted={onDeleted} />);

    fireEvent.click(screen.getByText("delete"));

    await waitFor(() => expect(onDeleted).toHaveBeenCalledTimes(1));

    expect(deleteLayerMock).toHaveBeenCalledTimes(1);
    expect(deleteLayerMock).toHaveBeenCalledWith("layer-1");
    expect(deleteFolderMock).toHaveBeenCalledTimes(1);
    expect(deleteFolderMock).toHaveBeenCalledWith("folder-1");
    expect(toastSuccessMock).toHaveBeenCalledWith("deleted_moved_to_trash");
    expect(refreshContentFeedMock).toHaveBeenCalled();
    expect(onClose).toHaveBeenCalledTimes(1);
  });

  it("keeps deleting the rest and reports the failure count when one item fails", async () => {
    deleteLayerMock.mockRejectedValue(new Error("nope"));
    const onClose = vi.fn();
    const onDeleted = vi.fn();
    render(<ContentDeleteDialog items={[layerItem, folderItem]} onClose={onClose} onDeleted={onDeleted} />);

    fireEvent.click(screen.getByText("delete"));

    await waitFor(() => expect(onClose).toHaveBeenCalledTimes(1));

    expect(deleteLayerMock).toHaveBeenCalledWith("layer-1");
    expect(deleteFolderMock).toHaveBeenCalledWith("folder-1");
    expect(toastErrorMock).toHaveBeenCalledWith(`delete_partial_failed:${JSON.stringify({ count: 1 })}`);
    expect(onDeleted).not.toHaveBeenCalled();
  });
});
