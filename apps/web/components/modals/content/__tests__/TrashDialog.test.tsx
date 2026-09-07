import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type { Space, TrashItem } from "@/lib/validations/content";

const { useTrashMock, restoreContentMock, refreshContentFeedMock, mutateMock, toastSuccessMock, toastErrorMock } =
  vi.hoisted(() => ({
    useTrashMock: vi.fn(),
    restoreContentMock: vi.fn(),
    refreshContentFeedMock: vi.fn(),
    mutateMock: vi.fn(),
    toastSuccessMock: vi.fn(),
    toastErrorMock: vi.fn(),
  }));

vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    t: (key: string, vars?: Record<string, unknown>) => (vars ? `${key}:${JSON.stringify(vars)}` : key),
    i18n: { language: "en" },
  }),
}));
vi.mock("react-toastify", () => ({ toast: { success: toastSuccessMock, error: toastErrorMock } }));
vi.mock("swr", () => ({ mutate: vi.fn() }));
vi.mock("@/lib/api/content", () => ({
  useTrash: useTrashMock,
  restoreContent: restoreContentMock,
  refreshContentFeed: refreshContentFeedMock,
}));

import TrashDialog from "@/components/modals/content/TrashDialog";

const space: Space = {
  id: "space-1",
  kind: "personal",
  name: "My Content",
  default_role: "viewer",
  my_role: "owner",
  team_id: null,
  organization_id: null,
};

const items: TrashItem[] = [
  {
    type: "layer",
    id: "layer-1",
    name: "Layer One",
    deleted_at: "2026-08-01T00:00:00Z",
    purge_after: "2026-08-31T00:00:00Z",
  },
  {
    type: "project",
    id: "project-1",
    name: "Project One",
    deleted_at: "2026-08-05T00:00:00Z",
    purge_after: "2026-09-04T00:00:00Z",
  },
];

const noop = () => {};

describe("TrashDialog", () => {
  beforeEach(() => {
    useTrashMock.mockReset();
    restoreContentMock.mockReset();
    refreshContentFeedMock.mockReset();
    mutateMock.mockReset();
    toastSuccessMock.mockReset();
    toastErrorMock.mockReset();
  });

  it("lists both trashed items with a restore button each", () => {
    useTrashMock.mockReturnValue({ items, isLoading: false, mutate: mutateMock });

    render(<TrashDialog space={space} onClose={noop} />);

    expect(screen.getByText("Layer One")).toBeInTheDocument();
    expect(screen.getByText("Project One")).toBeInTheDocument();
    expect(screen.getAllByRole("button", { name: "restore" })).toHaveLength(2);
  });

  it("restores a single item and refreshes the trash list and feed", async () => {
    useTrashMock.mockReturnValue({ items, isLoading: false, mutate: mutateMock });
    restoreContentMock.mockResolvedValue(undefined);

    render(<TrashDialog space={space} onClose={noop} />);

    const restoreButtons = screen.getAllByRole("button", { name: "restore" });
    fireEvent.click(restoreButtons[0]);

    await waitFor(() =>
      expect(restoreContentMock).toHaveBeenCalledWith([{ type: "layer", id: "layer-1" }])
    );
    expect(mutateMock).toHaveBeenCalled();
    expect(refreshContentFeedMock).toHaveBeenCalled();
    expect(toastSuccessMock).toHaveBeenCalledWith("restore_success");
  });

  it("selects all via the header checkbox and restores the whole selection", async () => {
    useTrashMock.mockReturnValue({ items, isLoading: false, mutate: mutateMock });
    restoreContentMock.mockResolvedValue(undefined);

    render(<TrashDialog space={space} onClose={noop} />);

    fireEvent.click(screen.getByRole("checkbox", { name: "select_all" }));

    expect(screen.getByRole("button", { name: 'restore_selected:{"count":2}' })).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: 'restore_selected:{"count":2}' }));

    await waitFor(() =>
      expect(restoreContentMock).toHaveBeenCalledWith([
        { type: "layer", id: "layer-1" },
        { type: "project", id: "project-1" },
      ])
    );
  });

  it("shows the empty state when there is nothing in the trash", () => {
    useTrashMock.mockReturnValue({ items: [], isLoading: false, mutate: mutateMock });

    render(<TrashDialog space={space} onClose={noop} />);

    expect(screen.getByText("trash_empty")).toBeInTheDocument();
  });

  it("shows an error toast and keeps the dialog open when restore fails", async () => {
    useTrashMock.mockReturnValue({ items, isLoading: false, mutate: mutateMock });
    restoreContentMock.mockRejectedValue(new Error("boom"));
    const onClose = vi.fn();

    render(<TrashDialog space={space} onClose={onClose} />);

    fireEvent.click(screen.getAllByRole("button", { name: "restore" })[0]);

    await waitFor(() => expect(toastErrorMock).toHaveBeenCalledWith("restore_failed"));
    expect(onClose).not.toHaveBeenCalled();
  });
});
