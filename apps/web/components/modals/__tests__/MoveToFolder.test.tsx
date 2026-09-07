import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import ContentMoveToFolderModal from "@/components/modals/MoveToFolder";

vi.mock("react-i18next", () => ({ useTranslation: () => ({ t: (key: string) => key }) }));
vi.mock("react-toastify", () => ({ toast: { success: vi.fn(), error: vi.fn() } }));
vi.mock("swr", () => ({ mutate: vi.fn() }));
vi.mock("@/lib/api/bundles", () => ({ isBundleTile: () => false, updateBundle: vi.fn() }));
vi.mock("@/lib/api/datasets", () => ({ matchesContentListKey: vi.fn() }));
vi.mock("@/lib/api/folders", () => ({
  useFolders: () => ({ folders: [] }),
  getWritableFolders: () => [],
}));
vi.mock("@/lib/api/layers", () => ({ updateDataset: vi.fn() }));
vi.mock("@/lib/api/projects", () => ({ PROJECTS_API_BASE_URL: "/projects", updateProject: vi.fn() }));
vi.mock("@/lib/api/content", () => ({ useSpaces: () => ({ spaces: [] }) }));
vi.mock("@/components/dashboard/common/FolderBrowser", () => ({ default: () => null }));

// eslint-disable-next-line @typescript-eslint/no-explicit-any
const layer = { id: "layer-1", name: "Roads", folder_id: "folder-1" } as any;

describe("MoveToFolder", () => {
  it("asks where to move the content", () => {
    render(<ContentMoveToFolderModal open type="layer" content={layer} onClose={() => {}} />);

    expect(screen.getByText("move_to")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "move" })).toBeInTheDocument();
  });

  it("refuses the move until a destination folder is picked", () => {
    render(<ContentMoveToFolderModal open type="layer" content={layer} onClose={() => {}} />);

    expect((screen.getByRole("button", { name: "move" }) as HTMLButtonElement).disabled).toBe(true);
  });
});
