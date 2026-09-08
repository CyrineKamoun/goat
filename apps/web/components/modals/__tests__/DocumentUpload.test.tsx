import { fireEvent, render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type { Folder } from "@/lib/validations/folder";

import DocumentUploadModal from "@/components/modals/DocumentUpload";

const { useFoldersMock, getWritableFoldersMock } = vi.hoisted(() => ({
  useFoldersMock: vi.fn(),
  getWritableFoldersMock: vi.fn(),
}));

vi.mock("react-i18next", () => ({ useTranslation: () => ({ t: (key: string) => key }) }));
vi.mock("react-toastify", () => ({ toast: { success: vi.fn(), error: vi.fn() } }));
vi.mock("@/lib/api/folders", () => ({
  useFolders: useFoldersMock,
  getWritableFolders: getWritableFoldersMock,
}));
vi.mock("@/lib/api/assets", () => ({ uploadAsset: vi.fn() }));
vi.mock("@/lib/api/content", () => ({
  useSpaces: () => ({
    spaces: [
      {
        id: "space-1",
        kind: "personal",
        name: "My Content",
        default_role: "viewer",
        my_role: "owner",
        team_id: null,
        organization_id: null,
      },
    ],
  }),
}));
vi.mock("@/components/dashboard/common/FolderBrowser", () => ({ default: () => null }));

const homeFolder: Folder = {
  id: "home-1",
  name: "home",
  parent_id: null,
  space_id: "space-1",
  depth: 0,
  is_owned: true,
  restricted: false,
};

/** A folder the caller may read but not write into. */
const readOnlyFolder: Folder = {
  id: "folder-ro",
  name: "Shared reports",
  parent_id: null,
  space_id: "space-1",
  depth: 0,
  is_owned: false,
  role: "folder-viewer",
  restricted: false,
};

const writableFolder: Folder = { ...readOnlyFolder, id: "folder-rw", role: "folder-editor" };

/** Picks a file, which is the other half of what `Upload` waits for. */
const pickFile = () => {
  const input = document.getElementById("document-file-input") as HTMLInputElement;
  fireEvent.change(input, {
    target: { files: [new File(["x"], "report.pdf", { type: "application/pdf" })] },
  });
};

const uploadButton = () => screen.getByRole("button", { name: "upload" }) as HTMLButtonElement;

describe("DocumentUpload", () => {
  beforeEach(() => {
    useFoldersMock.mockReset().mockReturnValue({ folders: [] });
    getWritableFoldersMock.mockReset().mockReturnValue([]);
  });

  it("offers a file picker and the upload action", () => {
    render(<DocumentUploadModal open onClose={() => {}} />);

    expect(screen.getByText("upload_document")).toBeInTheDocument();
    expect(screen.getByText("select_file")).toBeInTheDocument();
    expect(uploadButton()).toBeInTheDocument();
  });

  it("refuses the upload until both a folder and a file are chosen", () => {
    render(<DocumentUploadModal open onClose={() => {}} />);

    expect(uploadButton().disabled).toBe(true);
  });

  it("refuses the upload while the folder it was opened in is read-only", () => {
    useFoldersMock.mockReturnValue({ folders: [homeFolder, readOnlyFolder] });
    getWritableFoldersMock.mockReturnValue([homeFolder]);

    render(<DocumentUploadModal open defaultFolderId="folder-ro" onClose={() => {}} />);
    pickFile();

    // The file is picked and a folder is standing in, but writing into it
    // would only come back a 403.
    expect(screen.getByText("report.pdf")).toBeInTheDocument();
    expect(uploadButton().disabled).toBe(true);
  });

  it("takes the upload once the folder it was opened in is writable", () => {
    useFoldersMock.mockReturnValue({ folders: [homeFolder, writableFolder] });
    getWritableFoldersMock.mockReturnValue([homeFolder, writableFolder]);

    render(<DocumentUploadModal open defaultFolderId="folder-rw" onClose={() => {}} />);
    pickFile();

    expect(uploadButton().disabled).toBe(false);
  });
});
