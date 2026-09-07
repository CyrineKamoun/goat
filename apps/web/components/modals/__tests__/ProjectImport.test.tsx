import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import ProjectImportModal from "@/components/modals/ProjectImport";

vi.mock("react-i18next", () => ({ useTranslation: () => ({ t: (key: string) => key }) }));
vi.mock("react-toastify", () => ({ toast: { success: vi.fn(), error: vi.fn() } }));
vi.mock("@/lib/api/datasets", () => ({ requestDatasetUpload: vi.fn() }));
vi.mock("@/lib/api/folders", () => ({ useFolders: () => ({ folders: [] }) }));
vi.mock("@/lib/api/processes", () => ({ executeProcessAsync: vi.fn() }));
vi.mock("@/lib/services/s3", () => ({ uploadFileToS3: vi.fn() }));
vi.mock("@/lib/store/jobs/slice", () => ({ setRunningJobIds: vi.fn() }));
vi.mock("@/hooks/store/ContextHooks", () => ({
  useAppDispatch: () => vi.fn(),
  useAppSelector: () => [],
}));
vi.mock("@/components/common/form-inputs/AutocompleteField", () => ({
  RhfAutocompleteField: () => null,
}));
vi.mock("@/components/common/FileInput", () => ({ MuiFileInput: () => null }));

describe("ProjectImport", () => {
  it("offers the archive picker and the import action", () => {
    render(<ProjectImportModal open onClose={() => {}} />);

    expect(screen.getByText("import_project")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "import" })).toBeInTheDocument();
  });

  it("refuses the import until a folder and an archive are chosen", () => {
    render(<ProjectImportModal open onClose={() => {}} />);

    expect((screen.getByRole("button", { name: "import" }) as HTMLButtonElement).disabled).toBe(true);
  });
});
