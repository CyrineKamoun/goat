import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import ContentDeleteModal from "@/components/modals/ContentDelete";

vi.mock("react-i18next", () => ({
  useTranslation: () => ({ t: (key: string) => key }),
  Trans: ({ i18nKey }: { i18nKey: string }) => <>{i18nKey}</>,
}));
vi.mock("react-toastify", () => ({ toast: { success: vi.fn(), error: vi.fn() } }));
vi.mock("swr", () => ({ mutate: vi.fn() }));
vi.mock("@/lib/api/bundles", () => ({ deleteBundle: vi.fn(), isBundleTile: () => false }));
vi.mock("@/lib/api/datasets", () => ({ matchesContentListKey: vi.fn() }));
vi.mock("@/lib/api/layers", () => ({ deleteLayer: vi.fn() }));
vi.mock("@/lib/api/projects", () => ({ PROJECTS_API_BASE_URL: "/projects", deleteProject: vi.fn() }));

// eslint-disable-next-line @typescript-eslint/no-explicit-any
const layer = { id: "layer-1", name: "Roads" } as any;

describe("ContentDelete", () => {
  it("names what is being deleted and offers a delete action", () => {
    render(<ContentDeleteModal open type="layer" content={layer} onClose={() => {}} />);

    expect(screen.getByText("delete_layer")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "delete" })).toBeInTheDocument();
  });

  it("refuses the delete while the caller has it disabled", () => {
    render(<ContentDeleteModal open disabled type="layer" content={layer} onClose={() => {}} />);

    expect((screen.getByRole("button", { name: "delete" }) as HTMLButtonElement).disabled).toBe(true);
  });
});
