import { fireEvent, render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type { TemplateRead } from "@/lib/validations/template";

import TemplatePreviewDialog from "@/components/templates/TemplatePreviewDialog";

const { useTemplateMock } = vi.hoisted(() => ({ useTemplateMock: vi.fn() }));

vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    t: (key: string, opts?: Record<string, unknown>) => (opts ? `${key}:${JSON.stringify(opts)}` : key),
  }),
}));
vi.mock("@/lib/api/templates", () => ({
  useTemplate: (...args: unknown[]) => useTemplateMock(...args),
}));

const template = (overrides: Partial<TemplateRead> = {}): TemplateRead => ({
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
  catalog_status: "none",
  source_ref: {},
  datasets_needing_share: [],
  my_role: "owner",
  created_at: "2026-01-01T00:00:00Z",
  updated_at: "2026-01-01T00:00:00Z",
  ...overrides,
});

beforeEach(() => {
  useTemplateMock.mockReset().mockReturnValue({ template: undefined, isLoading: false, isError: undefined });
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

describe("TemplatePreviewDialog", () => {
  it("names the template in the header and in the preview", () => {
    render(
      <TemplatePreviewDialog
        open
        template={template()}
        onClose={vi.fn()}
        onUse={vi.fn()}
        sourceLabel="My Content"
      />
    );

    // Once as the dialog's title, once as the panel's own heading.
    expect(screen.getAllByText("Bus network analysis")).toHaveLength(2);
    expect(screen.getByText("My Content")).toBeInTheDocument();
    expect(screen.getByText("template_kind_workflow")).toBeInTheDocument();
  });

  it("hands the template over on Use template", () => {
    const onUse = vi.fn();
    render(<TemplatePreviewDialog open template={template()} onClose={vi.fn()} onUse={onUse} />);

    fireEvent.click(screen.getByRole("button", { name: "use_template" }));

    expect(onUse).toHaveBeenCalledTimes(1);
  });

  it("closes from Cancel", () => {
    const onClose = vi.fn();
    render(<TemplatePreviewDialog open template={template()} onClose={onClose} onUse={vi.fn()} />);

    fireEvent.click(screen.getByRole("button", { name: "cancel" }));

    expect(onClose).toHaveBeenCalledTimes(1);
  });

  it("shows a skeleton under the generic title while the template loads, with Use refused", () => {
    const { baseElement } = render(
      <TemplatePreviewDialog open template={undefined} loading onClose={vi.fn()} onUse={vi.fn()} />
    );

    expect(screen.getByText("template")).toBeInTheDocument();
    expect(baseElement.querySelectorAll(".MuiSkeleton-root").length).toBeGreaterThan(0);
    expect(screen.queryByTestId("template-preview-step")).not.toBeInTheDocument();
    // Read off the DOM property rather than a matcher: the repo's ESLint
    // setup reads jest-dom's `toBeDisabled` as Playwright's async matcher of
    // the same name.
    const use = screen.getByRole("button", { name: "use_template" }) as HTMLButtonElement;
    expect(use.disabled).toBe(true);
  });

  it("draws the payload's default picture for a template with no picture of its own", () => {
    render(<TemplatePreviewDialog open template={template()} onClose={vi.fn()} onUse={vi.fn()} />);

    // The same default the card shows, and still no structure reconstructed
    // for a reader: a chain of blank steps, not this workflow's nodes.
    expect(screen.getByTestId("template-default-workflow")).toBeInTheDocument();
    expect(screen.queryByTestId("template-preview-node")).not.toBeInTheDocument();
  });

  it("draws a layout's page in the orientation it prints on", () => {
    render(
      <TemplatePreviewDialog
        open
        template={template({
          payload_kind: "layout",
          kinds: ["layout"],
          page_size: "A4",
          page_orientation: "landscape",
        })}
        onClose={vi.fn()}
        onUse={vi.fn()}
      />
    );

    const page = screen.getByTestId("template-preview-page");
    expect(page.getAttribute("data-orientation")).toBe("landscape");
    // A blank page, not a wireframe of the frozen layout.
    expect(screen.queryByTestId("template-preview-element")).not.toBeInTheDocument();
  });

  it("shows the template mark for a project payload with no picture", () => {
    render(
      <TemplatePreviewDialog
        open
        template={template({ payload_kind: "project", kinds: [] })}
        onClose={vi.fn()}
        onUse={vi.fn()}
      />
    );

    expect(screen.getByTestId("content-thumbnail-mark")).toBeInTheDocument();
  });

  it("says which page a layout template prints on, in words and in millimetres", () => {
    render(
      <TemplatePreviewDialog
        open
        template={template({
          payload_kind: "layout",
          kinds: ["layout"],
          page_size: "A4",
          page_orientation: "landscape",
        })}
        onClose={vi.fn()}
        onUse={vi.fn()}
      />
    );

    // The mocked `t` echoes its key and options, so the orientation reads
    // as its key rather than as the word.
    expect(screen.getByText(/^A4 · landscape/)).toBeInTheDocument();
    expect(screen.getByTestId("template-preview-page-size").textContent).toBe(
      'page_dimensions_mm:{"width":297,"height":210}'
    );
  });

  it("says nothing about a page for a template that prints on none", () => {
    render(<TemplatePreviewDialog open template={template()} onClose={vi.fn()} onUse={vi.fn()} />);

    expect(screen.queryByTestId("template-preview-page-size")).not.toBeInTheDocument();
  });

  it("reads nothing of its own — everything it shows came with the template", () => {
    render(<TemplatePreviewDialog open template={template()} onClose={vi.fn()} onUse={vi.fn()} />);

    expect(useTemplateMock).not.toHaveBeenCalled();
  });

  it("renders nothing while closed", () => {
    render(<TemplatePreviewDialog open={false} template={template()} onClose={vi.fn()} onUse={vi.fn()} />);

    expect(screen.queryByText("Bus network analysis")).not.toBeInTheDocument();
  });
});
