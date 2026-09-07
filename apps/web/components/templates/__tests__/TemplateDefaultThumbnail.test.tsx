import { render, screen } from "@testing-library/react";
import type { ReactNode } from "react";
import { describe, expect, it } from "vitest";

import ThemeProvider from "@p4b/ui/theme/ThemeProvider";

import { LAYOUT_SNAPSHOT_PALETTE } from "@/lib/templates/layoutSnapshot";
import { layoutPageDescription } from "@/lib/templates/previewGeometry";
import { SNAPSHOT_PALETTE } from "@/lib/templates/workflowSnapshot";
import type { TemplateRead } from "@/lib/validations/template";

import TemplateDefaultThumbnail, {
  hasTemplateDefaultThumbnail,
} from "@/components/templates/TemplateDefaultThumbnail";

/** The page fields a template row carries, as the card and the panel read
 * them — the only input the default drawing is allowed. */
const pageOf = (
  page_size: string | null,
  page_orientation: "portrait" | "landscape" | null
): ReturnType<typeof layoutPageDescription> =>
  layoutPageDescription(
    { payload_kind: "layout", page_size, page_orientation } as TemplateRead,
    (key) => key
  );

/** `sx` compiles to a class, so the computed style is what to assert on. */
const withTheme = (mode: "light" | "dark", ui: ReactNode) =>
  render(<ThemeProvider settings={{ mode, locale: "en" }}>{ui}</ThemeProvider>);

/** jsdom reports colours as `rgb()`, the palettes carry `#rrggbb`. */
const rgb = (hex: string): string => {
  const value = parseInt(hex.replace("#", ""), 16);
  return `rgb(${(value >> 16) & 255}, ${(value >> 8) & 255}, ${value & 255})`;
};

describe("TemplateDefaultThumbnail", () => {
  it("draws a landscape page for a layout that prints landscape", () => {
    render(<TemplateDefaultThumbnail payloadKind="layout" page={pageOf("A3", "landscape")} />);

    const page = screen.getByTestId("template-preview-page");
    expect(page.getAttribute("data-orientation")).toBe("landscape");
    // A3 is 297×420mm, so landscape is the wider way round.
    expect(page).toHaveStyle({ aspectRatio: "420 / 297" });
  });

  it("draws a portrait page for a layout that prints portrait", () => {
    render(<TemplateDefaultThumbnail payloadKind="layout" page={pageOf("A3", "portrait")} />);

    const page = screen.getByTestId("template-preview-page");
    expect(page.getAttribute("data-orientation")).toBe("portrait");
    expect(page).toHaveStyle({ aspectRatio: "297 / 420" });
  });

  it("draws a neutral A4 page for a layout that stores no page", () => {
    render(<TemplateDefaultThumbnail payloadKind="layout" page={pageOf(null, null)} />);

    const page = screen.getByTestId("template-preview-page");
    expect(page.getAttribute("data-orientation")).toBe("portrait");
    expect(page).toHaveStyle({ aspectRatio: "210 / 297" });
  });

  it("draws a blank page rather than a wireframe of the real layout", () => {
    render(<TemplateDefaultThumbnail payloadKind="layout" page={pageOf("A4", "portrait")} />);

    // Three placeholder blocks, and nothing read from a config: no element
    // boxes, no glyphs, no drawing at all.
    expect(screen.getAllByTestId("template-page-block")).toHaveLength(3);
    expect(screen.queryByTestId("template-preview-element")).not.toBeInTheDocument();
  });

  it("draws a three-step chain for a workflow", () => {
    render(<TemplateDefaultThumbnail payloadKind="workflow" />);

    expect(screen.getByTestId("template-default-workflow")).toBeInTheDocument();
    expect(screen.getAllByTestId("template-preview-step")).toHaveLength(3);
    expect(screen.queryByTestId("template-preview-page")).not.toBeInTheDocument();
    // Nothing of a real workflow: no nodes, no edges.
    expect(screen.queryByTestId("template-preview-node")).not.toBeInTheDocument();
  });

  it("draws nothing for a project, which keeps its caller's mark", () => {
    const { container } = render(<TemplateDefaultThumbnail payloadKind="project" />);

    expect(container).toBeEmptyDOMElement();
    expect(hasTemplateDefaultThumbnail("project")).toBe(false);
    expect(hasTemplateDefaultThumbnail("layout")).toBe(true);
    expect(hasTemplateDefaultThumbnail("workflow")).toBe(true);
  });

  it.each(["light", "dark"] as const)("draws a layout's page on fixed light paper in %s mode", (mode) => {
    // The generated thumbnail is always light — white paper on the light
    // ground — so the default has to be light in both themes, or a card would
    // change appearance next to a template that has a real picture.
    const { unmount } = withTheme(
      mode,
      <TemplateDefaultThumbnail payloadKind="layout" page={pageOf("A4", "portrait")} />
    );

    const page = screen.getByTestId("template-preview-page");
    const style = getComputedStyle(page);
    expect(style.backgroundColor).toBe(rgb(LAYOUT_SNAPSHOT_PALETTE.page));
    expect(style.borderColor).toBe(rgb(LAYOUT_SNAPSHOT_PALETTE.pageBorder));
    expect(getComputedStyle(page.parentElement!).backgroundColor).toBe(rgb(LAYOUT_SNAPSHOT_PALETTE.ground));
    // The blocks are the snapshot palette's muted greys, not theme ink.
    expect(getComputedStyle(screen.getAllByTestId("template-page-block")[1]).backgroundColor).toBe(
      rgb(LAYOUT_SNAPSHOT_PALETTE.elementStroke)
    );
    unmount();
  });

  it.each(["light", "dark"] as const)("draws the workflow chain on the light canvas in %s mode", (mode) => {
    const { unmount } = withTheme(mode, <TemplateDefaultThumbnail payloadKind="workflow" />);

    const step = getComputedStyle(screen.getAllByTestId("template-preview-step")[0]);
    expect(step.backgroundColor).toBe(rgb(SNAPSHOT_PALETTE.nodeFill));
    expect(step.borderColor).toBe(rgb(SNAPSHOT_PALETTE.nodeStroke));
    expect(getComputedStyle(screen.getByTestId("template-default-workflow")).backgroundColor).toBe(
      rgb(SNAPSHOT_PALETTE.ground)
    );
    unmount();
  });

  it("draws the same picture in the preview column as on a card", () => {
    const { rerender } = render(
      <TemplateDefaultThumbnail payloadKind="layout" page={pageOf("A4", "landscape")} variant="card" />
    );

    expect(screen.getByTestId("template-preview-page").getAttribute("data-orientation")).toBe("landscape");

    rerender(
      <TemplateDefaultThumbnail payloadKind="layout" page={pageOf("A4", "landscape")} variant="panel" />
    );

    // Same drawing, only the frame and the block sizes change with the box.
    expect(screen.getByTestId("template-preview-page").getAttribute("data-orientation")).toBe("landscape");
    expect(screen.getAllByTestId("template-page-block")).toHaveLength(3);
  });

  it("draws the same picture on a browser row's tile, at the row's size", () => {
    const { rerender } = render(
      <TemplateDefaultThumbnail payloadKind="layout" page={pageOf("A3", "landscape")} variant="mark" />
    );

    expect(screen.getByTestId("template-preview-page").getAttribute("data-orientation")).toBe("landscape");
    expect(screen.getAllByTestId("template-page-block")).toHaveLength(3);

    rerender(<TemplateDefaultThumbnail payloadKind="workflow" variant="mark" />);

    // The chain keeps its steps at a tile's size; the bar inside a step
    // would be a single pixel there, so the step is drawn empty.
    expect(screen.getByTestId("template-default-workflow")).toBeInTheDocument();
    expect(screen.getAllByTestId("template-preview-step")).toHaveLength(3);
    for (const step of screen.getAllByTestId("template-preview-step")) {
      expect(step.childElementCount).toBe(0);
    }
  });
});
