import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { afterEach, describe, expect, it, vi } from "vitest";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import { NODE_ICON_TONES } from "@/lib/templates/nodeIcons";
import { ANNOTATION_FILL_OPACITY } from "@/lib/templates/workflowDrawing";
import {
  NOTE_CLASS,
  NOTE_CSS,
  SNAPSHOT_PALETTE,
  renderWorkflowSnapshot,
  svgForWorkflow,
} from "@/lib/templates/workflowSnapshot";
import type { TemplateWorkflowPreview } from "@/lib/validations/template";

const descriptor: TemplateWorkflowPreview = {
  kind: "workflow",
  nodes: [
    { label: "Stops", type: "dataset", x: 0, y: 0, w: 220, h: 92, icon: "point" },
    { label: "Buffer", type: "tool", x: 320, y: 0, w: 220, h: 108, icon: "buffer" },
    { label: "Save result", type: "export", x: 640, y: 0, w: 220, h: 92, icon: "export_dataset" },
  ],
  edges: [
    [0, 1],
    [1, 2],
  ],
};

/** The path data the `Icon` component draws a name as — read off the
 * component itself, so the assertion compares the drawing to what the canvas
 * renders rather than to a copy of the geometry. */
const iconPath = (iconName: ICON_NAME): string => {
  const path = /<path d="([^"]+)"/.exec(renderToStaticMarkup(createElement(Icon, { iconName })));
  if (!path) throw new Error(`No path drawn for ${iconName}`);
  return path[1];
};

/** An `Image` whose `src` setter reports the outcome the test asked for, so
 * the rasterising path can run in jsdom — which never loads an SVG data URL
 * on its own. */
const stubImage = (outcome: "load" | "error") => {
  class StubImage {
    onload: (() => void) | null = null;
    onerror: (() => void) | null = null;
    width = 0;
    height = 0;
    set src(_value: string) {
      setTimeout(() => (outcome === "load" ? this.onload?.() : this.onerror?.()), 0);
    }
  }
  vi.stubGlobal("Image", StubImage);
};

/** A canvas that hands back a 2D context and, on `toBlob`, whatever this
 * test wants of it. */
const stubCanvas = (blob: Blob | null) => {
  const context = {
    fillStyle: "",
    fillRect: vi.fn(),
    drawImage: vi.fn(),
  };
  vi.spyOn(HTMLCanvasElement.prototype, "getContext").mockReturnValue(
    context as unknown as CanvasRenderingContext2D
  );
  HTMLCanvasElement.prototype.toBlob = function toBlob(callback: BlobCallback) {
    callback(blob);
  };
  return context;
};

afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("svgForWorkflow", () => {
  it("fills the frame with a workflow smaller than it, rather than drawing it at canvas size", async () => {
    const twoNodes: TemplateWorkflowPreview = {
      kind: "workflow",
      nodes: [
        { label: "Stops", type: "dataset", x: 0, y: 0, w: 160, h: 68 },
        { label: "Buffer", type: "tool", x: 300, y: 0, w: 220, h: 68 },
      ],
      edges: [[0, 1]],
    };

    // The positioned rects: the node bodies and their swatches, not the two
    // full-frame grounds.
    const widths = [
      ...(await svgForWorkflow(twoNodes)).matchAll(/<rect x="[\d.]+" y="[\d.]+" width="([\d.]+)"/g),
    ].map((match) => Number(match[1]));

    // The nodes are drawn above their canvas size — a 520px-wide workflow on
    // a 1280×720 page would otherwise be a stamp in the middle of it.
    expect(Math.max(...widths)).toBeGreaterThan(220);
  });

  it("draws every node's label and one path per edge", async () => {
    const svg = await svgForWorkflow(descriptor);

    expect(svg.startsWith("<svg xmlns=")).toBe(true);
    expect(svg).toContain('width="1280"');
    expect(svg).toContain('height="720"');
    expect(svg).toContain(">Stops<");
    expect(svg).toContain(">Buffer<");
    expect(svg).toContain(">Save result<");
    // One path per edge.
    expect(svg.match(/<path d="M /g)).toHaveLength(2);
    // Three node cards, three icon wrappers, and the two background rects —
    // plus whatever rects the three glyphs are drawn out of.
    expect((svg.match(/<rect /g) ?? []).length).toBeGreaterThanOrEqual(8);
    // One nested icon svg per node, inside the drawing's own root svg.
    expect(svg.match(/<svg /g)).toHaveLength(4);
  });

  it("draws each node as the card its canvas node is, with that node's own icon", async () => {
    const svg = await svgForWorkflow(descriptor);

    // The dataset's point glyph — the geometry `DatasetNode` picks, not the
    // table default — drawn from the same component the canvas renders.
    expect(svg).toContain(iconPath(ICON_NAME.POINT_FEATURE));
    // A card: paper fill, the light divider border, and the shadow filter.
    expect(svg).toContain(`fill="${SNAPSHOT_PALETTE.nodeFill}"`);
    expect(svg).toContain(`stroke="${SNAPSHOT_PALETTE.nodeStroke}"`);
    expect(svg.match(/filter="url\(#goat-snapshot-shadow-\d+\)"/g)).toHaveLength(3);
    expect(svg).toContain("<feDropShadow");
    // Everything is drawn as SVG, so nothing depends on an HTML renderer.
    expect(svg).not.toContain("foreignObject");
  });

  it("draws each single-tone glyph in the colour its canvas node gives it", async () => {
    const svg = await svgForWorkflow({
      kind: "workflow",
      nodes: [
        { label: "Stops", type: "dataset", x: 0, y: 0, w: 220, h: 92, icon: "point" },
        { label: "Branch", type: "if", x: 320, y: 0, w: 220, h: 92, icon: "if" },
      ],
      edges: [],
    });

    // A dataset's geometry icon is `text.secondary`; the branch inherits the
    // node's `text.primary`.
    expect(svg).toContain(`color="${SNAPSHOT_PALETTE.icon}"`);
    expect(svg).toContain(`color="${SNAPSHOT_PALETTE.iconStrong}"`);
    expect(SNAPSHOT_PALETTE.icon).not.toBe(SNAPSHOT_PALETTE.iconStrong);
    // The title is written in the same value the geometry glyph takes.
    expect(SNAPSHOT_PALETTE.icon).toBe(SNAPSHOT_PALETTE.text);
  });

  it("falls a dataset with no icon back to the table glyph, as the canvas does", async () => {
    // A descriptor stored before the field existed carries no `icon` at all,
    // which reads the same as an explicit null.
    const table = iconPath(ICON_NAME.TABLE);
    for (const node of [
      { label: "Stops", type: "dataset", x: 0, y: 0, w: 220, h: 92 },
      { label: "Stops", type: "dataset", x: 0, y: 0, w: 220, h: 92, icon: null },
      // An icon naming something that is not a geometry is not a lookup hit.
      { label: "Stops", type: "dataset", x: 0, y: 0, w: 220, h: 92, icon: "constructor" },
    ]) {
      const svg = await svgForWorkflow({ kind: "workflow", nodes: [node], edges: [] });
      expect(svg).toContain(table);
    }
  });

  it("falls a tool with no process id back to the gear, and draws the tool's own icon when it has one", async () => {
    const gearless = await svgForWorkflow({
      kind: "workflow",
      nodes: [{ label: "Step", type: "tool", x: 0, y: 0, w: 220, h: 92 }],
      edges: [],
    });
    // MUI's gear, whose geometry is its own — it is enough that a glyph is
    // drawn, in its own nested svg, and that it is not the buffer icon.
    expect(gearless).toContain("<svg x=");
    expect(gearless).not.toContain("--icon-color-1");

    const buffered = await svgForWorkflow({
      kind: "workflow",
      nodes: [{ label: "Step", type: "tool", x: 0, y: 0, w: 220, h: 92, icon: "buffer" }],
      edges: [],
    });
    expect(buffered).not.toBe(gearless);
    // A two-tone tool icon's custom properties are substituted, since a
    // rasterised drawing carries no CSS.
    expect(buffered).not.toContain("var(--icon-color");
    expect(buffered).toContain(NODE_ICON_TONES.light[0]);
  });

  it("writes the title the canvas writes, translated where the canvas translates it", async () => {
    const translate = (key: string, options?: { defaultValue?: string }) =>
      key === "buffer" ? "Puffer" : key === "export_dataset" ? "Speichern" : (options?.defaultValue ?? key);
    const svg = await svgForWorkflow(descriptor, undefined, translate);

    // A dataset keeps its own label; a tool and an export take the canvas's
    // translated title over the label stored beside them.
    expect(svg).toContain(">Stops<");
    expect(svg).toContain(">Puffer<");
    expect(svg).toContain(">Speichern<");
    expect(svg).not.toContain(">Buffer<");
  });

  it("draws each node type's own handles, on the sides the canvas puts them", async () => {
    const svg = await svgForWorkflow({
      kind: "workflow",
      nodes: [{ label: "Branch", type: "if", x: 0, y: 0, w: 220, h: 120, icon: "if" }],
      edges: [],
    });

    // An if node: one target on the left, two sources on the right.
    const handles = [...svg.matchAll(/<circle cx="([\d.]+)" cy="([\d.]+)" r="[\d.]+" fill="#9E9E9E"/g)];
    expect(handles).toHaveLength(3);
    const xs = handles.map((match) => Number(match[1]));
    expect(new Set(xs).size).toBe(2);
    // The two sources sit at different heights — the true and false branches.
    const rightX = Math.max(...xs);
    const rightYs = handles.filter((match) => Number(match[1]) === rightX).map((match) => match[2]);
    expect(new Set(rightYs).size).toBe(2);
  });

  it("leaves the icon off an annotation, which carries its own colour and text", async () => {
    const svg = await svgForWorkflow({
      kind: "workflow",
      nodes: [
        {
          label: "textAnnotation",
          type: "textAnnotation",
          x: 0,
          y: 0,
          w: 400,
          h: 200,
          html: "<p>Read me</p>",
        },
      ],
      edges: [],
    });

    expect(svg).toContain(`fill="${SNAPSHOT_PALETTE.annotation}"`);
    // Its own markup, not a card title: there is no `<text>` on a note.
    expect(svg).toContain("<p>Read me</p>");
    expect(svg).not.toContain("<text ");
    // The two grounds and the note itself, no icon wrapper.
    expect(svg.match(/<rect /g)).toHaveLength(3);
    // No icon svg, and no handle — the one circle in the drawing is the
    // grid pattern's own dot.
    expect(svg.match(/<svg /g)).toHaveLength(1);
    expect(svg.match(/<circle cx=/g)).toHaveLength(1);
  });

  it("draws an annotation's own rich text in a foreignObject", async () => {
    const html = '<h2>Step one</h2><p>Buffer the <span style="color: #4C9F70">stops</span></p>';
    const svg = await svgForWorkflow({
      kind: "workflow",
      nodes: [{ label: "textAnnotation", type: "textAnnotation", x: 0, y: 0, w: 400, h: 200, html }],
      edges: [],
    });

    expect(svg).toContain("<foreignObject");
    // The markup itself, not a flattening of it: the heading and the run's
    // own colour survive into the picture.
    expect(svg).toContain(html);
    // In an xhtml fragment of its own, styled by rules that travel with it.
    expect(svg).toContain('xmlns="http://www.w3.org/1999/xhtml"');
    expect(svg).toContain(`class="${NOTE_CLASS}"`);
    expect(svg).toContain(NOTE_CSS);
    // The note's ink and its scaled body size are set on the element, so the
    // rules themselves carry no colour.
    expect(svg).toContain(`color: ${SNAPSHOT_PALETTE.noteText}`);
    // 16px at the 1.6 the snapshot magnifies a single node by.
    expect(svg).toContain("font-size: 25.6px");
  });

  it("paints an annotation in its own colour, and falls back to the canvas's", async () => {
    const own = await svgForWorkflow({
      kind: "workflow",
      nodes: [
        {
          label: "textAnnotation",
          type: "textAnnotation",
          x: 0,
          y: 0,
          w: 400,
          h: 200,
          color: "#4C9F70",
          html: "<p>green</p>",
        },
      ],
      edges: [],
    });
    expect(own).toContain(`fill="#4C9F70" fill-opacity="${ANNOTATION_FILL_OPACITY}"`);
    expect(own).toContain('stroke="#4C9F70"');
    expect(own).not.toContain(SNAPSHOT_PALETTE.annotation);

    const none = await svgForWorkflow({
      kind: "workflow",
      nodes: [{ label: "textAnnotation", type: "textAnnotation", x: 0, y: 0, w: 400, h: 200 }],
      edges: [],
    });
    expect(none).toContain(`fill="${SNAPSHOT_PALETTE.annotation}"`);
    // A note with no text of its own is a card and nothing else.
    expect(none).not.toContain("<foreignObject");
  });

  it("is a light drawing whatever the theme around it is", async () => {
    const svg = await svgForWorkflow(descriptor);

    expect(svg).toContain(`fill="${SNAPSHOT_PALETTE.ground}"`);
    expect(svg).toContain(`fill="${SNAPSHOT_PALETTE.text}"`);
    expect(svg).not.toContain("var(--");
  });

  it("carries no external reference beyond the SVG namespace itself", async () => {
    const svg = await svgForWorkflow(descriptor);

    expect(svg.replace('xmlns="http://www.w3.org/2000/svg"', "")).not.toContain("http");
    expect(svg).not.toContain("@import");
    expect(svg).not.toContain("<image");
    // Every url() names something this document defines: the grid pattern
    // and the card shadow.
    const references = [...svg.matchAll(/url\(#([^)]+)\)/g)].map((match) => match[1]);
    expect(references.length).toBeGreaterThan(0);
    // Every id the drawing refers to is one it defines itself, and each is
    // suffixed per drawing so two on one page cannot take each other's.
    for (const id of new Set(references)) {
      expect(id).toMatch(/^goat-snapshot-(grid|shadow)-\d+$/);
      expect(svg).toContain(`id="${id}"`);
    }
  });

  it("escapes a label that would otherwise break the markup", async () => {
    const svg = await svgForWorkflow({
      kind: "workflow",
      nodes: [{ label: "A & B <c>", type: "tool", x: 0, y: 0, w: 400, h: 120 }],
      edges: [],
    });

    expect(svg).toContain("A &amp; B &lt;c&gt;");
  });

  it("draws an empty workflow as the bare ground", async () => {
    const svg = await svgForWorkflow({ kind: "workflow", nodes: [], edges: [] });

    expect(svg).not.toContain("<path ");
    expect(svg).not.toContain("<text ");
    expect(svg.match(/<svg /g)).toHaveLength(1);
  });

  it("takes the size it is given", async () => {
    const svg = await svgForWorkflow(descriptor, { width: 640, height: 360 });

    expect(svg).toContain('viewBox="0 0 640 360"');
  });
});

describe("renderWorkflowSnapshot", () => {
  it("resolves the PNG the canvas produced", async () => {
    stubImage("load");
    const blob = new Blob(["png"], { type: "image/png" });
    const context = stubCanvas(blob);

    await expect(renderWorkflowSnapshot(descriptor)).resolves.toBe(blob);
    expect(context.drawImage).toHaveBeenCalledTimes(1);
  });

  it("rejects when the image cannot be loaded", async () => {
    stubImage("error");
    stubCanvas(new Blob(["png"], { type: "image/png" }));

    await expect(renderWorkflowSnapshot(descriptor)).rejects.toThrow(/image failed to load/i);
  });

  it("rejects when the canvas hands back no blob", async () => {
    stubImage("load");
    stubCanvas(null);

    await expect(renderWorkflowSnapshot(descriptor)).rejects.toThrow(/no PNG/i);
  });

  it("rejects when there is no 2D context to draw into", async () => {
    stubImage("load");
    vi.spyOn(HTMLCanvasElement.prototype, "getContext").mockReturnValue(null);

    await expect(renderWorkflowSnapshot(descriptor)).rejects.toThrow(/context/i);
  });
});
