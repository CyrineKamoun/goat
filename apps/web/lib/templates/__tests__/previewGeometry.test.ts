import { describe, expect, it } from "vitest";

import {
  NOTE_TEXT_CAP,
  PREVIEW_ITEM_CAP,
  PREVIEW_NODE_SIZE,
  PREVIEW_PAGE_SIZE,
  SCAFFOLD_MAX_SCALE,
  TEXT_ANNOTATION_SIZE,
  descriptorFromLayoutConfig,
  descriptorFromWorkflowConfig,
  fitLayout,
  fitWorkflow,
  layoutPageDescription,
  layoutPageFromConfig,
  previewNodeColor,
  previewNodeHandles,
  previewNodeIcon,
  previewNodeSize,
  previewNodeTitle,
  previewPageSizeName,
  sanitizeNoteHtml,
  truncateLabel,
} from "@/lib/templates/previewGeometry";
import type { TemplateLayoutPreview, TemplateWorkflowPreview } from "@/lib/validations/template";

const workflow = (
  nodes: TemplateWorkflowPreview["nodes"],
  edges: TemplateWorkflowPreview["edges"] = []
): TemplateWorkflowPreview => ({ kind: "workflow", nodes, edges });

const node = (
  overrides: Partial<TemplateWorkflowPreview["nodes"][number]> = {}
): TemplateWorkflowPreview["nodes"][number] => ({
  label: "Buffer",
  type: "tool",
  x: 0,
  y: 0,
  w: 200,
  h: 100,
  ...overrides,
});

/**
 * The numbers `apps/core/src/core/templates/snapshot.py` writes into a
 * descriptor (`_NODE_SIZES`, `_TEXT_ANNOTATION_SIZE`, `_PAGE_SIZES`,
 * `_PREVIEW_CAP`), spelled out here rather than derived: a descriptor built
 * on the client for the save dialog has to equal the one `build_preview`
 * stores for the same config, and only a literal table catches a drift on
 * either side.
 */
describe("the constants the backend mirrors", () => {
  it.each([
    ["dataset", 160, 56],
    ["tool", 220, 56],
    ["export", 220, 56],
    ["if", 220, 56],
  ])("sizes a %s node %d×%d, as _NODE_SIZES does", (type, w, h) => {
    expect(previewNodeSize(type)).toEqual({ w, h });
    expect(PREVIEW_NODE_SIZE[type]).toEqual({ w, h });
  });

  it("creates an annotation at 400×200, as _TEXT_ANNOTATION_SIZE does", () => {
    expect(TEXT_ANNOTATION_SIZE).toEqual({ w: 400, h: 200 });
    expect(previewNodeSize("textAnnotation")).toEqual({ w: 400, h: 200 });
  });

  it.each([
    ["A4", 210, 297],
    ["A3", 297, 420],
    ["Letter", 215.9, 279.4],
    ["Legal", 215.9, 355.6],
    ["Tabloid", 279.4, 431.8],
  ])("gives %s a %d×%d mm page, as _PAGE_SIZES does", (name, width, height) => {
    expect(PREVIEW_PAGE_SIZE[name]).toEqual({ width, height });
  });

  it("caps a descriptor at 60 items, as _PREVIEW_CAP does", () => {
    expect(PREVIEW_ITEM_CAP).toBe(60);
  });
});

describe("previewNodeSize", () => {
  it("reads only the table's own keys, the way the backend's `in` check does", () => {
    expect(previewNodeSize("constructor")).toBeNull();
    expect(previewNodeSize("toString")).toBeNull();
  });

  it("takes an annotation's own stored size", () => {
    expect(previewNodeSize("textAnnotation", { data: { width: 640, height: 320 } })).toEqual({
      w: 640,
      h: 320,
    });
  });

  it("falls back to ReactFlow's measurements, then to the created size", () => {
    expect(previewNodeSize("textAnnotation", { width: 512, height: 256 })).toEqual({ w: 512, h: 256 });
    expect(previewNodeSize("textAnnotation", { data: {} })).toEqual(TEXT_ANNOTATION_SIZE);
    expect(previewNodeSize("textAnnotation", { data: { width: 0, height: 0 } })).toEqual(
      TEXT_ANNOTATION_SIZE
    );
  });

  it("gives no size to a node type the canvas cannot place", () => {
    expect(previewNodeSize("something-new")).toBeNull();
    expect(previewNodeSize("")).toBeNull();
  });
});

describe("fitWorkflow", () => {
  it("scales a wide workflow down to fit and centres it", () => {
    const geometry = fitWorkflow(
      workflow([node({ x: 0, y: 0, w: 200, h: 100 }), node({ x: 600, y: 0, w: 200, h: 100 })]),
      480,
      270,
      20
    );

    // 800 wide into 440 of usable width, 100 tall into 230 → width decides.
    const scale = 440 / 800;
    expect(geometry.nodes[0].w).toBeCloseTo(200 * scale, 2);
    expect(geometry.nodes[0].x).toBeCloseTo(20, 2);
    expect(geometry.nodes[1].x).toBeCloseTo(20 + 600 * scale, 2);
    // Centred on the axis the scale did not fill.
    const contentHeight = 100 * scale;
    expect(geometry.nodes[0].y).toBeCloseTo(20 + (230 - contentHeight) / 2, 2);
    // Both axes take the same scale, so the canvas layout is not distorted.
    expect(geometry.nodes[0].h).toBeCloseTo(100 * scale, 2);
  });

  it("centres a single node at its canvas size rather than blowing it up", () => {
    const geometry = fitWorkflow(workflow([node({ x: 40, y: 900, w: 200, h: 100 })]), 480, 270, 20);

    expect(geometry.nodes).toHaveLength(1);
    expect(geometry.nodes[0].w).toBe(200);
    expect(geometry.nodes[0].h).toBe(100);
    expect(geometry.nodes[0].x).toBeCloseTo(140, 2);
    expect(geometry.nodes[0].y).toBeCloseTo(85, 2);
  });

  it("magnifies a small workflow up to the ceiling the caller allows", () => {
    // The scaffold's default ceiling is 1:1, the snapshot's is higher: the
    // same two nodes fill a 1280×720 frame instead of sitting in the middle
    // of it at canvas size.
    expect(SCAFFOLD_MAX_SCALE).toBe(1);

    const asIs = fitWorkflow(workflow([node({ x: 0, y: 0, w: 200, h: 100 })]), 480, 270, 20);
    const magnified = fitWorkflow(workflow([node({ x: 0, y: 0, w: 200, h: 100 })]), 480, 270, 20, 1.6);

    expect(asIs.scale).toBe(1);
    expect(magnified.scale).toBeCloseTo(1.6, 5);
    expect(asIs.nodes[0].w).toBe(200);
    expect(magnified.nodes[0].w).toBeCloseTo(320, 2);
    expect(magnified.nodes[0].h).toBeCloseTo(160, 2);
    // Still centred, and still one scale for both axes.
    expect(magnified.nodes[0].x).toBeCloseTo(20 + (440 - 320) / 2, 2);
    expect(magnified.nodes[0].y).toBeCloseTo(20 + (230 - 160) / 2, 2);
  });

  it("never magnifies past what fits, whatever ceiling the caller allows", () => {
    const geometry = fitWorkflow(workflow([node({ x: 0, y: 0, w: 800, h: 100 })]), 480, 270, 20, 4);

    // 800 into 440 of usable width still decides.
    expect(geometry.nodes[0].w).toBeCloseTo(440, 2);
  });

  it("offsets by the workflow's own origin, wherever on the canvas it sits", () => {
    const here = fitWorkflow(workflow([node({ x: 0, y: 0 }), node({ x: 400, y: 200 })]), 480, 270, 20);
    const faraway = fitWorkflow(
      workflow([node({ x: 5000, y: -3000 }), node({ x: 5400, y: -2800 })]),
      480,
      270,
      20
    );

    expect(faraway.nodes.map((box) => [box.x, box.y])).toEqual(here.nodes.map((box) => [box.x, box.y]));
  });

  it("draws one bezier per edge, from a right-centre to a left-centre", () => {
    const geometry = fitWorkflow(
      workflow([node({ x: 0, y: 0, w: 200, h: 100 }), node({ x: 400, y: 0, w: 200, h: 100 })], [[0, 1]]),
      480,
      270,
      20
    );

    expect(geometry.edges).toHaveLength(1);
    const start = geometry.nodes[0];
    const end = geometry.nodes[1];
    const match =
      /^M ([-\d.]+) ([-\d.]+) C ([-\d.]+) ([-\d.]+), ([-\d.]+) ([-\d.]+), ([-\d.]+) ([-\d.]+)$/.exec(
        geometry.edges[0].d
      );
    expect(match).not.toBeNull();
    const points = (match ?? []).slice(1).map(Number);
    // Leaves the source's right-centre, arrives at the target's left-centre.
    expect(points[0]).toBeCloseTo(start.x + start.w, 1);
    expect(points[1]).toBeCloseTo(start.y + start.h / 2, 1);
    expect(points[6]).toBeCloseTo(end.x, 1);
    expect(points[7]).toBeCloseTo(end.y + end.h / 2, 1);
    // Both control points sit between them on the x axis, which is what
    // makes the curve read as a flow left to right.
    expect(points[2]).toBeGreaterThan(points[0]);
    expect(points[4]).toBeLessThan(points[6]);
  });

  it("drops an edge naming a node the descriptor does not carry", () => {
    const geometry = fitWorkflow(
      workflow(
        [node()],
        [
          [0, 7],
          [3, 0],
        ]
      ),
      480,
      270
    );

    expect(geometry.edges).toEqual([]);
  });

  it("gives an empty or missing descriptor nothing to draw", () => {
    expect(fitWorkflow(workflow([]), 480, 270)).toEqual({ nodes: [], edges: [], scale: 1 });
    expect(fitWorkflow(null, 480, 270)).toEqual({ nodes: [], edges: [], scale: 1 });
    expect(fitWorkflow(undefined, 480, 270)).toEqual({ nodes: [], edges: [], scale: 1 });
  });

  it("keeps an annotation's label and type on its box", () => {
    const geometry = fitWorkflow(
      workflow([node({ type: "textAnnotation", label: "Read me", w: 400, h: 200 })]),
      480,
      270
    );

    expect(geometry.nodes[0].type).toBe("textAnnotation");
    expect(geometry.nodes[0].label).toBe("Read me");
  });
});

describe("fitLayout", () => {
  it("fits a landscape page and places its elements on it", () => {
    const descriptor: TemplateLayoutPreview = {
      kind: "layout",
      orientation: "landscape",
      page: { width: 297, height: 210 },
      elements: [{ type: "map", x: 10, y: 10, width: 200, height: 150 }],
    };

    const { page, elements } = fitLayout(descriptor, 480, 270, 20);

    // 297×210 into 440×230 → the height is what runs out first.
    const scale = 230 / 210;
    expect(page.h).toBeCloseTo(230, 2);
    expect(page.w).toBeCloseTo(297 * scale, 2);
    expect(page.x).toBeCloseTo(20 + (440 - 297 * scale) / 2, 2);
    expect(page.y).toBeCloseTo(20, 2);
    expect(elements[0].x).toBeCloseTo(page.x + 10 * scale, 2);
    expect(elements[0].w).toBeCloseTo(200 * scale, 2);
    expect(elements[0].type).toBe("map");
  });

  it("fits a portrait page against the width", () => {
    const { page } = fitLayout(
      { kind: "layout", orientation: "portrait", page: { width: 210, height: 297 }, elements: [] },
      480,
      270,
      20
    );

    expect(page.h).toBeCloseTo(230, 2);
    expect(page.w).toBeCloseTo(210 * (230 / 297), 2);
  });

  it("falls back to a portrait A4 page for a missing descriptor", () => {
    const { page, elements } = fitLayout(null, 480, 270, 20);

    expect(elements).toEqual([]);
    expect(page.w / page.h).toBeCloseTo(210 / 297, 3);
  });
});

describe("descriptorFromWorkflowConfig", () => {
  const config = {
    nodes: [
      {
        id: "a",
        type: "dataset",
        position: { x: 0, y: 0 },
        data: { type: "dataset", label: "Stops", geometryType: "point" },
      },
      {
        id: "b",
        type: "tool",
        position: { x: 320, y: 40 },
        data: { type: "tool", label: "Buffer", processId: "buffer" },
      },
      {
        id: "c",
        type: "textAnnotation",
        position: { x: 0, y: 300 },
        data: { type: "textAnnotation", width: 500, height: 240 },
      },
    ],
    edges: [
      { id: "e1", source: "a", target: "b" },
      { id: "e2", source: "a", target: "missing" },
    ],
  };

  /** The descriptor for `config` above, asserted whole: it is what both the
   * save dialog's live preview and the PNG writer draw from, so every field
   * a drawing reads is pinned here. */
  it("describes every node the canvas placed, with what a drawing reads off it", () => {
    expect(descriptorFromWorkflowConfig(config)).toEqual({
      kind: "workflow",
      nodes: [
        {
          label: "Stops",
          type: "dataset",
          x: 0,
          y: 0,
          w: 160,
          h: 56,
          icon: "point",
          color: null,
          html: null,
        },
        {
          label: "Buffer",
          type: "tool",
          x: 320,
          y: 40,
          w: 220,
          h: 56,
          icon: "buffer",
          color: null,
          html: null,
        },
        {
          label: "textAnnotation",
          type: "textAnnotation",
          x: 0,
          y: 300,
          w: 500,
          h: 240,
          icon: null,
          color: null,
          html: null,
        },
      ],
      edges: [[0, 1]],
    });
  });

  it("drops a node the canvas could not place, and the edges touching it", () => {
    const descriptor = descriptorFromWorkflowConfig({
      nodes: [
        { id: "a", type: "dataset", position: { x: 0, y: 0 }, data: { label: "Stops" } },
        { id: "b", type: "somethingNew", position: { x: 200, y: 0 }, data: { label: "Mystery" } },
        { id: "c", type: "tool", position: { x: 400 }, data: { label: "No y" } },
        { id: "d", type: "tool", data: { label: "No position" } },
        { id: "e", type: "tool", position: { x: 600, y: 0 }, data: { label: "Buffer" } },
      ],
      edges: [
        { source: "a", target: "b" },
        { source: "a", target: "c" },
        { source: "a", target: "e" },
      ],
    });

    expect(descriptor?.nodes.map((node) => node.label)).toEqual(["Stops", "Buffer"]);
    // Only the edge between the two survivors, reindexed onto them.
    expect(descriptor?.edges).toEqual([[0, 1]]);
  });

  it("labels a node whose label is missing or blank by its type", () => {
    const descriptor = descriptorFromWorkflowConfig({
      nodes: [
        { id: "a", type: "tool", position: { x: 0, y: 0 }, data: { label: "   " } },
        { id: "b", type: "export", position: { x: 0, y: 200 }, data: {} },
      ],
      edges: [],
    });

    expect(descriptor?.nodes.map((node) => node.label)).toEqual(["tool", "export"]);
  });

  it("carries an annotation's own markup and colour, and no label of its own", () => {
    const descriptor = descriptorFromWorkflowConfig({
      nodes: [
        {
          id: "a",
          type: "textAnnotation",
          position: { x: 0, y: 0 },
          data: {
            type: "textAnnotation",
            text: "<h2>Step one</h2><p>Buffer the <strong>stops</strong></p>",
            backgroundColor: "#4C9F70",
          },
        },
        {
          id: "b",
          type: "textAnnotation",
          position: { x: 0, y: 300 },
          data: { type: "textAnnotation", text: "<p></p>" },
        },
      ],
      edges: [],
    });

    expect(descriptor?.nodes[0].html).toBe("<h2>Step one</h2><p>Buffer the <strong>stops</strong></p>");
    expect(descriptor?.nodes[0].color).toBe("#4C9F70");
    // A note has no label on the canvas either, so the type stands.
    expect(descriptor?.nodes.map((item) => item.label)).toEqual(["textAnnotation", "textAnnotation"]);
    // An empty note carries no markup and no colour of its own.
    expect(descriptor?.nodes[1].html).toBeNull();
    expect(descriptor?.nodes[1].color).toBeNull();
  });

  it("caps the nodes it carries", () => {
    const many = {
      nodes: Array.from({ length: PREVIEW_ITEM_CAP + 12 }, (_unused, index) => ({
        id: `n${index}`,
        type: "tool",
        position: { x: index * 10, y: 0 },
        data: { type: "tool", label: `Step ${index}` },
      })),
      edges: [{ id: "e", source: "n0", target: `n${PREVIEW_ITEM_CAP + 5}` }],
    };

    const descriptor = descriptorFromWorkflowConfig(many);
    expect(descriptor?.nodes).toHaveLength(PREVIEW_ITEM_CAP);
    // The edge to a node past the cap has no index to point at.
    expect(descriptor?.edges).toEqual([]);
  });

  it("reads an absent, malformed or empty config as nothing to draw", () => {
    expect(descriptorFromWorkflowConfig(null)).toBeNull();
    expect(descriptorFromWorkflowConfig(undefined)).toBeNull();
    expect(descriptorFromWorkflowConfig({ nodes: "nope" })).toBeNull();
    expect(descriptorFromWorkflowConfig({ nodes: [] })).toBeNull();
    expect(descriptorFromWorkflowConfig({ nodes: [{ id: "a", type: "ghost" }] })).toBeNull();
  });
});

describe("descriptorFromLayoutConfig", () => {
  it("falls back to A4 for a page size named after an inherited member", () => {
    const descriptor = descriptorFromLayoutConfig({
      page: { size: "toString", orientation: "portrait" },
      elements: [],
    });

    expect(descriptor?.page).toEqual(PREVIEW_PAGE_SIZE.A4);
  });

  it("builds exactly what core.templates.snapshot.build_preview builds", () => {
    expect(
      descriptorFromLayoutConfig({
        page: { size: "A4", orientation: "landscape" },
        elements: [
          { type: "map", position: { x: 10, y: 10, width: 200, height: 150 } },
          { type: "text", position: { x: 220, y: 10, width: 60, height: 40 } },
        ],
      })
    ).toEqual({
      kind: "layout",
      orientation: "landscape",
      page: { width: 297, height: 210 },
      elements: [
        { type: "map", x: 10, y: 10, width: 200, height: 150 },
        { type: "text", x: 220, y: 10, width: 60, height: 40 },
      ],
    });
  });

  it("defaults to an A4 portrait page, and reads Custom as A4 too", () => {
    expect(descriptorFromLayoutConfig({})?.page).toEqual({ width: 210, height: 297 });
    expect(descriptorFromLayoutConfig({ page: { size: "Custom" } })?.page).toEqual({
      width: 210,
      height: 297,
    });
    expect(descriptorFromLayoutConfig({ page: { size: "Poster" } })?.page).toEqual({
      width: 210,
      height: 297,
    });
  });

  it("swaps the page in landscape", () => {
    expect(descriptorFromLayoutConfig({ page: { size: "Letter", orientation: "landscape" } })?.page).toEqual({
      width: 279.4,
      height: 215.9,
    });
  });

  it("drops an element with no numeric box, and caps the rest", () => {
    const descriptor = descriptorFromLayoutConfig({
      page: { size: "A4" },
      elements: [
        { type: "map", position: { x: 0, y: 0, width: 10, height: 10 } },
        { type: "legend" },
        { type: "text", position: { x: 0, y: 0, width: "wide", height: 10 } },
        { position: { x: 0, y: 0, width: 10, height: 10 } },
      ],
    });

    expect(descriptor?.elements).toEqual([{ type: "map", x: 0, y: 0, width: 10, height: 10 }]);

    const many = descriptorFromLayoutConfig({
      elements: Array.from({ length: PREVIEW_ITEM_CAP + 5 }, () => ({
        type: "text",
        position: { x: 0, y: 0, width: 10, height: 10 },
      })),
    });
    expect(many?.elements).toHaveLength(PREVIEW_ITEM_CAP);
  });

  it("keeps a page with no elements on it, and refuses a malformed config", () => {
    expect(descriptorFromLayoutConfig({ page: { size: "A4" } })?.elements).toEqual([]);
    expect(descriptorFromLayoutConfig({ elements: "nope" })).toBeNull();
    expect(descriptorFromLayoutConfig(null)).toBeNull();
  });

  it("carries a map frame's camera, a text block's markup and a legend's title", () => {
    const descriptor = descriptorFromLayoutConfig({
      page: { size: "A4", orientation: "landscape" },
      elements: [
        {
          type: "map",
          position: { x: 10, y: 30, width: 200, height: 150 },
          config: { viewState: { latitude: 48.1374, longitude: 11.5755, zoom: 11.5, pitch: 0, bearing: 0 } },
        },
        {
          type: "text",
          position: { x: 10, y: 8, width: 200, height: 18 },
          config: {
            setup: {
              text: '<p style="text-align: center"><strong><span style="font-size: 24pt">RIDERSHIP</span></strong></p>',
            },
          },
        },
        {
          type: "legend",
          position: { x: 220, y: 30, width: 60, height: 60 },
          config: { title: { text: "Legend" }, mapElementId: "map-1" },
        },
      ],
    });

    expect(descriptor?.elements[0].viewState).toEqual({
      latitude: 48.1374,
      longitude: 11.5755,
      zoom: 11.5,
    });
    // The markup travels sanitised — the styles the allow-list carries stay,
    // and the size it does not carry is read off separately, in the canvas's
    // own pixels (24pt at 96 DPI).
    expect(descriptor?.elements[1].html).toContain("RIDERSHIP");
    expect(descriptor?.elements[1].html).toContain("text-align: center");
    expect(descriptor?.elements[1].html).not.toContain("font-size");
    expect(descriptor?.elements[1].fontSize).toBe(32);
    expect(descriptor?.elements[2].title).toBe("Legend");
  });

  it("carries no payload for a block that names none, or names an unusable one", () => {
    const descriptor = descriptorFromLayoutConfig({
      elements: [
        // A camera off the globe, and one whose values are not numbers.
        {
          type: "map",
          position: { x: 0, y: 0, width: 10, height: 10 },
          config: { viewState: { latitude: 99, longitude: 11, zoom: 8 } },
        },
        {
          type: "map",
          position: { x: 0, y: 0, width: 10, height: 10 },
          config: { viewState: { latitude: "48", longitude: 11, zoom: 8 } },
        },
        // An empty text block, and a legend with no title of its own.
        {
          type: "text",
          position: { x: 0, y: 0, width: 10, height: 10 },
          config: { setup: { text: "<p></p>" } },
        },
        { type: "legend", position: { x: 0, y: 0, width: 10, height: 10 }, config: {} },
        // And a block carrying content that belongs to another type.
        {
          type: "scalebar",
          position: { x: 0, y: 0, width: 10, height: 10 },
          config: { title: { text: "Scale" } },
        },
      ],
    });

    expect(descriptor?.elements).toEqual([
      { type: "map", x: 0, y: 0, width: 10, height: 10 },
      { type: "map", x: 0, y: 0, width: 10, height: 10 },
      { type: "text", x: 0, y: 0, width: 10, height: 10 },
      { type: "legend", x: 0, y: 0, width: 10, height: 10 },
      { type: "scalebar", x: 0, y: 0, width: 10, height: 10 },
    ]);
  });

  it("carries the frame and fill a block asks for, and nothing where it asks for none", () => {
    const descriptor = descriptorFromLayoutConfig({
      elements: [
        { type: "table", position: { x: 0, y: 0, width: 10, height: 10 } },
        {
          type: "table",
          position: { x: 0, y: 0, width: 10, height: 10 },
          style: { border: { enabled: false, color: "#000000", width: 2 }, padding: 0, opacity: 1 },
        },
        {
          type: "table",
          position: { x: 0, y: 0, width: 10, height: 10 },
          style: {
            border: { enabled: true, color: "#1A73E8", width: 1.5 },
            background: { enabled: true, color: "#FFF3CD", opacity: 0.4 },
            padding: 3,
            opacity: 0.8,
          },
        },
        // A width outside the layout's own range, and a colour a drawing
        // will not paint with, fall back to what the layout defaults to.
        {
          type: "table",
          position: { x: 0, y: 0, width: 10, height: 10 },
          style: { border: { enabled: true, color: "red", width: 99 } },
        },
      ],
    });

    expect(descriptor?.elements[0].style).toBeUndefined();
    expect(descriptor?.elements[1].style).toBeUndefined();
    expect(descriptor?.elements[2].style).toEqual({
      border: { color: "#1A73E8", width: 1.5 },
      background: { color: "#FFF3CD", opacity: 0.4 },
      padding: 3,
      opacity: 0.8,
    });
    expect(descriptor?.elements[3].style).toEqual({ border: { color: "#000000", width: 5 } });
  });

  it("reads a text block's markup from the flatter shapes an older config carries", () => {
    expect(
      descriptorFromLayoutConfig({
        elements: [
          { type: "text", position: { x: 0, y: 0, width: 10, height: 10 }, config: { text: "<p>Notes</p>" } },
        ],
      })?.elements[0].html
    ).toBe("<p>Notes</p>");
    expect(
      descriptorFromLayoutConfig({
        elements: [
          {
            type: "text",
            position: { x: 0, y: 0, width: 10, height: 10 },
            config: { content: "<p>Notes</p>" },
          },
        ],
      })?.elements[0].html
    ).toBe("<p>Notes</p>");
  });
});

describe("truncateLabel", () => {
  it("leaves a label that fits", () => {
    expect(truncateLabel("Buffer", 200, 12)).toBe("Buffer");
  });

  it("cuts a label that does not fit", () => {
    const cut = truncateLabel("Aggregate points onto a hexagonal grid", 60, 12);
    expect(cut.endsWith("…")).toBe(true);
    expect(cut.length).toBeLessThan("Aggregate points onto a hexagonal grid".length);
  });

  it("drops a label with no room at all", () => {
    expect(truncateLabel("Buffer", 0, 12)).toBe("");
  });
});

describe("previewNodeIcon", () => {
  it("names a dataset's geometry, and reads anything else as the table glyph", () => {
    for (const geometry of ["point", "line", "polygon"]) {
      expect(previewNodeIcon("dataset", { geometryType: geometry })).toBe(geometry);
    }
    // `DatasetNode.getGeometryIcon` falls every other case through to the
    // table: an explicit table, an unknown geometry, and no data at all.
    expect(previewNodeIcon("dataset", { geometryType: "table" })).toBe("table");
    expect(previewNodeIcon("dataset", { geometryType: "raster" })).toBe("table");
    expect(previewNodeIcon("dataset", {})).toBe("table");
    expect(previewNodeIcon("dataset", null)).toBe("table");
  });

  it("names a tool by its process id, and carries none for a tool without one", () => {
    expect(previewNodeIcon("tool", { processId: "buffer" })).toBe("buffer");
    // A tool node the author has not picked a process for yet shows the
    // gear, which needs no name.
    expect(previewNodeIcon("tool", { processId: "" })).toBeNull();
    expect(previewNodeIcon("tool", { processId: 7 })).toBeNull();
    expect(previewNodeIcon("tool", {})).toBeNull();
  });

  it("names the fixed glyphs an export and a branch carry, and none for anything else", () => {
    expect(previewNodeIcon("export", {})).toBe("export_dataset");
    expect(previewNodeIcon("if", {})).toBe("if");
    expect(previewNodeIcon("textAnnotation", {})).toBeNull();
    expect(previewNodeIcon("somethingNew", {})).toBeNull();
  });
});

describe("previewNodeTitle", () => {
  const t = (key: string, options?: { defaultValue?: string }) =>
    key === "buffer"
      ? "Buffer"
      : key === "export_dataset"
        ? "Save as Dataset"
        : (options?.defaultValue ?? key);

  it("keeps a dataset's own label", () => {
    expect(previewNodeTitle(node({ type: "dataset", label: "Stops" }), t)).toBe("Stops");
  });

  it("translates a tool by the process its icon names", () => {
    expect(previewNodeTitle(node({ type: "tool", label: "buffer_v2", icon: "buffer" }), t)).toBe("Buffer");
    // A tool with no process falls back to whatever label it carries.
    expect(previewNodeTitle(node({ type: "tool", label: "Step" }), t)).toBe("Step");
  });

  it("writes the canvas's own words for an export and a branch", () => {
    expect(previewNodeTitle(node({ type: "export", label: "export" }), t)).toBe("Save as Dataset");
    expect(previewNodeTitle(node({ type: "if", label: "if" }), t)).toBe("Conditional");
    expect(previewNodeTitle(node({ type: "if", label: "Over 500m" }), t)).toBe("Over 500m");
  });

  it("leaves an annotation titleless and a descriptor untranslated without a translator", () => {
    expect(previewNodeTitle(node({ type: "textAnnotation", label: "textAnnotation" }), t)).toBe("");
    expect(previewNodeTitle(node({ type: "export", label: "Save", icon: "export_dataset" }))).toBe("Save");
  });
});

describe("previewNodeHandles", () => {
  it("puts each node type's handles where the canvas puts them", () => {
    expect(previewNodeHandles("dataset")).toEqual({ targets: [], sources: [0.5] });
    expect(previewNodeHandles("tool")).toEqual({ targets: [0.5], sources: [0.5] });
    expect(previewNodeHandles("export")).toEqual({ targets: [0.5], sources: [] });
    expect(previewNodeHandles("if")).toEqual({ targets: [0.5], sources: [0.33, 0.66] });
    expect(previewNodeHandles("textAnnotation")).toEqual({ targets: [], sources: [] });
  });
});

describe("previewPageSizeName", () => {
  it("names a standard page in either orientation, and nothing for a custom one", () => {
    expect(previewPageSizeName({ width: 210, height: 297 })).toBe("A4");
    expect(previewPageSizeName({ width: 297, height: 210 })).toBe("A4");
    expect(previewPageSizeName({ width: 297, height: 420 })).toBe("A3");
    expect(previewPageSizeName({ width: 215.9, height: 279.4 })).toBe("Letter");
    expect(previewPageSizeName({ width: 279.4, height: 431.8 })).toBe("Tabloid");
    expect(previewPageSizeName({ width: 500, height: 500 })).toBeNull();
  });
});

describe("layoutPageDescription", () => {
  const t = (key: string, options?: { defaultValue?: string }) =>
    ({ landscape: "Landscape", portrait: "Portrait", custom: "Custom" })[key] ?? options?.defaultValue ?? key;

  it("names the page and its orientation, and gives its millimetres", () => {
    expect(layoutPageDescription({ page_size: "A4", page_orientation: "landscape" }, t)).toEqual({
      name: "A4",
      label: "A4 · Landscape",
      width: 297,
      height: 210,
    });
    expect(layoutPageDescription({ page_size: "A4", page_orientation: "portrait" }, t)?.label).toBe(
      "A4 · Portrait"
    );
    expect(layoutPageDescription({ page_size: "Letter", page_orientation: "portrait" }, t)).toEqual({
      name: "Letter",
      label: "Letter · Portrait",
      width: 215.9,
      height: 279.4,
    });
  });

  it("calls a size it has no millimetres for Custom, on A4's page", () => {
    expect(layoutPageDescription({ page_size: "Custom", page_orientation: "landscape" }, t)).toEqual({
      name: "Custom",
      label: "Custom · Landscape",
      width: 297,
      height: 210,
    });
  });

  it("reads a template with no orientation as portrait", () => {
    expect(layoutPageDescription({ page_size: "A3" }, t)?.label).toBe("A3 · Portrait");
  });

  it("describes no page for a template that names none", () => {
    expect(layoutPageDescription({}, t)).toBeNull();
    expect(layoutPageDescription({ page_size: null, page_orientation: null }, t)).toBeNull();
    expect(layoutPageDescription({ page_orientation: "landscape" }, t)).toBeNull();
  });
});

describe("layoutPageFromConfig", () => {
  it("reads the page a layout config declares, verbatim", () => {
    expect(layoutPageFromConfig({ page: { size: "A3", orientation: "landscape" } })).toEqual({
      page_size: "A3",
      page_orientation: "landscape",
    });
    expect(layoutPageFromConfig({ page: { size: "Custom", orientation: "portrait" } })).toEqual({
      page_size: "Custom",
      page_orientation: "portrait",
    });
  });

  it("reads nothing off a config that declares no page", () => {
    const none = { page_size: null, page_orientation: null };
    expect(layoutPageFromConfig({})).toEqual(none);
    expect(layoutPageFromConfig(null)).toEqual(none);
    expect(layoutPageFromConfig({ page: {} })).toEqual(none);
    expect(layoutPageFromConfig({ page: { size: 4, orientation: "sideways" } })).toEqual(none);
  });
});

describe("previewNodeColor", () => {
  it("carries a note's own colour, and only a note's", () => {
    expect(previewNodeColor("textAnnotation", { backgroundColor: "#4C9F70" })).toBe("#4C9F70");
    expect(previewNodeColor("textAnnotation", { backgroundColor: "#ABC" })).toBe("#ABC");
    expect(previewNodeColor("tool", { backgroundColor: "#4C9F70" })).toBeNull();
  });

  it("carries nothing for a value that is not a colour to paint with", () => {
    expect(previewNodeColor("textAnnotation", {})).toBeNull();
    expect(previewNodeColor("textAnnotation", { backgroundColor: "goldenrod" })).toBeNull();
    expect(previewNodeColor("textAnnotation", { backgroundColor: "#12345" })).toBeNull();
    expect(previewNodeColor("textAnnotation", { backgroundColor: "#F2CE58;x" })).toBeNull();
    expect(previewNodeColor("textAnnotation", { backgroundColor: 16777215 })).toBeNull();
    expect(previewNodeColor("textAnnotation", null)).toBeNull();
  });
});

describe("sanitizeNoteHtml", () => {
  it("keeps the rich text TipTap writes", () => {
    expect(
      sanitizeNoteHtml(
        "<h2>Step one</h2><p>Buffer the <strong>stops</strong> by <em>500&nbsp;m</em>.</p>" +
          "<ul><li>then dissolve</li></ul>"
      )
    ).toBe(
      "<h2>Step one</h2><p>Buffer the <strong>stops</strong> by <em>500\u00a0m</em>.</p>" +
        "<ul><li>then dissolve</li></ul>"
    );
    expect(sanitizeNoteHtml("<p><u>u</u><s>s</s><mark>m</mark><sub>2</sub><sup>3</sup></p>")).toBe(
      "<p><u>u</u><s>s</s><mark>m</mark><sub>2</sub><sup>3</sup></p>"
    );
    expect(sanitizeNoteHtml("<p>a<br>b<br/>c</p>")).toBe("<p>a<br />b<br />c</p>");
  });

  it("drops a script or a style element with everything in it", () => {
    expect(sanitizeNoteHtml("<p><script>alert(1)</script>after</p>")).toBe("<p>after</p>");
    expect(sanitizeNoteHtml("<p><style>p{color:red}</style>styled</p>")).toBe("<p>styled</p>");
    // An unclosed script swallows the rest, so the note shows nothing.
    expect(sanitizeNoteHtml("<p><script>alert(1)</p>")).toBeNull();
    expect(sanitizeNoteHtml("<!-- a comment --><p>after comment</p>")).toBe("<p>after comment</p>");
  });

  it("drops every attribute but a filtered style and a safe href", () => {
    expect(sanitizeNoteHtml('<p onclick="alert(1)" class="x">click</p>')).toBe("<p>click</p>");
    expect(sanitizeNoteHtml('<p><img src="x" onerror="alert(1)">image</p>')).toBe("<p>image</p>");
    expect(sanitizeNoteHtml('<p><a href="javascript:alert(1)">bad</a></p>')).toBe("<p><a>bad</a></p>");
    expect(sanitizeNoteHtml('<p><a href="https://example.org/x?a=1&amp;b=2">good</a></p>')).toBe(
      '<p><a href="https://example.org/x?a=1&amp;b=2">good</a></p>'
    );
  });

  it("keeps only the style properties a note may carry", () => {
    expect(
      sanitizeNoteHtml('<p><span style="color: #4C9F70; font-family: Inter; background: red">g</span></p>')
    ).toBe('<p><span style="color: #4C9F70; font-family: Inter">g</span></p>');
    expect(sanitizeNoteHtml('<p style="text-align: center">centred</p>')).toBe(
      '<p style="text-align: center">centred</p>'
    );
    // Nothing that would make the drawing fetch something.
    expect(
      sanitizeNoteHtml('<p><span style="background-image: url(http://x/y.png); color: red">u</span></p>')
    ).toBe('<p><span style="color: red">u</span></p>');
  });

  it("keeps the text of a tag it drops, closes what was left open and drops a stray close", () => {
    expect(sanitizeNoteHtml("<div><p>outer div dropped</p></div>")).toBe("<p>outer div dropped</p>");
    expect(sanitizeNoteHtml("<p>unclosed <strong>bold")).toBe("<p>unclosed <strong>bold</strong></p>");
    expect(sanitizeNoteHtml("</strong><p>stray close</p>")).toBe("<p>stray close</p>");
  });

  it("writes XML the rasteriser's parser accepts", () => {
    // Only the five entities XML defines, whatever the note was written with.
    expect(sanitizeNoteHtml("<p>Tom &amp; Jerry &lt;3 &quot;b&quot; &#65;&#x42; &mdash;</p>")).toBe(
      '<p>Tom &amp; Jerry &lt;3 "b" AB &amp;mdash;</p>'
    );
  });

  it("shows nothing for an empty note, a blank one or a text that is not a string", () => {
    expect(sanitizeNoteHtml("<p></p>")).toBeNull();
    expect(sanitizeNoteHtml("   ")).toBeNull();
    expect(sanitizeNoteHtml("")).toBeNull();
    expect(sanitizeNoteHtml(undefined)).toBeNull();
    expect(sanitizeNoteHtml(null)).toBeNull();
    expect(sanitizeNoteHtml(42)).toBeNull();
  });

  it("caps how much of a long note it carries", () => {
    const html = sanitizeNoteHtml(`<p>${"ab ".repeat(2000)}</p>`);
    expect(html).not.toBeNull();
    // The visible characters are capped; the markup around them is not text.
    const text = (html as string).replace(/<[^>]*>/g, "");
    expect(text.length).toBe(NOTE_TEXT_CAP);
  });
});
