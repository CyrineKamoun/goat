import { nodeIconGlyphSize } from "@/lib/templates/nodeIcons";
import type { PreviewTranslate } from "@/lib/templates/previewGeometry";
import {
  SNAPSHOT_SIZE,
  fitWorkflow,
  previewNodeHandles,
  previewNodeTitle,
  truncateLabel,
} from "@/lib/templates/previewGeometry";
import type { TemplateWorkflowPreview } from "@/lib/validations/template";

export { SNAPSHOT_SIZE };

/** The padding the snapshot keeps around the fitted workflow. */
const SNAPSHOT_PADDING = 64;

/**
 * How far the snapshot magnifies a workflow smaller than its frame: at
 * 1280×720 a two-node workflow drawn at canvas size would sit in the middle
 * of an empty page, so the fit is allowed past 1:1 — but not so far that a
 * single node fills the whole picture.
 */
const SNAPSHOT_MAX_SCALE = 1.6;

/**
 * The card a node is drawn as, in the canvas's own pixels
 * (`components/workflows/nodes/shared.tsx` and `DatasetNode.tsx`):
 * `NodeContainer`'s padding, 6px radius and 2px border, the 40px
 * `NodeIconWrapper` with its 1px border and 6px radius, `NodeHeader`'s gap,
 * the 12px bold caption beside it, and the 12px `StyledHandle` circles with
 * their 2px ring. The padding and the gap are `theme.spacing(1.5)` and
 * `theme.spacing(1)`, and this theme's spacing is `0.25 * factor rem`
 * (`packages/js/ui/theme/spacing.ts`), so they are 6px and 4px rather than
 * MUI's default 12 and 8. Every one of them is multiplied by the fit's
 * scale, so a node's furniture is drawn in the same proportion as its box.
 */
const NODE_DESIGN = {
  padding: 6,
  radius: 6,
  border: 2,
  iconBox: 40,
  iconBoxBorder: 1,
  gap: 4,
  fontSize: 12,
  handle: 12,
  handleRing: 2,
  /** A note's own padding is `theme.spacing(2)` — 8px, not the card's 6
   * (`TextAnnotationNode.tsx`). */
  annotationPadding: 8,
  /** The size a note's own text is set at: nothing between the canvas pane
   * and the TipTap content sets a font, so the prose renders in `body1` —
   * 1rem in `text.primary`. Every size inside the note is relative to it
   * (`NOTE_CSS`), so scaling this scales the whole note. */
  annotationFontSize: 16,
  /** The canvas's edges are 2px; a thumbnail reads better with a thinner
   * line between the handles. */
  edgeWidth: 1.5,
  /** `<Background variant="dots" gap={16} size={1} />`. */
  gridGap: 16,
  gridDot: 0.5,
} as const;

/** The share of an annotation's own colour its fill carries — the `0D` alpha
 * suffix `TextAnnotationNode`'s container paints its background with. */
export const ANNOTATION_FILL_OPACITY = 0.05;

const round = (value: number): number => Math.round(value * 100) / 100;

/** The 40px `NodeIconWrapper` and, inside it, the glyph it centres. */
export interface DrawingIcon {
  /** The wrapper's own box. */
  x: number;
  y: number;
  size: number;
  radius: number;
  border: number;
  /** The glyph, centred in the wrapper at the canvas's own font size. */
  glyphX: number;
  glyphY: number;
  glyphSize: number;
}

/** One handle circle, centred on the edge of its node's box. */
export interface DrawingHandle {
  x: number;
  y: number;
  r: number;
  ring: number;
}

/** The box a note's rich text is rendered into, and the font size it is set
 * at — the canvas's own 1rem body, scaled by the drawing's fit, with every
 * size inside the note relative to it. */
export interface DrawingProse {
  x: number;
  y: number;
  w: number;
  h: number;
  fontSize: number;
}

/** One node's card, with everything on it already placed. */
export interface DrawingNode {
  type: string;
  /** Which of the node type's icons to draw. Missing on a descriptor built
   * before the field existed, which reads as the type's default. */
  icon: string | null;
  /** A text annotation is a tinted note rather than a card: it carries its
   * own colour, no shadow, no icon and no handle. */
  annotation: boolean;
  /** The colour a note is painted in — a 5% fill and a border of it, the way
   * the canvas paints it. Null where the node carries none of its own, and
   * on every other node type, which are painted from the palette. */
  color: string | null;
  x: number;
  y: number;
  w: number;
  h: number;
  radius: number;
  border: number;
  /** Null where the card is too small to hold the icon wrapper, and on an
   * annotation, which shows none. */
  icons: DrawingIcon | null;
  /** The node's title, already translated and already truncated to the room
   * beside its icon. Empty where nothing is written on the card, and on an
   * annotation — whose content is its own markup (`html`). */
  label: string;
  labelX: number;
  labelY: number;
  fontSize: number;
  letterSpacing: number;
  handles: DrawingHandle[];
  /** An annotation's own note markup, already sanitised by the descriptor.
   * Null on every other node type and on a note that carries no text. */
  html: string | null;
  /** Where that markup is laid out and at what size — the note's box inside
   * the canvas's own padding, and the font the prose is set at. Null wherever
   * there is no markup to lay out. */
  prose: DrawingProse | null;
}

/**
 * A whole workflow drawing, in the frame's own coordinates: what to draw and
 * where, with no colour and no markup in it. The one geometry source behind
 * both drawings of a workflow — the stored PNG's SVG string and the React
 * scaffold rendered into the page — so the two are the same picture.
 */
export interface WorkflowDrawing {
  width: number;
  height: number;
  /** What the canvas was scaled by to fit the frame. */
  scale: number;
  grid: { gap: number; dot: number };
  /** `NodeContainer`'s `0 2px 8px rgba(0,0,0,0.08)`, scaled. */
  shadow: { dy: number; blur: number };
  edges: { d: string; width: number }[];
  nodes: DrawingNode[];
}

export interface WorkflowDrawingOptions {
  /** The frame to fit the workflow into. */
  size?: { width: number; height: number };
  padding?: number;
  maxScale?: number;
  /** Resolves the canvas's own title keys; the titles stay untranslated
   * without it. */
  translate?: PreviewTranslate;
}

/**
 * Everything a workflow drawing consists of: the frame, the canvas's dot
 * grid, the edges, and one card per node — placed exactly as an idle node is
 * on the canvas, down to its icon wrapper, its handles and its shadow.
 */
export const workflowDrawing = (
  descriptor: TemplateWorkflowPreview | null | undefined,
  options: WorkflowDrawingOptions = {}
): WorkflowDrawing => {
  const { width, height } = options.size ?? SNAPSHOT_SIZE;
  const { nodes, edges, scale } = fitWorkflow(
    descriptor,
    width,
    height,
    options.padding ?? SNAPSHOT_PADDING,
    options.maxScale ?? SNAPSHOT_MAX_SCALE
  );

  // The card's furniture, scaled the same way its box was.
  const padding = NODE_DESIGN.padding * scale;
  const annotationPadding = NODE_DESIGN.annotationPadding * scale;
  const radius = round(NODE_DESIGN.radius * scale);
  const border = round(Math.max(NODE_DESIGN.border * scale, 1));
  const iconBox = NODE_DESIGN.iconBox * scale;
  const iconBoxBorder = round(Math.max(NODE_DESIGN.iconBoxBorder * scale, 0.75));
  const gap = NODE_DESIGN.gap * scale;
  const fontSize = round(NODE_DESIGN.fontSize * scale);
  const handleRadius = round((NODE_DESIGN.handle * scale) / 2);
  const handleRing = round(Math.max(NODE_DESIGN.handleRing * scale, 0.75));

  return {
    width,
    height,
    scale,
    grid: {
      gap: round(Math.max(NODE_DESIGN.gridGap * scale, 8)),
      dot: round(Math.max(NODE_DESIGN.gridDot * scale, 0.5)),
    },
    shadow: { dy: round(2 * scale), blur: round(4 * scale) },
    edges: edges.map((edge) => ({ d: edge.d, width: round(NODE_DESIGN.edgeWidth * scale) })),
    nodes: nodes.map((node): DrawingNode => {
      const annotation = node.type === "textAnnotation";
      const centerY = node.y + node.h / 2;
      const inset = border + (annotation ? annotationPadding : padding);
      const hasIcon = !annotation && iconBox + inset * 2 <= node.w && iconBox <= node.h;
      const glyphSize = Math.min(nodeIconGlyphSize(node.type) * scale, iconBox);
      const iconY = centerY - iconBox / 2;
      const labelX = node.x + inset + (hasIcon ? iconBox + gap : 0);
      const title = previewNodeTitle(node, options.translate);
      const available = Math.max(node.x + node.w - inset - labelX, 0);
      // A note is the canvas's own rich text, not a card title: its markup is
      // laid out inside its box the way the canvas lays it out, so the
      // drawing hands on the box and the scaled body size and leaves the
      // rendering to whoever draws it.
      const html = annotation ? (node.html ?? null) : null;
      // A note has no handle; every card carries the ones its node type does.
      const { targets, sources } = annotation ? { targets: [], sources: [] } : previewNodeHandles(node.type);
      const handle = (x: number, fraction: number): DrawingHandle => ({
        x: round(x),
        y: round(node.y + node.h * fraction),
        r: handleRadius,
        ring: handleRing,
      });

      return {
        type: node.type,
        icon: node.icon ?? null,
        annotation,
        color: node.color ?? null,
        x: round(node.x),
        y: round(node.y),
        w: round(node.w),
        h: round(node.h),
        radius,
        border,
        icons: hasIcon
          ? {
              x: round(node.x + inset),
              y: round(iconY),
              size: round(iconBox),
              radius,
              border: iconBoxBorder,
              glyphX: round(node.x + inset + (iconBox - glyphSize) / 2),
              glyphY: round(iconY + (iconBox - glyphSize) / 2),
              glyphSize: round(glyphSize),
            }
          : null,
        label: annotation ? "" : truncateLabel(title, available, fontSize),
        labelX: round(labelX),
        labelY: round(centerY + fontSize * 0.35),
        fontSize,
        letterSpacing: round(0.4 * scale),
        handles: [
          ...targets.map((fraction) => handle(node.x, fraction)),
          ...sources.map((fraction) => handle(node.x + node.w, fraction)),
        ],
        html,
        prose: html
          ? {
              x: round(node.x + inset),
              y: round(node.y + inset),
              w: round(Math.max(node.w - 2 * inset, 0)),
              h: round(Math.max(node.h - 2 * inset, 0)),
              fontSize: round(NODE_DESIGN.annotationFontSize * scale),
            }
          : null,
      };
    }),
  };
};
