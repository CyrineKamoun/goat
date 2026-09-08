"use client";

import { Box, alpha, useTheme } from "@mui/material";
import { cloneElement } from "react";
import { useTranslation } from "react-i18next";

import type { LayoutDrawingElement } from "@/lib/templates/layoutDrawing";
import { layoutDrawing } from "@/lib/templates/layoutDrawing";
import type { GlyphShape } from "@/lib/templates/layoutGlyphs";
import type { LayoutPalette } from "@/lib/templates/layoutSnapshot";
import { elementStrokeColor, layoutPaletteForTheme } from "@/lib/templates/layoutSnapshot";
import { nodeIconElement, nodeIconToneStyle } from "@/lib/templates/nodeIcons";
import { SCAFFOLD_FRAME } from "@/lib/templates/previewGeometry";
import { staticMapUrl } from "@/lib/templates/staticMap";
import type { DrawingNode, WorkflowDrawing } from "@/lib/templates/workflowDrawing";
import { ANNOTATION_FILL_OPACITY, workflowDrawing } from "@/lib/templates/workflowDrawing";
import type { WorkflowPalette } from "@/lib/templates/workflowSnapshot";
import {
  NOTE_CLASS,
  NOTE_CSS,
  NOTE_FONT_STACK,
  nodeIconColor,
  workflowPaletteForTheme,
} from "@/lib/templates/workflowSnapshot";
import type { TemplatePayloadKind, TemplatePreviewDescriptor } from "@/lib/validations/template";

import MarkBlock from "@/components/dashboard/common/MarkBlock";
import TemplateDefaultThumbnail from "@/components/templates/TemplateDefaultThumbnail";

interface TemplatePreviewFallbackProps {
  /** The structure to draw, built by the caller from a config it can see.
   * Null draws the blank stand-in for that payload kind. */
  descriptor: TemplatePreviewDescriptor | null;
  /** What the payload is, for the two kinds that are drawn without a
   * descriptor: a project's mark, and a layout's blank page. */
  payloadKind: TemplatePayloadKind;
  /** Suffixes the ids one drawing defines, so two scaffolds on one page do
   * not share a scale-dependent definition. */
  id?: string;
  /**
   * `panel` is the preview column's own 16:9 box — bordered, rounded and
   * capped in height. `card` fills the thumbnail band a card gives it
   * instead: the band's own shape, with no border or rounding of its own
   * (the card clips its top corners) and the blocks drawn small enough for a
   * 96px band.
   */
  variant?: "panel" | "card";
}

/** One glyph shape as its SVG element. A rect, a circle and a path are
 * filled; a line is stroked. */
const GlyphPrimitive = ({ shape, palette }: { shape: GlyphShape; palette: LayoutPalette }) => {
  const color = palette.glyph[shape.role];
  switch (shape.shape) {
    case "rect":
      return <rect x={shape.x} y={shape.y} width={shape.w} height={shape.h} rx={shape.rx} fill={color} />;
    case "circle":
      return <circle cx={shape.cx} cy={shape.cy} r={shape.r} fill={color} />;
    case "line":
      return (
        <line
          x1={shape.x1}
          y1={shape.y1}
          x2={shape.x2}
          y2={shape.y2}
          stroke={color}
          strokeWidth={shape.width}
          strokeLinecap="round"
        />
      );
    default:
      return shape.stroke ? (
        <path d={shape.d} fill="none" stroke={color} strokeWidth={shape.stroke} />
      ) : (
        <path d={shape.d} fill={color} />
      );
  }
};

/**
 * One layout element: the placeholder glyph its type reads as, the basemap
 * frame a map element's camera names, its own outline, and whatever real
 * content it carries — a text block's markup, a legend's title.
 *
 * The scaffold is live DOM rather than a rasterised string, so the basemap is
 * referenced by url and fetched by the browser; the placeholder map stays
 * under it, which is what shows where the request does not answer. The
 * markup comes from `sanitizeNoteHtml`, which is what makes writing it as
 * HTML safe.
 */
const DrawnElement = ({ element, palette }: { element: LayoutDrawingElement; palette: LayoutPalette }) => {
  const basemap = element.viewState
    ? staticMapUrl(element.viewState, { width: element.content.w, height: element.content.h })
    : null;
  return (
    <g
      data-testid="template-preview-element"
      data-type={element.type}
      opacity={element.opacity < 1 ? element.opacity : undefined}>
      {/* What the element itself asks to be filled and framed with — and
       * nothing where it asks for neither, which is a layout element's own
       * default. */}
      {element.background && (
        <rect
          x={element.x}
          y={element.y}
          width={element.w}
          height={element.h}
          fill={element.background.color}
          fillOpacity={element.background.opacity}
        />
      )}
      {element.glyphs.map((shape, shapeIndex) => (
        <GlyphPrimitive key={`glyph-${shapeIndex}`} shape={shape} palette={palette} />
      ))}
      {basemap && (
        <image
          data-testid="template-preview-basemap"
          x={element.content.x}
          y={element.content.y}
          width={element.content.w}
          height={element.content.h}
          preserveAspectRatio="xMidYMid slice"
          href={basemap}
        />
      )}
      {/* An element with nothing at all to draw keeps the faintest outline,
       * so its box does not vanish from the page. */}
      {element.placeholder && (
        <rect
          x={element.x}
          y={element.y}
          width={element.w}
          height={element.h}
          rx={element.radius}
          fill="none"
          stroke={elementStrokeColor(element.filled, palette)}
          strokeWidth={element.border}
        />
      )}
      {/* The element's own border goes over its content rather than under
       * it: a basemap covers its whole box, and the border reads on top. */}
      {element.frame && (
        <rect
          x={element.x}
          y={element.y}
          width={element.w}
          height={element.h}
          fill="none"
          stroke={element.frame.color}
          strokeWidth={element.frame.width}
        />
      )}
      {element.html && element.prose && (
        <foreignObject
          data-testid="template-preview-prose"
          x={element.prose.x}
          y={element.prose.y}
          width={element.prose.w}
          height={element.prose.h}>
          <div
            className={NOTE_CLASS}
            style={{
              fontFamily: NOTE_FONT_STACK,
              fontSize: element.prose.fontSize,
              color: palette.text,
              width: "100%",
              height: "100%",
            }}>
            {/* The same rules the stored PNG carries, scoped to the note's
             * own class so nothing else on the page can be reached by them. */}
            <style>{NOTE_CSS}</style>
            <div dangerouslySetInnerHTML={{ __html: element.html }} />
          </div>
        </foreignObject>
      )}
      {element.title && (
        <text
          data-testid="template-preview-element-title"
          x={element.title.x}
          y={element.title.y}
          fontFamily={NOTE_FONT_STACK}
          fontSize={element.title.fontSize}
          fontWeight={600}
          fill={palette.text}>
          {element.title.text}
        </text>
      )}
    </g>
  );
};

/**
 * A node's icon as the canvas's own component, sized and coloured by the
 * drawing rather than by the class MUI would apply: a nested `<svg>` at the
 * glyph's place in the card, with the tool-icon tones set on it so a
 * two-tone glyph reads its own colours off the drawing's palette. Null for a
 * node type that shows no icon.
 */
const NodeGlyph = ({ node, palette }: { node: DrawingNode; palette: WorkflowPalette }) => {
  const element = node.icons ? nodeIconElement(node.type, node.icon) : null;
  if (!element || !node.icons) return null;
  const { glyphX, glyphY, glyphSize } = node.icons;
  // `x`/`y` place the nested svg; the size and the colours go through
  // `style`, which wins over the emotion class's own `width: 1em`.
  return cloneElement(element, {
    x: glyphX,
    y: glyphY,
    style: {
      width: glyphSize,
      height: glyphSize,
      ...nodeIconToneStyle(palette.iconTones, nodeIconColor(node.type, palette)),
    },
  });
};

/**
 * A text annotation's own note, drawn as the canvas draws it: the sanitised
 * TipTap markup the descriptor carries, laid out inside the note's box in a
 * `foreignObject` so the headings, emphasis, per-run colours and alignment
 * the author applied survive into the preview. The markup comes from
 * `sanitizeNoteHtml`, which is what makes writing it as HTML safe. Null for
 * a note with no text.
 */
const DrawnNote = ({ node, palette }: { node: DrawingNode; palette: WorkflowPalette }) => {
  if (!node.html || !node.prose) return null;
  return (
    <foreignObject
      data-testid="template-preview-note"
      x={node.prose.x}
      y={node.prose.y}
      width={node.prose.w}
      height={node.prose.h}>
      <div
        className={NOTE_CLASS}
        style={{
          fontFamily: NOTE_FONT_STACK,
          fontSize: node.prose.fontSize,
          color: palette.noteText,
          width: "100%",
          height: "100%",
        }}>
        {/* The same rules the stored PNG carries, scoped to the note's own
         * class so nothing else on the page can be reached by them. */}
        <style>{NOTE_CSS}</style>
        <div dangerouslySetInnerHTML={{ __html: node.html }} />
      </div>
    </foreignObject>
  );
};

/** One node's card: the same geometry the stored PNG is written from, as
 * React elements. */
const DrawnNode = ({ node, palette }: { node: DrawingNode; palette: WorkflowPalette }) => {
  // A note is painted in its own colour — the canvas's 5% fill and 2px
  // border of it — and falls back to the colour the canvas defaults to.
  const noteColor = node.color ?? palette.annotation;
  return (
    <g data-testid="template-preview-node" data-type={node.type}>
      <rect
        x={node.x}
        y={node.y}
        width={node.w}
        height={node.h}
        rx={node.radius}
        fill={node.annotation ? noteColor : palette.nodeFill}
        fillOpacity={node.annotation ? ANNOTATION_FILL_OPACITY : undefined}
        stroke={node.annotation ? noteColor : palette.nodeStroke}
        strokeWidth={node.border}
      />
      {node.icons && (
        <rect
          x={node.icons.x}
          y={node.icons.y}
          width={node.icons.size}
          height={node.icons.size}
          rx={node.icons.radius}
          fill="none"
          stroke={palette.nodeStroke}
          strokeWidth={node.icons.border}
        />
      )}
      <NodeGlyph node={node} palette={palette} />
      {node.label && (
        <text
          x={node.labelX}
          y={node.labelY}
          fontSize={node.fontSize}
          fontWeight={700}
          letterSpacing={node.letterSpacing}
          fill={palette.text}>
          {node.label}
        </text>
      )}
      <DrawnNote node={node} palette={palette} />
      {node.handles.map((handle, index) => (
        <circle
          key={index}
          cx={handle.x}
          cy={handle.y}
          r={handle.r}
          fill={palette.handle}
          stroke={palette.handleRing}
          strokeWidth={handle.ring}
        />
      ))}
    </g>
  );
};

/** The whole workflow drawing as React elements: the canvas's dot grid, the
 * edges and the cards. The grid and the shadow are defined per drawing, so
 * two scaffolds on one page do not share a scale-dependent definition. */
const DrawnWorkflow = ({
  drawing,
  palette,
  id,
}: {
  drawing: WorkflowDrawing;
  palette: WorkflowPalette;
  id: string;
}) => (
  <>
    <defs>
      <pattern
        id={`${id}-grid`}
        width={drawing.grid.gap}
        height={drawing.grid.gap}
        patternUnits="userSpaceOnUse">
        <circle cx={drawing.grid.dot} cy={drawing.grid.dot} r={drawing.grid.dot} fill={palette.grid} />
      </pattern>
    </defs>
    <rect width={drawing.width} height={drawing.height} fill={palette.ground} />
    <rect width={drawing.width} height={drawing.height} fill={`url(#${id}-grid)`} />
    {drawing.edges.map((edge, index) => (
      <path
        key={index}
        data-testid="template-preview-edge"
        d={edge.d}
        fill="none"
        stroke={palette.edge}
        strokeWidth={edge.width}
        strokeLinecap="round"
      />
    ))}
    {drawing.nodes.map((node, index) => (
      <DrawnNode key={index} node={node} palette={palette} />
    ))}
  </>
);

/**
 * A drawing of a payload's structure rather than a picture of it — a
 * workflow's nodes where they sit on the canvas, drawn as the cards the
 * canvas draws them as, a layout's page with a placeholder glyph per block,
 * a project's mark.
 *
 * It draws from a descriptor the caller built from a config it can see, so
 * the only place it is shown is the save dialog's live preview: a saved
 * template's picture is the PNG written from this same drawing, and one with
 * no picture shows the generic template mark rather than a reconstruction.
 */
const TemplatePreviewFallback = ({
  descriptor,
  payloadKind,
  id = "draft",
  variant = "panel",
}: TemplatePreviewFallbackProps) => {
  const theme = useTheme();
  const { t } = useTranslation("common");

  // The box the drawings size off. In the panel it is a 16/9 box of the
  // column's width, capped so it cannot take the whole dialog. In a card it
  // is the thumbnail band itself, whatever shape the band is: the drawings
  // are `meet`-fitted svgs, so they scale down inside a band flatter than
  // 16/9 rather than being cropped by it — a layout template's portrait page
  // would lose its head and foot to a crop.
  const frameBase = {
    backgroundColor: theme.palette.action.hover,
    overflow: "hidden",
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
  } as const;
  const frame =
    variant === "card"
      ? ({ ...frameBase, width: "100%", height: "100%" } as const)
      : ({
          ...frameBase,
          width: "100%",
          aspectRatio: "16 / 9",
          maxHeight: 420,
          borderRadius: "10px",
          border: `1px solid ${theme.palette.divider}`,
        } as const);

  if (payloadKind === "project") {
    return (
      <Box
        sx={{
          ...frame,
          backgroundImage: `linear-gradient(135deg, ${alpha(theme.palette.primary.main, 0.14)}, ${alpha(
            theme.palette.info.main,
            0.1
          )})`,
        }}>
        <Box data-testid="template-preview-project">
          <MarkBlock kind="project" size={64} />
        </Box>
      </Box>
    );
  }

  const viewBox = `0 0 ${SCAFFOLD_FRAME.width} ${SCAFFOLD_FRAME.height}`;

  if (descriptor?.kind === "workflow" && descriptor.nodes.length > 0) {
    // The same geometry the stored snapshot is written from, in the reader's
    // theme and at the frame's size — so the scaffold and the PNG that
    // replaces it are the same picture. Here it is drawn as React elements,
    // which keeps React's server renderer out of every page that can open a
    // template preview.
    return (
      <Box sx={frame}>
        <svg width="100%" height="100%" viewBox={viewBox} preserveAspectRatio="xMidYMid meet">
          <DrawnWorkflow
            drawing={workflowDrawing(descriptor, { size: SCAFFOLD_FRAME, translate: t })}
            palette={workflowPaletteForTheme(theme)}
            id={`template-preview-${id}`}
          />
        </svg>
      </Box>
    );
  }

  if (descriptor?.kind === "layout") {
    // The same geometry the stored snapshot is written from, in the reader's
    // theme and at the frame's size — so the scaffold and the PNG that
    // replaces it are the same wireframe.
    const drawing = layoutDrawing(descriptor, { size: SCAFFOLD_FRAME });
    const palette = layoutPaletteForTheme(theme);
    return (
      <Box sx={frame}>
        <svg width="100%" height="100%" viewBox={viewBox} preserveAspectRatio="xMidYMid meet">
          <rect
            data-testid="template-preview-page"
            data-orientation={drawing.orientation}
            x={drawing.page.x}
            y={drawing.page.y}
            width={drawing.page.w}
            height={drawing.page.h}
            rx={drawing.page.radius}
            fill={palette.page}
            stroke={palette.pageBorder}
            strokeWidth={drawing.page.border}
          />
          {drawing.elements.map((element, index) => (
            <DrawnElement key={`element-${index}`} element={element} palette={palette} />
          ))}
        </svg>
      </Box>
    );
  }

  // Nothing to draw from: the same default a card shows — a blank page for a
  // layout, a step chain for a workflow — rather than invented content. The
  // page it prints on is not in a descriptor, so this draws the neutral one.
  return <TemplateDefaultThumbnail payloadKind={payloadKind} variant={variant} />;
};

export default TemplatePreviewFallback;
