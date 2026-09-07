"use client";

import { Box, useTheme } from "@mui/material";

import { LAYOUT_SNAPSHOT_PALETTE } from "@/lib/templates/layoutSnapshot";
import type { LayoutPageDescription } from "@/lib/templates/previewGeometry";
import { PREVIEW_PAGE_SIZE } from "@/lib/templates/previewGeometry";
import { SNAPSHOT_PALETTE } from "@/lib/templates/workflowSnapshot";
import type { TemplatePayloadKind } from "@/lib/validations/template";

/** How many steps the workflow chain draws. */
const CHAIN_STEPS = 3;

/**
 * What a placeholder block is filled with: the layout snapshot's muted
 * element grey, in both drawings, so a blank page's blocks and a chain
 * step's bar read as the same kind of stand-in.
 */
const PLACEHOLDER_FILL = LAYOUT_SNAPSHOT_PALETTE.elementStroke;

/**
 * The size the drawing is rendered at: `panel` is a preview column's 16:9
 * box, bordered and capped in height; `card` fills the thumbnail band it is
 * handed, with the smaller blocks a 96px band needs; `mark` is a browser
 * row's 34px tile, where the same drawing is a glyph.
 */
type TemplateDefaultVariant = "panel" | "card" | "mark";

/** The sheet's furniture per size: the page's own padding, the gap between
 * its blocks, the height of the two bars and the corner radius. */
const PAGE_SCALE: Record<
  TemplateDefaultVariant,
  { padding: string; gap: string; bar: number; radius: string }
> = {
  mark: { padding: "3px", gap: "2px", bar: 2, radius: "1px" },
  card: { padding: "7px", gap: "4px", bar: 4, radius: "2px" },
  panel: { padding: "16px", gap: "10px", bar: 10, radius: "4px" },
};

/**
 * The chain's furniture per size: the frame's padding, the gap around a
 * link, the step's width and the link's length, the step's own padding, the
 * height of the bar inside a step — 0 where the step is too small to hold
 * one, which at a row's size is a single pixel of noise — and the dotted
 * ground's tile.
 */
const CHAIN_SCALE: Record<
  TemplateDefaultVariant,
  {
    padding: string;
    gap: string;
    step: number;
    stepMax: number;
    link: number;
    stepPadding: string;
    bar: number;
    dots: string;
  }
> = {
  mark: {
    padding: "2px",
    gap: "2px",
    step: 5,
    stepMax: 6,
    link: 2,
    stepPadding: "6px 2px",
    bar: 0,
    dots: "7px 7px",
  },
  // The chain at the band's size rather than the panel's: three 96px-wide
  // steps with 24px of padding do not fit a card's width, and a row clipped
  // mid-step reads as a rendering fault.
  card: {
    padding: "12px",
    gap: "8px",
    step: 40,
    stepMax: 56,
    link: 10,
    stepPadding: "10px 12px",
    bar: 8,
    dots: "14px 14px",
  },
  panel: {
    padding: "24px",
    gap: "14px",
    step: 96,
    stepMax: 128,
    link: 22,
    stepPadding: "10px 12px",
    bar: 8,
    dots: "14px 14px",
  },
};

interface TemplateDefaultThumbnailProps {
  payloadKind: TemplatePayloadKind;
  /**
   * The page the template stores, as `layoutPageDescription` reads it off the
   * row: the size and the orientation, and nothing from the frozen config.
   * Null — a layout saved before the fields existed — draws A4 portrait,
   * which is what the layout canvas itself falls back to.
   */
  page?: LayoutPageDescription | null;
  variant?: TemplateDefaultVariant;
}

/** Which payload kinds draw a default of their own. A project keeps the
 * template mark its caller draws: there is nothing to say about it that the
 * mark does not already say. */
export const hasTemplateDefaultThumbnail = (payloadKind: TemplatePayloadKind): boolean =>
  payloadKind === "layout" || payloadKind === "workflow";

/**
 * What a template with no stored picture shows: a blank page in the
 * orientation it prints on for a layout, a three-step chain for a workflow.
 *
 * It is drawn from the two page fields the template row carries and from
 * nothing else — a reader has no config, so this must never look like a
 * wireframe of the real elements. Card, row and preview render the same
 * component so one template reads the same in all three.
 */
const TemplateDefaultThumbnail = ({ payloadKind, page, variant = "card" }: TemplateDefaultThumbnailProps) => {
  const theme = useTheme();

  // Fixed light, not the reader's theme: a generated thumbnail is always
  // light — white paper for a layout, the light dotted canvas for a workflow
  // — so a default drawn in the same palette sits beside a real one without
  // changing appearance with the theme. Both palettes are the ones the
  // snapshot writers use, so the default and the picture that replaces it
  // cannot drift apart.
  const frameBase = {
    backgroundColor: LAYOUT_SNAPSHOT_PALETTE.ground,
    overflow: "hidden",
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
  } as const;
  const frame =
    variant === "panel"
      ? ({
          ...frameBase,
          width: "100%",
          aspectRatio: "16 / 9",
          maxHeight: 420,
          borderRadius: "10px",
          // The box's own chrome stays with the theme: it is the frame the
          // preview column draws around a real thumbnail too. Only what is
          // drawn inside it is fixed light.
          border: `1px solid ${theme.palette.divider}`,
        } as const)
      : ({ ...frameBase, width: "100%", height: "100%" } as const);

  if (payloadKind === "layout") {
    const size = page ?? PREVIEW_PAGE_SIZE.A4;
    const landscape = size.width > size.height;
    // The page is sized off the frame's height in both orientations and
    // capped in width, so a landscape sheet stays inside a narrow band
    // instead of being cropped by it.
    const scale = PAGE_SCALE[variant];

    return (
      <Box sx={frame}>
        <Box
          data-testid="template-preview-page"
          data-orientation={landscape ? "landscape" : "portrait"}
          // The sheet's own shape, inline: it is one value per page size, so
          // it does not belong in a generated class of its own.
          style={{ aspectRatio: `${size.width} / ${size.height}` }}
          sx={{
            height: "84%",
            maxWidth: "86%",
            flexShrink: 0,
            padding: scale.padding,
            display: "flex",
            flexDirection: "column",
            gap: scale.gap,
            borderRadius: scale.radius,
            backgroundColor: LAYOUT_SNAPSHOT_PALETTE.page,
            // A hairline rather than a shadow: the same page border the
            // generated wireframe draws.
            border: `1px solid ${LAYOUT_SNAPSHOT_PALETTE.pageBorder}`,
          }}>
          <Box
            data-testid="template-page-block"
            sx={{
              height: scale.bar,
              width: "62%",
              flexShrink: 0,
              borderRadius: "2px",
              backgroundColor: LAYOUT_SNAPSHOT_PALETTE.glyph.line,
            }}
          />
          <Box
            data-testid="template-page-block"
            sx={{
              flex: 1,
              minHeight: 0,
              borderRadius: "2px",
              backgroundColor: PLACEHOLDER_FILL,
            }}
          />
          <Box
            data-testid="template-page-block"
            sx={{
              height: scale.bar,
              width: "38%",
              flexShrink: 0,
              borderRadius: "2px",
              backgroundColor: PLACEHOLDER_FILL,
            }}
          />
        </Box>
      </Box>
    );
  }

  if (payloadKind !== "workflow") return null;

  const chain = CHAIN_SCALE[variant];

  return (
    <Box
      data-testid="template-default-workflow"
      sx={{
        ...frame,
        gap: chain.gap,
        padding: chain.padding,
        // The canvas's own ground and dot colour, the pair the generated
        // workflow picture is painted on.
        backgroundColor: SNAPSHOT_PALETTE.ground,
        backgroundImage: `radial-gradient(${SNAPSHOT_PALETTE.grid} 1px, transparent 1px)`,
        backgroundSize: chain.dots,
      }}>
      {Array.from({ length: CHAIN_STEPS }, (_unused, index) => (
        <Box key={index} sx={{ display: "flex", alignItems: "center", gap: chain.gap, minWidth: 0 }}>
          {index > 0 && (
            <Box
              sx={{
                width: chain.link,
                height: 1,
                backgroundColor: SNAPSHOT_PALETTE.edge,
                flexShrink: 0,
              }}
            />
          )}
          <Box
            data-testid="template-preview-step"
            sx={{
              minWidth: chain.step,
              maxWidth: chain.stepMax,
              padding: chain.stepPadding,
              borderRadius: variant === "mark" ? "2px" : "8px",
              // A node as the canvas paints it: paper with its divider
              // hairline, and no shadow, which the drawing does not carry.
              backgroundColor: SNAPSHOT_PALETTE.nodeFill,
              border: `1px solid ${SNAPSHOT_PALETTE.nodeStroke}`,
            }}>
            {chain.bar > 0 && (
              <Box
                sx={{
                  height: chain.bar,
                  borderRadius: "2px",
                  backgroundColor: PLACEHOLDER_FILL,
                }}
              />
            )}
          </Box>
        </Box>
      ))}
    </Box>
  );
};

export default TemplateDefaultThumbnail;
