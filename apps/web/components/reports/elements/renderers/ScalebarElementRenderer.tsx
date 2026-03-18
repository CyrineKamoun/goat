"use client";

import { Box, Typography } from "@mui/material";
import React, { useMemo } from "react";

import type { TypographyStyle } from "@/lib/constants/typography";
import { DEFAULT_FONT_FAMILY } from "@/lib/constants/typography";
import { mmToPx, SCREEN_DPI } from "@/lib/print/units";
import type { ReportElement } from "@/lib/validations/reportLayout";

import {
  type ScalebarElementConfig,
  type ScalebarUnit,
  getUnitAbbreviation,
} from "@/components/reports/elements/config/ScalebarElementConfig";

/**
 * Convert TypographyStyle to MUI sx props
 */
function typographyToSx(style?: TypographyStyle): Record<string, unknown> {
  if (!style) return { fontFamily: DEFAULT_FONT_FAMILY };
  const sx: Record<string, unknown> = {};
  sx.fontFamily = style.fontFamily || DEFAULT_FONT_FAMILY;
  if (style.fontSize) sx.fontSize = style.fontSize;
  if (style.fontColor) sx.color = style.fontColor;
  if (style.fontWeight) sx.fontWeight = style.fontWeight;
  return sx;
}

interface ScalebarElementRendererProps {
  element: ReportElement;
  mapElements?: ReportElement[];
  zoom?: number;
}

/**
 * Convert meters to the target unit
 */
const metersToUnit = (meters: number, unit: ScalebarUnit): number => {
  switch (unit) {
    case "kilometers":
      return meters / 1000;
    case "meters":
      return meters;
    case "feet":
      return meters * 3.28084;
    case "yards":
      return meters * 1.09361;
    case "miles":
      return meters / 1609.344;
    case "nautical_miles":
      return meters / 1852;
    case "centimeters":
      return meters * 100;
    case "millimeters":
      return meters * 1000;
    case "inches":
      return meters * 39.3701;
    case "map_units":
    default:
      return meters;
  }
};

/**
 * Get the largest nice round number that fits within the given value.
 * Rounds DOWN to 1, 2, or 5 × 10^n so the bar never exceeds the available width.
 */
const getNiceNumber = (value: number): number => {
  if (value <= 0) return 1;
  const magnitude = Math.pow(10, Math.floor(Math.log10(value)));
  const normalized = value / magnitude;

  // Round DOWN: pick the largest nice number <= value
  if (normalized < 2) return magnitude;
  if (normalized < 5) return 2 * magnitude;
  if (normalized < 10) return 5 * magnitude;
  return 10 * magnitude;
};

/**
 * Calculate scale bar parameters using "Fit Segment Width" mode (QGIS-style).
 *
 * The scalebar's total distance is rounded to a nice number, and the bar width
 * adjusts (shrinks) so its pixel length exactly represents that distance.
 * Changing segment count only subdivides the same total — labels stay consistent.
 *
 * Left segments are subdivisions of one right-side segment (finer detail),
 * placed to the left of the zero mark.
 */
const calculateScaleBarParams = (
  metersPerPixel: number,
  maxWidthPx: number,
  unit: ScalebarUnit,
  segmentsRight: number,
  segmentsLeft: number
) => {
  const effectiveSegmentsRight = Math.max(1, segmentsRight);

  // Total distance the max width represents
  const rawTotalMeters = metersPerPixel * maxWidthPx;
  const rawTotalUnits = metersToUnit(rawTotalMeters, unit);

  // Round the TOTAL to a nice number (not per-segment)
  const niceTotalUnits = getNiceNumber(rawTotalUnits);

  // Width ratio: how much of the max width the bar actually occupies
  // getNiceNumber can round up (e.g. 3.5 → 5), so clamp to 1
  const widthRatio = Math.min(niceTotalUnits / rawTotalUnits, 1);

  // Right segment value: total divided evenly
  const segmentValueRight = niceTotalUnits / effectiveSegmentsRight;

  // Left segments subdivide one right segment
  const effectiveSegmentsLeft = Math.max(0, segmentsLeft);
  const segmentValueLeft = effectiveSegmentsLeft > 0
    ? segmentValueRight / effectiveSegmentsLeft
    : 0;

  return {
    niceTotalUnits,
    segmentValueRight,
    segmentValueLeft,
    segmentsRight: effectiveSegmentsRight,
    segmentsLeft: effectiveSegmentsLeft,
    widthRatio,
  };
};

/**
 * Build segment data: relative widths and label values.
 *
 * Left segments subdivide the FIRST right segment into finer detail.
 * All labels are positive, reading left to right: 0 → subdivisions → full segments → total.
 *
 * Example with segmentsRight=2, segmentsLeft=4, segmentValueRight=5:
 *   Segments: [1.25][1.25][1.25][1.25][    5    ]
 *   Labels:   0   1.25  2.5  3.75  5         10
 *
 * Returns:
 * - segments: array of { relativeWidth } for rendering bars
 * - labels: array of { value, isLast } for rendering labels at segment boundaries
 */
const buildSegmentData = (
  segmentsLeft: number,
  segmentsRight: number,
  segmentValueRight: number,
  segmentValueLeft: number,
  labelMultiplier: number,
) => {
  const segments: { relativeWidth: number }[] = [];
  const labels: { value: number; isLast: boolean }[] = [];

  if (segmentsLeft > 0) {
    // First right segment is subdivided into segmentsLeft sub-segments
    for (let i = 0; i < segmentsLeft; i++) {
      segments.push({ relativeWidth: 1 / segmentsLeft });
      labels.push({
        value: i * segmentValueLeft * labelMultiplier,
        isLast: false,
      });
    }
    // Remaining right segments (starting from the 2nd one)
    for (let i = 1; i < segmentsRight; i++) {
      segments.push({ relativeWidth: 1 });
      labels.push({
        value: i * segmentValueRight * labelMultiplier,
        isLast: false,
      });
    }
  } else {
    // No left subdivisions — all segments are uniform
    for (let i = 0; i < segmentsRight; i++) {
      segments.push({ relativeWidth: 1 });
      labels.push({
        value: i * segmentValueRight * labelMultiplier,
        isLast: false,
      });
    }
  }

  // Final label (end of last right segment)
  labels.push({
    value: segmentsRight * segmentValueRight * labelMultiplier,
    isLast: true,
  });

  return { segments, labels };
};

/**
 * Format a label value to avoid floating point artifacts (e.g. 5.000000001 -> "5")
 */
const formatLabelValue = (value: number): string => {
  return parseFloat(value.toPrecision(10)).toString();
};

// Common props interface for all bar-style scalebar components
interface ScalebarStyleProps {
  segments: { relativeWidth: number }[];
  labels: { value: number; isLast: boolean }[];
  labelUnit: string;
  height: number;
  labelSx?: Record<string, unknown>;
}

/**
 * Render labels row (shared by all bar styles)
 */
const ScalebarLabels: React.FC<{
  segments: { relativeWidth: number }[];
  labels: { value: number; isLast: boolean }[];
  labelUnit: string;
  labelSx?: Record<string, unknown>;
}> = ({ segments, labels, labelUnit, labelSx }) => {
  // Position labels at segment boundaries using the same flex proportions
  // We create a flex row where each "gap" between labels matches the segment width
  const totalRelativeWidth = segments.reduce((sum, s) => sum + s.relativeWidth, 0);

  return (
    <Box sx={{ position: "relative", width: "100%", height: "1.2em" }}>
      {labels.map((label, i) => {
        // Calculate the percentage position of this label
        let position = 0;
        for (let j = 0; j < i; j++) {
          position += segments[j].relativeWidth;
        }
        const pct = (position / totalRelativeWidth) * 100;
        const isFirst = i === 0;

        // First label: left-aligned at 0. Last label: right-aligned at end.
        // Middle labels: centered on their position.
        const positionSx = label.isLast
          ? { right: 0 }
          : isFirst
            ? { left: 0 }
            : { left: `${pct}%`, transform: "translateX(-50%)" };

        return (
          <Typography
            key={i}
            variant="caption"
            sx={{
              position: "absolute",
              fontSize: "0.65rem",
              whiteSpace: "nowrap",
              ...positionSx,
              ...labelSx,
            }}>
            {formatLabelValue(label.value)}
            {label.isLast && labelUnit ? ` ${labelUnit}` : ""}
          </Typography>
        );
      })}
    </Box>
  );
};

/**
 * Render segments bar row (shared by box styles)
 */
const SegmentsBar: React.FC<{
  segments: { relativeWidth: number }[];
  height: number;
  invertColors?: boolean;
  border?: boolean;
}> = ({ segments, height, invertColors = false, border = true }) => (
  <Box
    sx={{
      display: "flex",
      height: `${height}px`,
      ...(border ? { border: "1px solid #000" } : {}),
    }}>
    {segments.map((seg, i) => (
      <Box
        key={i}
        sx={{
          flex: seg.relativeWidth,
          backgroundColor: invertColors
            ? (i % 2 === 0 ? "#fff" : "#000")
            : (i % 2 === 0 ? "#000" : "#fff"),
        }}
      />
    ))}
  </Box>
);

/**
 * Render Single Box style scalebar
 */
const SingleBoxScalebar: React.FC<ScalebarStyleProps> = ({
  segments, labels, labelUnit, height, labelSx,
}) => (
  <Box sx={{ width: "100%", display: "flex", flexDirection: "column", gap: 0.5 }}>
    <ScalebarLabels segments={segments} labels={labels} labelUnit={labelUnit} labelSx={labelSx} />
    <SegmentsBar segments={segments} height={height} />
  </Box>
);

/**
 * Render Double Box style scalebar
 */
const DoubleBoxScalebar: React.FC<ScalebarStyleProps> = ({
  segments, labels, labelUnit, height, labelSx,
}) => (
  <Box sx={{ width: "100%", display: "flex", flexDirection: "column", gap: 0.5 }}>
    <ScalebarLabels segments={segments} labels={labels} labelUnit={labelUnit} labelSx={labelSx} />
    <Box sx={{ display: "flex", flexDirection: "column", border: "1px solid #000" }}>
      <SegmentsBar segments={segments} height={height / 2} border={false} />
      <SegmentsBar segments={segments} height={height / 2} border={false} invertColors />
    </Box>
  </Box>
);

/**
 * Render Line Ticks style scalebar
 */
const LineTicksScalebar: React.FC<ScalebarStyleProps & { tickPosition: "middle" | "down" | "up" }> = ({
  segments, labels, labelUnit, height, labelSx, tickPosition,
}) => {
  const totalRelativeWidth = segments.reduce((sum, s) => sum + s.relativeWidth, 0);

  // Calculate tick positions (at each segment boundary)
  const tickPositions: number[] = [];
  let cumulative = 0;
  for (let i = 0; i <= segments.length; i++) {
    tickPositions.push((cumulative / totalRelativeWidth) * 100);
    if (i < segments.length) cumulative += segments[i].relativeWidth;
  }

  return (
    <Box sx={{ width: "100%", display: "flex", flexDirection: "column", gap: 0.5 }}>
      <ScalebarLabels segments={segments} labels={labels} labelUnit={labelUnit} labelSx={labelSx} />
      <Box sx={{ position: "relative", height: `${height}px`, width: "100%" }}>
        {/* Horizontal line */}
        <Box
          sx={{
            position: "absolute",
            left: 0,
            right: 0,
            top: tickPosition === "up" ? `${height - 1}px` : tickPosition === "down" ? 0 : "50%",
            height: "1px",
            backgroundColor: "#000",
          }}
        />
        {/* Ticks */}
        {tickPositions.map((pct, i) => (
          <Box
            key={i}
            sx={{
              position: "absolute",
              left: `${pct}%`,
              top: 0,
              width: "1px",
              height: "100%",
              backgroundColor: "#000",
              transform: "translateX(-50%)",
            }}
          />
        ))}
      </Box>
    </Box>
  );
};

/**
 * Render Stepped Line style scalebar
 */
const SteppedLineScalebar: React.FC<ScalebarStyleProps> = ({
  segments, labels, labelUnit, height, labelSx,
}) => {
  const totalRelativeWidth = segments.reduce((sum, s) => sum + s.relativeWidth, 0);

  // Calculate segment positions and widths as percentages
  const segmentLayout: { leftPct: number; widthPct: number }[] = [];
  let cumulative = 0;
  for (const seg of segments) {
    segmentLayout.push({
      leftPct: (cumulative / totalRelativeWidth) * 100,
      widthPct: (seg.relativeWidth / totalRelativeWidth) * 100,
    });
    cumulative += seg.relativeWidth;
  }

  return (
    <Box sx={{ width: "100%", display: "flex", flexDirection: "column", gap: 0.5 }}>
      <ScalebarLabels segments={segments} labels={labels} labelUnit={labelUnit} labelSx={labelSx} />
      <Box sx={{ position: "relative", height: `${height}px`, width: "100%" }}>
        {segmentLayout.map((layout, i) => {
          const isEven = i % 2 === 0;
          return (
            <React.Fragment key={i}>
              {/* Horizontal segment */}
              <Box
                sx={{
                  position: "absolute",
                  left: `${layout.leftPct}%`,
                  width: `${layout.widthPct}%`,
                  top: isEven ? 0 : `${height - 1}px`,
                  height: "1px",
                  backgroundColor: "#000",
                }}
              />
              {/* Vertical connector */}
              {i > 0 && (
                <Box
                  sx={{
                    position: "absolute",
                    left: `${layout.leftPct}%`,
                    top: 0,
                    width: "1px",
                    height: `${height}px`,
                    backgroundColor: "#000",
                    transform: "translateX(-50%)",
                  }}
                />
              )}
            </React.Fragment>
          );
        })}
        {/* End ticks */}
        <Box
          sx={{
            position: "absolute",
            left: 0,
            top: 0,
            width: "1px",
            height: `${height}px`,
            backgroundColor: "#000",
          }}
        />
        <Box
          sx={{
            position: "absolute",
            right: 0,
            top: 0,
            width: "1px",
            height: `${height}px`,
            backgroundColor: "#000",
          }}
        />
      </Box>
    </Box>
  );
};

/**
 * Render Hollow style scalebar
 */
const HollowScalebar: React.FC<ScalebarStyleProps> = ({
  segments, labels, labelUnit, height, labelSx,
}) => (
  <Box sx={{ width: "100%", display: "flex", flexDirection: "column", gap: 0.5 }}>
    <ScalebarLabels segments={segments} labels={labels} labelUnit={labelUnit} labelSx={labelSx} />
    <Box
      sx={{
        display: "flex",
        height: `${height}px`,
        border: "1px solid #000",
        backgroundColor: "#fff",
      }}>
      {segments.map((seg, i) => (
        <Box
          key={i}
          sx={{
            flex: seg.relativeWidth,
            borderRight: i < segments.length - 1 ? "1px solid #000" : "none",
          }}
        />
      ))}
    </Box>
  </Box>
);

/**
 * Render Numeric style scalebar (just text showing scale ratio)
 */
const NumericScalebar: React.FC<{
  scaleDenominator: number;
  labelSx?: Record<string, unknown>;
}> = ({ scaleDenominator, labelSx }) => {
  const formatScale = (ratio: number): string => {
    if (ratio >= 1000000) {
      return `1:${(ratio / 1000000).toFixed(1)}M`;
    }
    if (ratio >= 1000) {
      return `1:${(ratio / 1000).toFixed(0)}K`;
    }
    return `1:${ratio}`;
  };

  return (
    <Box
      sx={{
        width: "100%",
        height: "100%",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
      }}>
      <Typography variant="body2" sx={{ fontWeight: 500, ...labelSx }}>
        {formatScale(scaleDenominator)}
      </Typography>
    </Box>
  );
};

/**
 * Scalebar Element Renderer for print reports
 *
 * Uses QGIS-style "Fit Segment Width" mode: the total distance is rounded
 * to a nice number, and the bar width adjusts so its pixel length exactly
 * represents that distance. Left segments are subdivisions of one right segment.
 */
const ScalebarElementRenderer: React.FC<ScalebarElementRendererProps> = ({
  element,
  mapElements = [],
  zoom = 1,
}) => {
  // Extract config
  const config = (element.config || {}) as ScalebarElementConfig;
  const mapElementId = config.mapElementId;
  const style = config.style ?? "single_box";
  const unit = config.unit ?? "kilometers";
  const labelMultiplier = config.labelMultiplier ?? 1;
  const labelUnit = config.labelUnit ?? getUnitAbbreviation(unit);
  const height = config.height ?? 8;
  const segmentsLeft = config.segmentsLeft ?? 0;
  const segmentsRight = config.segmentsRight ?? 2;

  const METERS_PER_PIXEL_SCREEN = 0.0254 / SCREEN_DPI;

  // Max width of the scalebar element in CSS pixels (from element dimensions in mm)
  const maxWidthPx = mmToPx(element.position.width, SCREEN_DPI);

  // Get connected map's scale
  const { mapScale, scaleDenominator } = useMemo(() => {
    if (!mapElementId || !mapElements.length) {
      const defaultMpp = 100;
      return {
        mapScale: defaultMpp,
        scaleDenominator: Math.round(defaultMpp / METERS_PER_PIXEL_SCREEN),
      };
    }

    const connectedMap = mapElements.find((el) => el.id === mapElementId);
    if (!connectedMap?.config?.viewState) {
      const defaultMpp = 100;
      return {
        mapScale: defaultMpp,
        scaleDenominator: Math.round(defaultMpp / METERS_PER_PIXEL_SCREEN),
      };
    }

    const viewState = connectedMap.config.viewState;
    const zoomLevel = viewState.zoom ?? 10;
    const latitude = viewState.latitude ?? 48;

    // Calculate meters per pixel at this zoom level and latitude
    // MapLibre uses 512px tiles, so constant = 40075016.686 / 512 = 78271.51696
    const metersPerPixel = (78271.51696 * Math.cos((latitude * Math.PI) / 180)) / Math.pow(2, zoomLevel);

    const storedScale = viewState.scale_denominator as number | undefined;
    const derivedScale = Math.round(metersPerPixel / METERS_PER_PIXEL_SCREEN);

    return {
      mapScale: metersPerPixel,
      scaleDenominator: storedScale ?? derivedScale,
    };
  }, [mapElementId, mapElements, METERS_PER_PIXEL_SCREEN]);

  // Calculate scale bar values (fit segment width mode)
  const scaleParams = useMemo(
    () => calculateScaleBarParams(mapScale, maxWidthPx, unit, segmentsRight, segmentsLeft),
    [mapScale, maxWidthPx, unit, segmentsRight, segmentsLeft]
  );

  // Build segment layout data
  const { segments, labels } = useMemo(
    () => buildSegmentData(
      scaleParams.segmentsLeft,
      scaleParams.segmentsRight,
      scaleParams.segmentValueRight,
      scaleParams.segmentValueLeft,
      labelMultiplier,
    ),
    [scaleParams, labelMultiplier]
  );

  const labelSx = useMemo(() => typographyToSx(config.typography), [config.typography]);

  const renderScalebar = () => {
    const commonProps: ScalebarStyleProps = {
      segments,
      labels,
      labelUnit,
      height,
      labelSx,
    };

    switch (style) {
      case "single_box":
        return <SingleBoxScalebar {...commonProps} />;
      case "double_box":
        return <DoubleBoxScalebar {...commonProps} />;
      case "line_ticks_middle":
        return <LineTicksScalebar {...commonProps} tickPosition="middle" />;
      case "line_ticks_down":
        return <LineTicksScalebar {...commonProps} tickPosition="down" />;
      case "line_ticks_up":
        return <LineTicksScalebar {...commonProps} tickPosition="up" />;
      case "stepped_line":
        return <SteppedLineScalebar {...commonProps} />;
      case "hollow":
        return <HollowScalebar {...commonProps} />;
      case "numeric":
        return <NumericScalebar scaleDenominator={scaleDenominator} labelSx={labelSx} />;
      default:
        return <SingleBoxScalebar {...commonProps} />;
    }
  };

  return (
    <Box
      sx={{
        width: `${100 / zoom}%`,
        height: `${100 / zoom}%`,
        display: "flex",
        alignItems: "center",
        justifyContent: "flex-start",
        p: 1,
        boxSizing: "border-box",
        transform: `scale(${zoom})`,
        transformOrigin: "top left",
      }}>
      <Box sx={{ width: `${scaleParams.widthRatio * 100}%` }}>{renderScalebar()}</Box>
    </Box>
  );
};

export default ScalebarElementRenderer;
