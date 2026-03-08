import { Box, Typography, useTheme } from "@mui/material";
import chroma from "chroma-js";
import { useCallback, useEffect, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { Cell, Label, Pie, PieChart, ResponsiveContainer } from "recharts";

import { useProjectLayerAggregationStats } from "@/lib/api/projects";
import { formatNumber } from "@/lib/utils/format-number";
import { normalizeValue } from "@/lib/utils/normalize-value";
import type { AggregationStatsQueryParams } from "@/lib/validations/project";
import { aggregationStatsQueryParams } from "@/lib/validations/project";
import type { ColorRange } from "@/lib/validations/layer";
import type { PieChartSchema } from "@/lib/validations/widget";
import { pieChartConfigSchema, pieLayoutTypes } from "@/lib/validations/widget";

import { useChartWidget } from "@/hooks/map/DashboardBuilderHooks";

import { StaleDataLoader } from "@/components/builder/widgets/common/StaleDataLoader";
import { WidgetStatusContainer } from "@/components/builder/widgets/common/WidgetStatusContainer";

const FALLBACK_COLORS = ["#5A1846", "#900C3F", "#C70039", "#E3611C", "#F1920E", "#FFC300"];
const OPACITY_MODIFIER = "33";

export const PieChartWidget = ({ config: rawConfig }: { config: PieChartSchema }) => {
  const { t, i18n } = useTranslation("common");
  const theme = useTheme();
  const { config, queryParams, layerId } = useChartWidget(
    rawConfig,
    pieChartConfigSchema,
    aggregationStatsQueryParams
  );

  const effectiveQueryParams = useMemo(() => {
    if (!queryParams) return queryParams;

    const customOrder = config?.setup?.custom_order;
    if (customOrder === undefined) {
      return queryParams;
    }

    if (customOrder.length === 0) {
      return queryParams;
    }

    return {
      ...queryParams,
      size: Math.max(5000, customOrder.length),
    } as AggregationStatsQueryParams;
  }, [queryParams, config?.setup?.custom_order]);

  const { aggregationStats, isLoading, isError } = useProjectLayerAggregationStats(
    layerId,
    effectiveQueryParams as AggregationStatsQueryParams
  );

  const originalData = useMemo(() => aggregationStats?.items || [], [aggregationStats]);

  // Apply custom order if defined
  const orderedData = useMemo(() => {
    if (!originalData.length) return originalData;

    const customOrder = config?.setup?.custom_order;

    if (customOrder === undefined) {
      return originalData;
    }

    if (customOrder.length === 0) {
      return [];
    }

    // Sort by custom order - items in customOrder come first in that order,
    // items not in customOrder are excluded
    // Use normalized comparison to handle format differences (e.g., "12" vs "12.0")
    const orderMap = new Map(customOrder.map((val, idx) => [normalizeValue(val), idx]));
    return originalData
      .filter((item) => orderMap.has(normalizeValue(item.grouped_value)))
      .sort((a, b) => {
        const aIdx = orderMap.get(normalizeValue(a.grouped_value)) ?? Infinity;
        const bIdx = orderMap.get(normalizeValue(b.grouped_value)) ?? Infinity;
        return aIdx - bIdx;
      });
  }, [originalData, config?.setup?.custom_order]);

  const data = orderedData;
  const pieLayout = config?.options?.layout || pieLayoutTypes.Values.center_active;
  const isCenterActiveLayout = pieLayout === pieLayoutTypes.Values.center_active;
  const isAllLabelsOutsideLayout = pieLayout === pieLayoutTypes.Values.all_labels_outside;
  const isLegendLayout = pieLayout === pieLayoutTypes.Values.legend;
  const chartMargin = {
    top: 8,
    right: isAllLabelsOutsideLayout ? 24 : 8,
    left: isAllLabelsOutsideLayout ? 24 : 8,
    bottom: 12,
  };

  const chartAspect = isAllLabelsOutsideLayout ? 1.35 : 1.2;

  const displayData = useMemo(() => {
    return data.length > 0 ? data : [{ grouped_value: t("no_data"), operation_value: 1 }];
  }, [data, t]);

  const [activeIndex, setActiveIndex] = useState(0);
  const [isHovering, setIsHovering] = useState(false);

  const totalOperationValue = useMemo(
    () => displayData.reduce((sum, item) => sum + item.operation_value, 0),
    [displayData]
  );

  const selectedValues = useMemo(() => [] as string[], []); // TODO: Connect to filters

  const calculateDefaultActiveIndex = useCallback(() => {
    if (data.length === 0) return 0;

    // If custom_order is defined, use the first item in the order as default
    const customOrder = config?.setup?.custom_order;
    if (customOrder && customOrder.length > 0) {
      // The data is already sorted by custom_order, so first item (index 0) is the default
      return 0;
    }

    // Fallback: show the item with max value
    const candidates = data.filter((item) =>
      selectedValues.length > 0 ? selectedValues.includes(item.grouped_value) : true
    );

    const validData = candidates.length > 0 ? candidates : data;
    const maxValue = Math.max(...validData.map((item) => item.operation_value));
    return data.findIndex((item) => item.operation_value === maxValue);
  }, [data, selectedValues, config?.setup?.custom_order]);

  // Build color lookup from color_map if available (normalized for format differences)
  const colorMapLookup = useMemo(() => {
    const lookup = new Map<string, string>();
    config?.options?.color_map?.forEach(([value, color]) => {
      lookup.set(normalizeValue(value), color);
    });
    return lookup;
  }, [config?.options?.color_map]);

  const labelMapLookup = useMemo(() => {
    const lookup = new Map<string, string>();
    config?.options?.label_map?.forEach(([value, label]) => {
      lookup.set(normalizeValue(value), label);
    });
    return lookup;
  }, [config?.options?.label_map]);

  const getDisplayLabel = useCallback(
    (groupedValue: unknown) => {
      const fallback = String(groupedValue ?? t("no_data", { defaultValue: "No data" }));
      const mapped = labelMapLookup.get(normalizeValue(fallback));
      return mapped ? String(mapped) : fallback;
    },
    [labelMapLookup, t]
  );

  const truncateLabel = useCallback((value: unknown, maxLength = 16) => {
    const safeValue = String(value ?? "");
    return safeValue.length > maxLength ? `${safeValue.slice(0, maxLength - 1)}…` : safeValue;
  }, []);

  const configuredPalette = useMemo(() => {
    return (config?.options?.color_range as ColorRange | undefined)?.colors;
  }, [config?.options?.color_range]);

  const baseColors = useMemo(() => {
    if (displayData.length === 0) return [];

    // If we have a color_map, use it for colors
    if (colorMapLookup.size > 0) {
      return displayData.map((item, index) => {
        const mappedColor = colorMapLookup.get(normalizeValue(item.grouped_value));
        if (mappedColor) return mappedColor;
        // Fallback for items not in color_map
        const palette = configuredPalette || FALLBACK_COLORS;
        const colors = chroma.scale(palette).mode("lch").colors(displayData.length);
        return colors[index];
      });
    }

    // Default behavior: generate from color_range
    const palette = data.length > 0 ? configuredPalette || FALLBACK_COLORS : ["#e0e0e0"];

    return displayData.length === 1
      ? [palette[0]]
      : chroma.scale(palette).mode("lch").colors(displayData.length);
  }, [displayData, data.length, configuredPalette, colorMapLookup]);

  const computedColors = useMemo(() => {
    return displayData.map((item, index) => {
      const baseColor = baseColors[index % baseColors.length];
      const isSelected = selectedValues.includes(item.grouped_value);

      if (data.length === 0) return baseColor; // Keep full color for "No data" state

      return isHovering || selectedValues.length > 0
        ? isSelected
          ? baseColor
          : `${baseColor}${OPACITY_MODIFIER}`
        : baseColor;
    });
  }, [displayData, baseColors, selectedValues, data.length, isHovering]);

  const piePaddingAngle = useMemo(() => {
    if (data.length === 0) return 0;
    if (data.length <= 5) return 3;
    if (data.length <= 10) return 2;
    if (data.length <= 20) return 1;
    return 0;
  }, [data.length]);

  const legendItems = useMemo(() => {
    if (!isLegendLayout || totalOperationValue <= 0) return [] as Array<{ label: string; percent: string; color: string }>;

    return displayData.map((item, index) => ({
      label: getDisplayLabel(item.grouped_value),
      percent: formatNumber(item.operation_value / totalOperationValue, "percent_1d", i18n.language),
      color: baseColors[index % baseColors.length] || "#999",
    }));
  }, [baseColors, displayData, getDisplayLabel, i18n.language, isLegendLayout, totalOperationValue]);

  const renderLegendPercentOutsideLabel = useCallback(
    ({
      x,
      y,
      textAnchor,
      percent,
    }: {
      x?: number;
      y?: number;
      textAnchor?: "start" | "middle" | "end";
      percent?: number;
    }) => {
      return (
        <text
          x={x}
          y={y}
          textAnchor={textAnchor}
          dominantBaseline="central"
          fontSize={12}
          fontWeight={500}
          fill={theme.palette.text.primary}>
          {formatNumber(percent || 0, "percent_1d", i18n.language)}
        </text>
      );
    },
    [i18n.language, theme.palette.text.primary]
  );

  useEffect(() => {
    const newIndex = calculateDefaultActiveIndex();
    setActiveIndex(Math.max(newIndex, 0));
  }, [calculateDefaultActiveIndex]);

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const handlePieEnter = (_: any, index: number) => {
    setActiveIndex(index);
  };

  const handleChartHover = (isEnter: boolean) => {
    setIsHovering(isEnter);
    if (!isEnter) {
      const newIndex = calculateDefaultActiveIndex();
      setActiveIndex(Math.max(newIndex, 0));
    }
  };

  const isChartConfigured = useMemo(() => {
    return config?.setup?.layer_project_id && queryParams;
  }, [config, queryParams]);

  return (
    <>
      <WidgetStatusContainer
        isLoading={isLoading && !aggregationStats && !isError}
        isNotConfigured={!isChartConfigured}
        isError={isError}
        height={150}
        isNotConfiguredMessage={t("please_configure_chart")}
        errorMessage={t("cannot_render_chart_error")}
      />

      {config && !isError && aggregationStats && isChartConfigured && (
        <Box sx={{ position: "relative", width: "100%" }}>
          <ResponsiveContainer width="100%" aspect={chartAspect}>
            <PieChart
              margin={chartMargin}
              onMouseEnter={() => handleChartHover(true)}
              onMouseLeave={() => handleChartHover(false)}>
              <Pie
                activeIndex={activeIndex}
                activeShape={{
                  fill: baseColors[activeIndex % baseColors.length],
                  strokeWidth: 0,
                }}
                data={displayData}
                dataKey="operation_value"
                nameKey="grouped_value"
                cx="50%"
                cy={isAllLabelsOutsideLayout ? "46%" : "50%"}
                innerRadius={isAllLabelsOutsideLayout ? "50%" : "65%"}
                outerRadius={isAllLabelsOutsideLayout ? "68%" : undefined}
                cursor="pointer"
                isAnimationActive={false}
                paddingAngle={piePaddingAngle}
                onMouseEnter={handlePieEnter}
                labelLine={(isAllLabelsOutsideLayout || isLegendLayout) && data.length > 0}
                label={
                  isAllLabelsOutsideLayout && data.length > 0
                    ? ({
                        x,
                        y,
                        textAnchor,
                        percent,
                        payload,
                      }: {
                        x?: number;
                        y?: number;
                        textAnchor?: "start" | "middle" | "end";
                        percent?: number;
                        payload?: { grouped_value?: string };
                      }) => {
                        const groupedValue = payload?.grouped_value || "";
                        const label = getDisplayLabel(groupedValue);
                        const shortLabel = truncateLabel(label, 22);
                        const percentLabel = formatNumber(percent || 0, "percent_1d", i18n.language);
                        return (
                          <text
                            x={x}
                            y={y}
                            textAnchor={textAnchor}
                            dominantBaseline="central"
                            fontSize={11}
                            fill={theme.palette.text.primary}>
                            <tspan x={x} dy="0">{shortLabel}</tspan>
                            <tspan x={x} dy="1.1em">{percentLabel}</tspan>
                          </text>
                        );
                      }
                    : isLegendLayout && data.length > 0
                      ? renderLegendPercentOutsideLabel
                      : false
                }>
                {displayData.map((_, index) => (
                  <Cell key={`cell-${index}`} fill={computedColors[index]} stroke="none" />
                ))}

                {isCenterActiveLayout && (
                  <>
                    <Label
                      content={(props: Record<string, unknown>) => {
                        const vb = props.viewBox as { cx?: number; cy?: number } | undefined;
                        const cx = vb?.cx ?? 0;
                        const cy = vb?.cy ?? 0;
                        const activeColor = baseColors[activeIndex % baseColors.length];
                        const percentText = formatNumber(
                          displayData[activeIndex].operation_value / totalOperationValue,
                          "percent_1d",
                          i18n.language
                        );
                        const label = getDisplayLabel(displayData[activeIndex].grouped_value);
                        const maxChars = 12;
                        const words = label.split(/\s+/);
                        const lines: string[] = [];
                        let currentLine = "";
                        for (const word of words) {
                          if (!currentLine) {
                            currentLine = word;
                          } else if ((currentLine + " " + word).length <= maxChars) {
                            currentLine += " " + word;
                          } else {
                            lines.push(currentLine);
                            currentLine = word;
                          }
                        }
                        if (currentLine) lines.push(currentLine);
                        const labelLineHeight = 14;
                        const percentSize = 16;
                        const gap = 4;
                        const totalLabelHeight = lines.length * labelLineHeight;
                        const totalHeight = percentSize + gap + totalLabelHeight;
                        const startY = cy - totalHeight / 2 + percentSize / 2;
                        return (
                          <g>
                            <text
                              x={cx}
                              y={startY}
                              textAnchor="middle"
                              fontSize={percentSize}
                              fontWeight="bold"
                              fill={activeColor}>
                              {percentText}
                            </text>
                            <text
                              x={cx}
                              y={startY + percentSize / 2 + gap}
                              textAnchor="middle"
                              dominantBaseline="hanging"
                              fontSize={11}
                              fontWeight="bold"
                              fill={activeColor}>
                              {lines.map((line, i) => (
                                <tspan key={i} x={cx} dy={i === 0 ? 0 : labelLineHeight}>
                                  {line}
                                </tspan>
                              ))}
                            </text>
                          </g>
                        );
                      }}
                    />
                  </>
                )}
              </Pie>
            </PieChart>
          </ResponsiveContainer>
          {isLegendLayout && legendItems.length > 0 && (
            <Box
              sx={{
                mt: 1,
                display: "flex",
                flexWrap: "wrap",
                gap: 1,
                justifyContent: "center",
              }}>
              {legendItems.map((item, index) => (
                <Box
                  key={`${item.label}-${index}`}
                  sx={{
                    display: "flex",
                    alignItems: "center",
                    gap: 0.75,
                    minWidth: 0,
                    maxWidth: 220,
                  }}>
                  <Box
                    sx={{
                      width: 10,
                      height: 10,
                      borderRadius: "50%",
                      backgroundColor: item.color,
                      flexShrink: 0,
                    }}
                  />
                  <Typography variant="caption" sx={{ minWidth: 0, color: "text.primary" }} title={item.label}>
                    {truncateLabel(item.label, 22)}
                  </Typography>
                </Box>
              ))}
            </Box>
          )}
        </Box>
      )}
      <StaleDataLoader isLoading={isLoading} hasData={!!aggregationStats} />
    </>
  );
};
