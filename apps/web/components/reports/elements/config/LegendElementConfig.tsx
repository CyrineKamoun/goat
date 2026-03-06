"use client";

import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import { Checkbox, Collapse, FormControlLabel, Stack, TextField, Typography, useTheme } from "@mui/material";
import React, { useMemo, useState } from "react";
import { useTranslation } from "react-i18next";

import { ICON_NAME } from "@p4b/ui/components/Icon";

import type { TypographyStyle } from "@/lib/constants/typography";
import type { ReportElement } from "@/lib/validations/reportLayout";

import type { SelectorItem } from "@/types/map/common";

import FormLabelHelper from "@/components/common/FormLabelHelper";
import SectionHeader from "@/components/map/panels/common/SectionHeader";
import SectionOptions from "@/components/map/panels/common/SectionOptions";
import Selector from "@/components/map/panels/common/Selector";
import TypographyStyleControl from "@/components/reports/elements/config/TypographyStyleControl";

/**
 * Legend typography configuration (per text role)
 */
export interface LegendTypographyConfig {
  title?: TypographyStyle;
  layerName?: TypographyStyle;
  legendItem?: TypographyStyle;
  caption?: TypographyStyle;
}

/**
 * Legend element configuration interface (matches LegendElementRenderer)
 */
interface LegendElementConfig {
  /** Title configuration */
  title?: {
    text?: string;
  };
  /** Map element ID to bind to (null = show all layers) */
  mapElementId?: string | null;
  /** Auto-update legend from connected map (default true) */
  auto_update?: boolean;
  /** Layout options */
  layout?: {
    columns?: number;
    showBackground?: boolean;
  };
  /** Typography settings for different text roles */
  typography?: LegendTypographyConfig;
  /** Text overrides when auto_update is off */
  textOverrides?: Record<string, string>;
}

interface LegendElementConfigProps {
  element: ReportElement;
  mapElements?: ReportElement[];
  onChange: (updates: Partial<ReportElement>) => void;
}

/**
 * Legend Element Configuration Panel
 *
 * Uses consistent UI patterns with SectionHeader and SectionOptions.
 *
 * Structure:
 * - Info: Title text
 * - Configuration: Columns, map connection
 */
const LegendElementConfig: React.FC<LegendElementConfigProps> = ({ element, mapElements = [], onChange }) => {
  const { t } = useTranslation("common");
  const theme = useTheme();

  // Section collapsed states
  const [infoCollapsed, setInfoCollapsed] = useState(false);
  const [dataCollapsed, setDataCollapsed] = useState(false);
  const [optionsCollapsed, setOptionsCollapsed] = useState(false);
  const [typographyCollapsed, setTypographyCollapsed] = useState(false);

  // Typography sub-group collapsed states
  const [titleTypoOpen, setTitleTypoOpen] = useState(false);
  const [layerNameTypoOpen, setLayerNameTypoOpen] = useState(false);
  const [legendItemTypoOpen, setLegendItemTypoOpen] = useState(false);
  const [captionTypoOpen, setCaptionTypoOpen] = useState(false);

  // Extract current config
  const config = (element.config || {}) as LegendElementConfig;
  const titleText = config.title?.text ?? t("legend");
  const layoutConfig = config.layout ?? { columns: 1, showBackground: true };
  const mapElementId = config.mapElementId ?? "";
  const autoUpdate = config.auto_update !== false; // Default true

  // Create map selector items
  const mapSelectorItems: SelectorItem[] = useMemo(() => {
    const maps = mapElements.filter((el) => el.type === "map");
    return [
      { label: t("all_layers"), value: "" },
      ...maps.map((mapEl, index) => ({
        label: `${t("map")} ${index + 1}`,
        value: mapEl.id,
      })),
    ];
  }, [mapElements, t]);

  const selectedMapItem = useMemo(
    () => mapSelectorItems.find((item) => item.value === mapElementId),
    [mapSelectorItems, mapElementId]
  );

  // Create columns selector items
  const columnsSelectorItems: SelectorItem[] = useMemo(
    () =>
      [1, 2, 3, 4, 5].map((num) => ({
        label: `${num} ${num === 1 ? t("column") : t("columns")}`,
        value: num,
      })),
    [t]
  );

  const selectedColumnsItem = useMemo(
    () => columnsSelectorItems.find((item) => item.value === (layoutConfig.columns || 1)),
    [columnsSelectorItems, layoutConfig.columns]
  );

  // Handle config updates
  const updateConfig = (updates: Partial<LegendElementConfig>) => {
    onChange({
      config: {
        ...config,
        ...updates,
      },
    });
  };

  // Handle title change
  const handleTitleChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    updateConfig({
      title: { text: event.target.value },
    });
  };

  // Handle map selection
  const handleMapChange = (item: SelectorItem | SelectorItem[] | undefined) => {
    if (!item || Array.isArray(item)) return;
    updateConfig({ mapElementId: item.value ? String(item.value) : null });
  };

  // Handle auto-update toggle
  const handleAutoUpdateChange = (_event: React.ChangeEvent<HTMLInputElement>, checked: boolean) => {
    if (checked) {
      // Clear text overrides when re-enabling auto-update
      updateConfig({ auto_update: true, textOverrides: undefined });
    } else {
      updateConfig({ auto_update: false });
    }
  };

  // Handle columns selection
  const handleColumnsChange = (item: SelectorItem | SelectorItem[] | undefined) => {
    if (!item || Array.isArray(item)) return;
    updateConfig({
      layout: { ...layoutConfig, columns: item.value as number },
    });
  };

  // Handle typography updates
  const typographyConfig = config.typography ?? {};
  const handleTypographyChange = (role: keyof LegendTypographyConfig, style: TypographyStyle) => {
    updateConfig({
      typography: {
        ...typographyConfig,
        [role]: style,
      },
    });
  };

  return (
    <Stack spacing={2}>
      {/* Info Section */}
      <SectionHeader
        label={t("info")}
        icon={ICON_NAME.CIRCLEINFO}
        active={true}
        alwaysActive
        collapsed={infoCollapsed}
        setCollapsed={setInfoCollapsed}
        disableAdvanceOptions
      />
      <SectionOptions
        active={true}
        collapsed={infoCollapsed}
        baseOptions={
          <Stack spacing={3}>
            <Stack>
              <FormLabelHelper label={t("title_text")} color={theme.palette.text.secondary} />
              <TextField
                size="small"
                value={titleText}
                onChange={handleTitleChange}
                placeholder={t("legend")}
                fullWidth
              />
            </Stack>
          </Stack>
        }
      />

      {/* Data Section */}
      <SectionHeader
        label={t("data")}
        icon={ICON_NAME.LAYERS}
        active={true}
        alwaysActive
        collapsed={dataCollapsed}
        setCollapsed={setDataCollapsed}
        disableAdvanceOptions
      />
      <SectionOptions
        active={true}
        collapsed={dataCollapsed}
        baseOptions={
          <Stack spacing={3}>
            {/* Connected Map */}
            <Selector
              selectedItems={selectedMapItem}
              setSelectedItems={handleMapChange}
              items={mapSelectorItems}
              label={t("connected_map")}
              tooltip={t("legend_map_connection_help")}
            />
            {/* Auto Update */}
            <FormControlLabel
              control={
                <Checkbox
                  size="small"
                  checked={autoUpdate}
                  onChange={handleAutoUpdateChange}
                />
              }
              label={
                <Typography variant="body2">
                  {t("auto_update")}
                </Typography>
              }
            />
          </Stack>
        }
      />

      {/* Options Section */}
      <SectionHeader
        label={t("options")}
        icon={ICON_NAME.SLIDERS}
        active={true}
        alwaysActive
        collapsed={optionsCollapsed}
        setCollapsed={setOptionsCollapsed}
        disableAdvanceOptions
      />
      <SectionOptions
        active={true}
        collapsed={optionsCollapsed}
        baseOptions={
          <Stack spacing={3}>
            {/* Columns */}
            <Selector
              selectedItems={selectedColumnsItem}
              setSelectedItems={handleColumnsChange}
              items={columnsSelectorItems}
              label={t("columns")}
            />
          </Stack>
        }
      />

      {/* Typography Section */}
      <SectionHeader
        label={t("typography")}
        icon={ICON_NAME.TEXT}
        active={true}
        alwaysActive
        collapsed={typographyCollapsed}
        setCollapsed={setTypographyCollapsed}
        disableAdvanceOptions
      />
      <SectionOptions
        active={true}
        collapsed={typographyCollapsed}
        baseOptions={
          <Stack spacing={1}>
            {/* Title Typography */}
            <TypographySubGroup
              label={t("title_typography")}
              open={titleTypoOpen}
              onToggle={() => setTitleTypoOpen(!titleTypoOpen)}>
              <TypographyStyleControl
                value={typographyConfig.title ?? {}}
                onChange={(style) => handleTypographyChange("title", style)}
              />
            </TypographySubGroup>

            {/* Layer Name Typography */}
            <TypographySubGroup
              label={t("layer_name_typography")}
              open={layerNameTypoOpen}
              onToggle={() => setLayerNameTypoOpen(!layerNameTypoOpen)}>
              <TypographyStyleControl
                value={typographyConfig.layerName ?? {}}
                onChange={(style) => handleTypographyChange("layerName", style)}
              />
            </TypographySubGroup>

            {/* Legend Item Typography */}
            <TypographySubGroup
              label={t("legend_item_typography")}
              open={legendItemTypoOpen}
              onToggle={() => setLegendItemTypoOpen(!legendItemTypoOpen)}>
              <TypographyStyleControl
                value={typographyConfig.legendItem ?? {}}
                onChange={(style) => handleTypographyChange("legendItem", style)}
              />
            </TypographySubGroup>

            {/* Caption Typography */}
            <TypographySubGroup
              label={t("caption_typography")}
              open={captionTypoOpen}
              onToggle={() => setCaptionTypoOpen(!captionTypoOpen)}>
              <TypographyStyleControl
                value={typographyConfig.caption ?? {}}
                onChange={(style) => handleTypographyChange("caption", style)}
              />
            </TypographySubGroup>
          </Stack>
        }
      />
    </Stack>
  );
};

/**
 * Collapsible sub-group for a typography text role
 */
const TypographySubGroup: React.FC<{
  label: string;
  open: boolean;
  onToggle: () => void;
  children: React.ReactNode;
}> = ({ label, open, onToggle, children }) => {
  const theme = useTheme();
  return (
    <Stack spacing={0}>
      <Stack
        direction="row"
        alignItems="center"
        justifyContent="space-between"
        onClick={onToggle}
        sx={{
          cursor: "pointer",
          py: 1,
          px: 0.5,
          borderRadius: 1,
          "&:hover": { backgroundColor: theme.palette.action.hover },
        }}>
        <Typography variant="body2" fontWeight={500}>
          {label}
        </Typography>
        <ExpandMoreIcon
          fontSize="small"
          sx={{
            transform: open ? "rotate(180deg)" : "rotate(0deg)",
            transition: "transform 0.2s",
          }}
        />
      </Stack>
      <Collapse in={open}>
        <Stack sx={{ pl: 1, pt: 1, pb: 2 }}>{children}</Stack>
      </Collapse>
    </Stack>
  );
};

export default LegendElementConfig;
