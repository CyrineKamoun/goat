import {
  Checkbox,
  FormControlLabel,
  MenuItem,
  Select,
  Stack,
  Typography,
  useTheme,
} from "@mui/material";
import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";

import type { FeatureLayerLineProperties, FeatureLayerProperties } from "@/lib/validations/layer";

import SectionHeader from "@/components/map/panels/common/SectionHeader";
import SectionOptions from "@/components/map/panels/common/SectionOptions";
import SliderInput from "@/components/map/panels/common/SliderInput";

type StrokePattern = "solid" | "dashed" | "dotted" | "dash_dot";
type StrokeDashDensity = "tight" | "normal" | "loose";
type StrokeCap = "butt" | "round" | "square";
type StrokeJoin = "bevel" | "round" | "miter";
type DecorationType = "none" | "arrow";
type DecorationDirection = "forward" | "backward" | "both";
type DecorationPlacement = "repeat" | "start" | "end" | "start_and_end" | "center";
type ArrowDropdownValue = "none" | "forward" | "backward" | "both";

interface LineStyleSectionProps {
  layerProperties: FeatureLayerProperties;
  onStyleChange: (newStyle: FeatureLayerProperties) => void;
}

const LineStyleSection = ({ layerProperties, onStyleChange }: LineStyleSectionProps) => {
  const { t } = useTranslation("common");
  const theme = useTheme();
  // `collapsed` controls the advanced (cap/join) disclosure only — the section's
  // base options (pattern, arrows, etc.) are always visible while the layer is active.
  const [collapsed, setCollapsed] = useState(true);

  // Section is rendered only for line layers (gated by LayerStyle.tsx), so the cast is safe.
  // `??` fallbacks protect legacy layers persisted before these schema fields existed.
  const lineProps = layerProperties as FeatureLayerLineProperties;
  const pattern = (lineProps.stroke_pattern ?? "solid") as StrokePattern;
  const density = (lineProps.stroke_dash_density ?? "normal") as StrokeDashDensity;
  const decorationType = (lineProps.decoration_type ?? "none") as DecorationType;
  const decorationDirection = (lineProps.decoration_direction ?? "forward") as DecorationDirection;
  const decorationPlacement = (lineProps.decoration_placement ?? "repeat") as DecorationPlacement;
  const decorationSize = (lineProps.decoration_size ?? 32) as number;
  const decorationSpacing = (lineProps.decoration_spacing ?? 200) as number;
  const decorationAllowOverlap = (lineProps.decoration_allow_overlap ?? true) as boolean;
  const cap = (lineProps.stroke_cap ?? "butt") as StrokeCap;
  const join = (lineProps.stroke_join ?? "miter") as StrokeJoin;
  const offset = (lineProps.stroke_offset ?? 0) as number;

  const arrowDropdown: ArrowDropdownValue =
    decorationType === "none" ? "none" : decorationDirection;

  // Local slider state — updated continuously during drag (so the thumb tracks the
  // pointer smoothly), then committed to the layer style on drag-end. Re-syncs
  // from props when the active layer or its persisted value changes.
  const [sizeDraft, setSizeDraft] = useState<number>(decorationSize);
  const [spacingDraft, setSpacingDraft] = useState<number>(decorationSpacing);
  const [offsetDraft, setOffsetDraft] = useState<number>(offset);

  useEffect(() => {
    setSizeDraft(decorationSize);
  }, [decorationSize]);

  useEffect(() => {
    setSpacingDraft(decorationSpacing);
  }, [decorationSpacing]);

  useEffect(() => {
    setOffsetDraft(offset);
  }, [offset]);

  const patch = (partial: Partial<FeatureLayerProperties>) => {
    onStyleChange({ ...layerProperties, ...partial } as FeatureLayerProperties);
  };

  const onArrowChange = (next: ArrowDropdownValue) => {
    if (next === "none") {
      // Preserve the previous direction so toggling back doesn't reset the user's choice.
      patch({ decoration_type: "none" } as Partial<FeatureLayerProperties>);
    } else {
      patch({
        decoration_type: "arrow",
        decoration_direction: next,
      } as Partial<FeatureLayerProperties>);
    }
  };

  const showDensity = pattern !== "solid";
  const showArrowKnobs = decorationType !== "none";

  const fieldLabel = (label: string) => (
    <Typography variant="caption" color={theme.palette.text.secondary}>
      {label}
    </Typography>
  );

  return (
    <>
      <SectionHeader
        active={true}
        alwaysActive={true}
        label={t("line_style")}
        collapsed={collapsed}
        setCollapsed={setCollapsed}
      />
      <SectionOptions
        active={true}
        collapsed={collapsed}
        baseOptions={
          <Stack direction="column" spacing={4}>
            <Stack spacing={1}>
              {fieldLabel(t("pattern"))}
              <Select
                size="small"
                value={pattern}
                onChange={(e) =>
                  patch({
                    stroke_pattern: e.target.value as StrokePattern,
                  } as Partial<FeatureLayerProperties>)
                }>
                <MenuItem value="solid">{t("solid")}</MenuItem>
                <MenuItem value="dashed">{t("dashed")}</MenuItem>
                <MenuItem value="dotted">{t("dotted")}</MenuItem>
                <MenuItem value="dash_dot">{t("dash_dot")}</MenuItem>
              </Select>
            </Stack>

            {showDensity && (
              <Stack spacing={1}>
                {fieldLabel(t("density"))}
                <Select
                  size="small"
                  value={density}
                  onChange={(e) =>
                    patch({
                      stroke_dash_density: e.target.value as StrokeDashDensity,
                    } as Partial<FeatureLayerProperties>)
                  }>
                  <MenuItem value="tight">{t("tight")}</MenuItem>
                  <MenuItem value="normal">{t("normal")}</MenuItem>
                  <MenuItem value="loose">{t("loose")}</MenuItem>
                </Select>
              </Stack>
            )}

            <Stack spacing={1}>
              {fieldLabel(t("arrows"))}
              <Select
                size="small"
                value={arrowDropdown}
                onChange={(e) => onArrowChange(e.target.value as ArrowDropdownValue)}>
                <MenuItem value="none">{t("none")}</MenuItem>
                <MenuItem value="forward">{t("forward")}</MenuItem>
                <MenuItem value="backward">{t("backward")}</MenuItem>
                <MenuItem value="both">{t("both")}</MenuItem>
              </Select>
            </Stack>

            {showArrowKnobs && (
              <>
                <Stack spacing={1}>
                  {fieldLabel(t("placement"))}
                  <Select
                    size="small"
                    value={decorationPlacement}
                    onChange={(e) =>
                      patch({
                        decoration_placement: e.target.value as DecorationPlacement,
                      } as Partial<FeatureLayerProperties>)
                    }>
                    <MenuItem value="repeat">{t("placement_repeat")}</MenuItem>
                    <MenuItem value="start">{t("placement_start")}</MenuItem>
                    <MenuItem value="end">{t("placement_end")}</MenuItem>
                    <MenuItem value="start_and_end">{t("placement_start_and_end")}</MenuItem>
                    <MenuItem value="center">{t("placement_center")}</MenuItem>
                  </Select>
                </Stack>
                <Stack spacing={1}>
                  {fieldLabel(t("decoration_size"))}
                  <SliderInput
                    value={sizeDraft}
                    isRange={false}
                    min={8}
                    max={64}
                    step={2}
                    onChange={(v) => setSizeDraft(v as number)}
                    onChangeCommitted={(v) =>
                      patch({ decoration_size: v as number } as Partial<FeatureLayerProperties>)
                    }
                  />
                </Stack>
                {decorationPlacement === "repeat" && (
                  <Stack spacing={1}>
                    {fieldLabel(t("decoration_spacing"))}
                    <SliderInput
                      value={spacingDraft}
                      isRange={false}
                      min={50}
                      max={800}
                      step={25}
                      onChange={(v) => setSpacingDraft(v as number)}
                      onChangeCommitted={(v) =>
                        patch({ decoration_spacing: v as number } as Partial<FeatureLayerProperties>)
                      }
                    />
                  </Stack>
                )}
                <FormControlLabel
                  control={
                    <Checkbox
                      size="small"
                      color="primary"
                      checked={decorationAllowOverlap}
                      onChange={(e) =>
                        patch({
                          decoration_allow_overlap: e.target.checked,
                        } as Partial<FeatureLayerProperties>)
                      }
                    />
                  }
                  label={<Typography variant="body2">{t("allow_overlap")}</Typography>}
                />
              </>
            )}
          </Stack>
        }
        advancedOptions={
          <Stack direction="column" spacing={4}>
            <Stack spacing={1}>
              {fieldLabel(t("line_cap"))}
              <Select
                size="small"
                value={cap}
                onChange={(e) =>
                  patch({ stroke_cap: e.target.value as StrokeCap } as Partial<FeatureLayerProperties>)
                }>
                <MenuItem value="butt">{t("butt")}</MenuItem>
                <MenuItem value="round">{t("round")}</MenuItem>
                <MenuItem value="square">{t("square")}</MenuItem>
              </Select>
            </Stack>
            <Stack spacing={1}>
              {fieldLabel(t("line_join"))}
              <Select
                size="small"
                value={join}
                onChange={(e) =>
                  patch({ stroke_join: e.target.value as StrokeJoin } as Partial<FeatureLayerProperties>)
                }>
                <MenuItem value="bevel">{t("bevel")}</MenuItem>
                <MenuItem value="round">{t("round")}</MenuItem>
                <MenuItem value="miter">{t("miter")}</MenuItem>
              </Select>
            </Stack>
            <Stack spacing={1}>
              {fieldLabel(t("line_offset"))}
              <SliderInput
                value={offsetDraft}
                isRange={false}
                min={-50}
                max={50}
                step={1}
                onChange={(v) => setOffsetDraft(v as number)}
                onChangeCommitted={(v) =>
                  patch({ stroke_offset: v as number } as Partial<FeatureLayerProperties>)
                }
              />
              <Typography variant="caption" color={theme.palette.text.secondary}>
                {t("line_offset_helper")}
              </Typography>
            </Stack>
          </Stack>
        }
      />
    </>
  );
};

export default LineStyleSection;
