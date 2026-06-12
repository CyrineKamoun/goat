import { Stack } from "@mui/material";
import { useEffect, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";

import {
  clusterSchema,
  type ColorRange,
  type FeatureLayerPointProperties,
  type FeatureLayerProperties,
} from "@/lib/validations/layer";

import type { RGBAColor, RGBColor } from "@/types/map/color";

import FormLabelHelper from "@/components/common/FormLabelHelper";
import SectionHeader from "@/components/map/panels/common/SectionHeader";
import SectionOptions from "@/components/map/panels/common/SectionOptions";
import SliderInput from "@/components/map/panels/common/SliderInput";
import TextFieldInput from "@/components/map/panels/common/TextFieldInput";
import ColorSelector from "@/components/map/panels/style/color/ColorSelector";

interface ClusteringSectionProps {
  layerProperties: FeatureLayerProperties;
  onStyleChange: (newStyle: FeatureLayerProperties) => void;
}

const DEFAULTS = clusterSchema.parse({});

const toRgb = (arr: number[]): RGBColor => [arr[0], arr[1], arr[2]];

const ClusteringSection = ({ layerProperties, onStyleChange }: ClusteringSectionProps) => {
  const { t } = useTranslation("common");
  const [collapsed, setCollapsed] = useState(true);

  const pointProps = layerProperties as FeatureLayerPointProperties;
  const cluster = { ...DEFAULTS, ...(pointProps.cluster ?? {}) };
  const active = !!pointProps.cluster?.enabled;

  // Local slider state for live preview during drag — committing on every
  // tick would re-key the cluster Source and trigger a GeoJSON refetch +
  // supercluster reindex. We persist only on commit (mouse up).
  const [localRadius, setLocalRadius] = useState(cluster.radius);
  const [localMaxZoom, setLocalMaxZoom] = useState(cluster.max_zoom);
  useEffect(() => setLocalRadius(cluster.radius), [cluster.radius]);
  useEffect(() => setLocalMaxZoom(cluster.max_zoom), [cluster.max_zoom]);

  const update = (patch: Partial<typeof cluster>) => {
    const next: FeatureLayerProperties = JSON.parse(JSON.stringify(layerProperties));
    (next as FeatureLayerPointProperties).cluster = { ...cluster, ...patch };
    onStyleChange(next);
  };

  // ColorSet closures must capture the LATEST layerProperties / cluster /
  // onStyleChange so they don't serialise stale prop values back into the
  // layer when the user toggles something (e.g., custom_marker) and then
  // picks a color. Depending on layerProperties (the only prop that drives
  // both `cluster` and `update`) re-creates the closure on every relevant
  // change.
  const clusterColorSet = useMemo(
    () => ({
      selectedColor: toRgb(cluster.color),
      isRange: false,
      setColor: (color: RGBColor | RGBAColor | ColorRange) => {
        if (Array.isArray(color) && typeof color[0] === "number") {
          update({ color: [color[0] as number, color[1] as number, color[2] as number] });
        }
      },
    }),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [layerProperties],
  );

  const textColorSet = useMemo(
    () => ({
      selectedColor: toRgb(cluster.text_color),
      isRange: false,
      setColor: (color: RGBColor | RGBAColor | ColorRange) => {
        if (Array.isArray(color) && typeof color[0] === "number") {
          update({ text_color: [color[0] as number, color[1] as number, color[2] as number] });
        }
      },
    }),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [layerProperties],
  );

  return (
    <>
      <SectionHeader
        active={active}
        label={t("clustering")}
        onToggleChange={(e) => {
          const enabling = e.target.checked;
          // Seed cluster.color from the layer's fill color on first enable
          // so the cluster bubble/badge picks up the layer's palette by
          // default. Once `pointProps.cluster` exists the user owns the
          // value via the color picker — we don't overwrite on later toggles.
          const isFirstEnable = enabling && !pointProps.cluster;
          if (isFirstEnable) {
            const fill = pointProps.color;
            const seedColor =
              Array.isArray(fill) && fill.length >= 3 && typeof fill[0] === "number"
                ? ([fill[0], fill[1], fill[2]] as [number, number, number])
                : undefined;
            update({ enabled: true, ...(seedColor ? { color: seedColor } : {}) });
          } else {
            update({ enabled: enabling });
          }
        }}
        collapsed={collapsed}
        setCollapsed={setCollapsed}
      />
      <SectionOptions
        active={active}
        collapsed={collapsed}
        baseOptions={
          <Stack spacing={3}>
            <Stack>
              <FormLabelHelper label={t("cluster_radius")} color="inherit" />
              <SliderInput
                value={localRadius}
                isRange={false}
                min={1}
                max={100}
                step={1}
                onChange={(v) => setLocalRadius(v as number)}
                onChangeCommitted={(v) => update({ radius: v as number })}
              />
            </Stack>
          </Stack>
        }
        advancedOptions={
          <Stack spacing={3}>
            <Stack>
              <FormLabelHelper label={t("min_cluster_size")} color="inherit" />
              <TextFieldInput
                type="number"
                value={String(cluster.min_points)}
                onChange={(v) => {
                  const n = Number(v);
                  if (Number.isFinite(n) && n >= 2 && n <= 20) update({ min_points: n });
                }}
                clearable={false}
              />
            </Stack>
            <Stack>
              <FormLabelHelper label={t("max_zoom_to_cluster")} color="inherit" />
              <SliderInput
                value={localMaxZoom}
                isRange={false}
                min={0}
                max={20}
                step={1}
                onChange={(v) => setLocalMaxZoom(v as number)}
                onChangeCommitted={(v) => update({ max_zoom: v as number })}
              />
            </Stack>
            <ColorSelector colorSet={clusterColorSet} label={t("cluster_color")} />
            <ColorSelector colorSet={textColorSet} label={t("text_color")} />
          </Stack>
        }
      />
    </>
  );
};

export default ClusteringSection;
