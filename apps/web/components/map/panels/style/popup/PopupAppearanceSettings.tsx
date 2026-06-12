import { InputAdornment, Stack, TextField } from "@mui/material";
import { useMemo } from "react";
import { useTranslation } from "react-i18next";

import type { PopupProperties } from "@/lib/validations/layer";

import type { SelectorItem } from "@/types/map/common";

import FormLabelHelper from "@/components/common/FormLabelHelper";
import Selector from "@/components/map/panels/common/Selector";

interface Props {
  popup: PopupProperties;
  onChange: (patch: Partial<PopupProperties>) => void;
}

// Phase 1 renders only popup + pinned. modal/sidebar are valid in the schema
// but hidden here until their render slots exist.
const ENABLED_LAYOUTS = ["popup", "pinned"] as const;

// Parse a numeric text input to a positive integer, or undefined (= auto).
// Mirrors the schema constraint z.number().int().positive().
function parsePositiveInt(raw: string): number | undefined {
  const n = parseInt(raw, 10);
  return Number.isFinite(n) && n > 0 ? n : undefined;
}

export function PopupAppearanceSettings({ popup, onChange }: Props) {
  const { t } = useTranslation("common");

  const layoutItems: SelectorItem[] = useMemo(
    () => ENABLED_LAYOUTS.map((value) => ({ value, label: t(`popup_layout_${value}`) })),
    [t],
  );
  const selectedLayout = useMemo(
    () => layoutItems.find((i) => i.value === popup.layout),
    [layoutItems, popup.layout],
  );

  const anchorItems: SelectorItem[] = useMemo(
    () => [
      { value: "top_left", label: t("anchor_top_left") },
      { value: "top_right", label: t("anchor_top_right") },
      { value: "bottom_left", label: t("anchor_bottom_left") },
      { value: "bottom_right", label: t("anchor_bottom_right") },
    ],
    [t],
  );
  const selectedAnchor = useMemo(
    () => anchorItems.find((i) => i.value === (popup.anchor ?? "top_right")),
    [anchorItems, popup.anchor],
  );

  const headerItems: SelectorItem[] = useMemo(
    () => [
      { value: "standard", label: t("popup_header_standard") },
      { value: "compact", label: t("popup_header_compact") },
      { value: "none", label: t("popup_header_none") },
    ],
    [t],
  );
  const selectedHeader = useMemo(
    () => headerItems.find((i) => i.value === popup.header),
    [headerItems, popup.header],
  );

  const pxAdornment = (
    <InputAdornment position="end">
      <span style={{ fontSize: 12, opacity: 0.7 }}>px</span>
    </InputAdornment>
  );

  return (
    <Stack spacing={3}>
      <Selector
        label={t("popup_layout")}
        items={layoutItems}
        selectedItems={selectedLayout}
        setSelectedItems={(item) => {
          if (item && !Array.isArray(item)) {
            onChange({ layout: item.value as PopupProperties["layout"] });
          }
        }}
      />

      {popup.layout === "pinned" && (
        <Selector
          label={t("anchor")}
          items={anchorItems}
          selectedItems={selectedAnchor}
          setSelectedItems={(item) => {
            if (item && !Array.isArray(item)) {
              onChange({ anchor: item.value as PopupProperties["anchor"] });
            }
          }}
        />
      )}

      <Stack spacing={1}>
        <FormLabelHelper label={t("popup_width")} color="inherit" />
        <TextField
          size="small"
          fullWidth
          type="number"
          placeholder={t("auto")}
          value={popup.width ?? ""}
          onChange={(e) => onChange({ width: parsePositiveInt(e.target.value) })}
          inputProps={{ "aria-label": t("popup_width"), min: 1 }}
          InputProps={{ endAdornment: pxAdornment }}
        />
      </Stack>

      <Stack spacing={1}>
        <FormLabelHelper label={t("popup_max_height")} color="inherit" />
        <TextField
          size="small"
          fullWidth
          type="number"
          placeholder={t("auto")}
          value={popup.max_height ?? ""}
          onChange={(e) => onChange({ max_height: parsePositiveInt(e.target.value) })}
          inputProps={{ "aria-label": t("popup_max_height"), min: 1 }}
          InputProps={{ endAdornment: pxAdornment }}
        />
      </Stack>

      <Selector
        label={t("popup_header_label")}
        items={headerItems}
        selectedItems={selectedHeader}
        setSelectedItems={(item) => {
          if (item && !Array.isArray(item)) {
            onChange({ header: item.value as PopupProperties["header"] });
          }
        }}
      />
    </Stack>
  );
}
