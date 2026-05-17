import {
  Box,
  Fade,
  IconButton,
  Popper,
  Stack,
  Switch,
  Tooltip,
  Typography,
} from "@mui/material";
import { alpha } from "@mui/material/styles";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";
import type { ReactNode } from "react";
import { useMemo, useState } from "react";
import { useTranslation } from "react-i18next";

import type { FeatureLayerProperties, PopupBlock, PopupProperties } from "@/lib/validations/layer";
import type { ProjectLayer } from "@/lib/validations/project";

import type { SelectorItem } from "@/types/map/common";

import useLayerFields from "@/hooks/map/CommonHooks";

import type { LayerField } from "@/components/map/popover/formatFeatureProperties";
import SectionHeader from "@/components/map/panels/common/SectionHeader";
import SectionOptions from "@/components/map/panels/common/SectionOptions";
import Selector from "@/components/map/panels/common/Selector";

import { HtmlModeEditor } from "./HtmlModeEditor";
import { PopupBlockList } from "./PopupBlockList";
import { PopupPreviewController } from "./PopupPreviewController";
import { BadgeBlockEditor } from "./blocks/BadgeBlockEditor";
import { ButtonBlockEditor } from "./blocks/ButtonBlockEditor";
import { DividerBlockEditor } from "./blocks/DividerBlockEditor";
import { FieldListBlockEditor } from "./blocks/FieldListBlockEditor";
import { ImageBlockEditor } from "./blocks/ImageBlockEditor";
import { TextBlockEditor } from "./blocks/TextBlockEditor";
import { seedPopupFromInteraction } from "./seedFromLegacy";

interface PopupSectionProps {
  layer: ProjectLayer;
  onStyleChange?: (newStyle: FeatureLayerProperties) => void;
}

// i18n key per block type. Used as the title bar label inside the block
// edit popper. Reuses existing keys defined for the "Add block" menu.
const BLOCK_LABEL_KEYS: Record<PopupBlock["type"], string> = {
  text: "text_block",
  fieldList: "field_list",
  image: "image",
  button: "button_block",
  badge: "badge_block",
  divider: "divider_block",
};

export default function PopupSection({ layer, onStyleChange }: PopupSectionProps) {
  const { t } = useTranslation("common");
  const [editing, setEditing] = useState<{
    block: PopupBlock;
    anchorEl: HTMLElement;
  } | null>(null);
  // Off by default so the Popup section can mount (e.g. when the user
  // selects a layer in the tree) without auto-opening a sample-feature
  // popup on the map. User opts in via the "Show preview" toggle when
  // they actually want to iterate on the popup styling.
  const [previewOn, setPreviewOn] = useState(false);

  const { layerFields } = useLayerFields(layer.layer_id);

  const popup = useMemo<PopupProperties>(
    () =>
      (layer.properties as { popup?: PopupProperties })?.popup ??
      seedPopupFromInteraction(
        (layer.properties as {
          interaction?: { type?: string; content?: never[] };
        })?.interaction,
      ),
    [layer.properties],
  );

  const updatePopup = (patch: Partial<PopupProperties>) => {
    const next = JSON.parse(JSON.stringify(layer.properties)) || {};
    next.popup = { ...popup, ...patch };
    onStyleChange?.(next);
  };

  const updateBlock = (next: PopupBlock) => {
    updatePopup({ blocks: popup.blocks.map((b) => (b.id === next.id ? next : b)) });
    setEditing((cur) => (cur ? { ...cur, block: next } : null));
  };

  const triggerItems: SelectorItem[] = useMemo(
    () => [
      { value: "click", label: t("on_click") },
      { value: "hover", label: t("on_hover_only") },
      { value: "click_and_hover", label: t("on_click_and_hover") },
    ],
    [t],
  );

  const selectedTrigger = useMemo(
    () => triggerItems.find((i) => i.value === popup.trigger),
    [triggerItems, popup.trigger],
  );

  const positionItems: SelectorItem[] = useMemo(
    () => [
      { value: "in_place", label: t("position_in_place") },
      { value: "fixed", label: t("position_fixed") },
    ],
    [t],
  );
  const selectedPosition = useMemo(
    () => positionItems.find((i) => i.value === popup.position),
    [positionItems, popup.position],
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
    () => anchorItems.find((i) => i.value === popup.anchor),
    [anchorItems, popup.anchor],
  );

  return (
    <>
      <SectionHeader
        active={popup.enabled}
        onToggleChange={(event) => updatePopup({ enabled: event.target.checked })}
        label={t("popup")}
        disableAdvanceOptions
      />
      <SectionOptions
        active={popup.enabled}
        baseOptions={
          <Stack spacing={2.5}>
            <Selector
              label={t("show_popup_on")}
              items={triggerItems}
              selectedItems={selectedTrigger}
              setSelectedItems={(item) => {
                if (item && !Array.isArray(item)) {
                  updatePopup({ trigger: item.value as "click" | "hover" });
                }
              }}
            />

            {/* Content sub-section */}
            <Stack spacing={1.25}>
              <Stack
                direction="row"
                alignItems="center"
                justifyContent="space-between"
                spacing={1}>
                <SubSectionLabel>{t("content")}</SubSectionLabel>
                <Stack direction="row" spacing={1} alignItems="center">
                  {/* Live-preview is an editor tool, not a popup property —
                      represented as an inline icon toggle next to the
                      section header rather than as a saved-style toggle. */}
                  <Tooltip title={t("show_preview")}>
                    <IconButton
                      size="small"
                      onClick={() => setPreviewOn((v) => !v)}
                      sx={(theme) => ({
                        p: 0.5,
                        color: previewOn
                          ? theme.palette.primary.main
                          : alpha(theme.palette.text.primary, 0.55),
                        "&:hover": { bgcolor: "transparent" },
                      })}>
                      <Icon iconName={ICON_NAME.EYE} style={{ fontSize: 14 }} />
                    </IconButton>
                  </Tooltip>
                  {/* HTML popup mode is gated behind a follow-up. Toggle
                      stays hidden for now so users only see the block
                      editor. Re-enable once HTML mode ships. */}
                </Stack>
              </Stack>
              {popup.mode === "simple" ? (
                <PopupBlockList
                  blocks={popup.blocks}
                  onChange={(blocks) => updatePopup({ blocks })}
                  onEdit={(block, anchorEl) => setEditing({ block, anchorEl })}
                  editingId={editing?.block.id ?? null}
                />
              ) : (
                <HtmlModeEditor
                  value={popup.html}
                  onChange={(html) => updatePopup({ html })}
                  fields={layerFields as LayerField[]}
                />
              )}
              {popup.enabled && previewOn && (
                <PopupPreviewController layerId={layer.layer_id} enabled />
              )}
            </Stack>

            {/* Appearance sub-section */}
            <Stack spacing={1.5}>
              <SubSectionLabel>{t("appearance")}</SubSectionLabel>

              <Selector
                label={t("popup_position")}
                items={positionItems}
                selectedItems={selectedPosition}
                setSelectedItems={(item) => {
                  if (item && !Array.isArray(item)) {
                    updatePopup({ position: item.value as "in_place" | "fixed" });
                  }
                }}
              />

              {popup.position === "fixed" && (
                <Selector
                  label={t("anchor")}
                  items={anchorItems}
                  selectedItems={selectedAnchor}
                  setSelectedItems={(item) => {
                    if (item && !Array.isArray(item)) {
                      updatePopup({ anchor: item.value as typeof popup.anchor });
                    }
                  }}
                />
              )}

              <ToggleRow
                label={t("show_layer_name_header")}
                checked={popup.show_layer_header}
                onChange={(v) => updatePopup({ show_layer_header: v })}
              />

              <ToggleRow
                label={t("highlight_active_feature")}
                checked={popup.highlight_active_feature}
                onChange={(v) => updatePopup({ highlight_active_feature: v })}
              />
            </Stack>
          </Stack>
        }
        collapsed={!popup.enabled}
      />
      <Popper
        open={Boolean(editing)}
        anchorEl={editing?.anchorEl}
        placement="left-start"
        // Matches the offset used by SingleColorPopper /
        // LayerValueSelectorPopper so the block-edit popper sits with
        // the same small gap from the style panel as the rest of the
        // GOAT style-panel poppers.
        modifiers={[{ name: "offset", options: { offset: [0, 75] } }]}
        sx={{ zIndex: (theme) => theme.zIndex.modal }}>
        {editing && (
          <Fade in>
            <Box
              onKeyDown={(e) => {
                if (e.key === "Escape") setEditing(null);
              }}
              sx={{
                width: 380,
                maxHeight: "70vh",
                display: "flex",
                flexDirection: "column",
                bgcolor: "background.paper",
                border: 1,
                borderColor: "divider",
                borderRadius: 1,
                boxShadow: 8,
                overflow: "hidden",
              }}>
              {/* Header — block-type label on the left, small close X
                  on the right. We previously relied on ClickAwayListener
                  for dismissal but it fired on every Portal'd dropdown
                  click (Selector / LayerFieldSelector), which closed the
                  popper any time the user opened a sub-dropdown. Explicit
                  close + Escape is bulletproof. */}
              <Stack
                direction="row"
                alignItems="center"
                justifyContent="space-between"
                sx={{ px: 2, pt: 2, pb: 1, flexShrink: 0 }}>
                <Typography variant="body2" fontWeight="bold">
                  {t(BLOCK_LABEL_KEYS[editing.block.type])}
                </Typography>
                <IconButton
                  size="small"
                  disableRipple
                  onClick={() => setEditing(null)}
                  sx={(theme) => ({
                    p: 0.5,
                    color: alpha(theme.palette.text.primary, 0.55),
                    "&:hover": {
                      bgcolor: "transparent",
                      color: theme.palette.text.primary,
                    },
                  })}>
                  <Icon iconName={ICON_NAME.XCLOSE} style={{ fontSize: 14 }} />
                </IconButton>
              </Stack>
                <Box sx={{ flex: 1, minHeight: 0, overflowY: "auto", pt: 1 }}>
                {editing.block.type === "text" && (
                  <TextBlockEditor
                    block={editing.block}
                    fields={layerFields as LayerField[]}
                    onChange={updateBlock}
                  />
                )}
                {editing.block.type === "fieldList" && (
                  <FieldListBlockEditor
                    block={editing.block}
                    fields={layerFields as LayerField[]}
                    layerId={layer.layer_id}
                    onChange={updateBlock}
                  />
                )}
                {editing.block.type === "image" && (
                  <ImageBlockEditor
                    block={editing.block}
                    fields={layerFields as LayerField[]}
                    onChange={updateBlock}
                  />
                )}
                {editing.block.type === "button" && (
                  <ButtonBlockEditor block={editing.block} onChange={updateBlock} />
                )}
                {editing.block.type === "badge" && (
                  <BadgeBlockEditor
                    block={editing.block}
                    fields={layerFields as LayerField[]}
                    layerId={layer.layer_id}
                    onChange={updateBlock}
                  />
                )}
                {editing.block.type === "divider" && (
                  <DividerBlockEditor block={editing.block} onChange={updateBlock} />
                )}
                </Box>
            </Box>
          </Fade>
        )}
      </Popper>
    </>
  );
}

// Sub-section header inside the Popup section (e.g. "Content",
// "Appearance"). Bolder than a field label so the hierarchy is clear —
// field labels keep the uppercase 10px style, headers are body2 weight
// 700 sentence case.
function SubSectionLabel({ children }: { children: ReactNode }) {
  return (
    <Typography
      variant="body2"
      sx={{ fontWeight: "bold", color: "text.primary" }}>
      {children}
    </Typography>
  );
}

// Compact "label [hint] — small Switch" row. Wraps the MUI Switch with
// `size="small"` (matching NoDataRow.tsx, ColorRangeSelector.tsx, etc.)
// and constrains layout so labels and switches line up consistently.
function ToggleRow({
  label,
  hint,
  checked,
  onChange,
}: {
  label: string;
  hint?: string;
  checked: boolean;
  onChange: (next: boolean) => void;
}) {
  return (
    <Stack direction="row" justifyContent="space-between" alignItems="center" spacing={2}>
      <Stack sx={{ minWidth: 0, flex: 1 }}>
        <Typography variant="body2">{label}</Typography>
        {hint && (
          <Typography variant="caption" color="text.secondary">
            {hint}
          </Typography>
        )}
      </Stack>
      <Switch size="small" checked={checked} onChange={(_, v) => onChange(v)} />
    </Stack>
  );
}
