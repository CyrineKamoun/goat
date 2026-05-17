import {
  Box,
  Button,
  IconButton,
  Paper,
  Stack,
  Switch,
  Typography,
} from "@mui/material";
import { alpha } from "@mui/material/styles";
import { useRef, useState } from "react";
import { useTranslation } from "react-i18next";

import type { LayerFieldType, PopupBadgeBlock } from "@/lib/validations/layer";

import { rgbToHex } from "@/lib/utils/helpers";

import type { RGBColor } from "@/types/map/color";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import { ArrowPopper } from "@/components/ArrowPoper";
import LayerFieldSelector from "@/components/map/common/LayerFieldSelector";
import type { LayerField } from "@/components/map/popover/formatFeatureProperties";
import SingleColorSelector from "@/components/map/panels/style/color/SingleColorSelector";
import { LayerValueSelectorPopper } from "@/components/map/panels/style/other/LayerValueSelectorPopper";

import { MiniLabel, PillToggleGroup } from "./_shared";

interface Props {
  block: PopupBadgeBlock;
  fields: LayerField[];
  layerId: string;
  onChange: (next: PopupBadgeBlock) => void;
}

const DEFAULT_COLOR = "#56CA00";
const FALLBACK_COLOR = "#8A8D93";

export function BadgeBlockEditor({ block, fields, layerId, onChange }: Props) {
  const { t } = useTranslation("common");

  // Categorical badge values come from string/text fields. Anything else
  // (numbers, dates) doesn't fit the "label chip" mental model.
  const badgeFields = fields.filter((f) => f.type === "string") as LayerFieldType[];
  const selectedField = badgeFields.find((f) => f.name === block.field);

  const removePaletteEntry = (key: string) => {
    const next = { ...block.palette };
    delete next[key];
    onChange({ ...block, palette: next });
  };

  const setPaletteColor = (key: string, color: string) => {
    onChange({ ...block, palette: { ...block.palette, [key]: color } });
  };

  return (
    <Stack spacing={2} sx={{ px: 2, pb: 2 }}>
      <Box>
        <MiniLabel>{t("badge_field")}</MiniLabel>
        <LayerFieldSelector
          fields={badgeFields}
          selectedField={selectedField}
          setSelectedField={(field) => {
            if (field && !Array.isArray(field)) {
              onChange({ ...block, field: field.name });
            }
          }}
        />
      </Box>

      <Box>
        <MiniLabel>{t("color_mode")}</MiniLabel>
        <PillToggleGroup
          value={block.mode}
          onChange={(v) =>
            onChange({ ...block, mode: v as PopupBadgeBlock["mode"] })
          }
          options={[
            { value: "single", label: t("single_color") },
            { value: "per_value", label: t("by_value") },
          ]}
        />
      </Box>

      {block.mode === "single" ? (
        <Box>
          <MiniLabel>{t("color")}</MiniLabel>
          <ColorSwatchButton
            color={block.color || DEFAULT_COLOR}
            onPick={(hex) => onChange({ ...block, color: hex })}
          />
        </Box>
      ) : (
        <PerValuePalette
          layerId={layerId}
          fieldName={block.field}
          palette={block.palette}
          onRemove={removePaletteEntry}
          onChangeColor={setPaletteColor}
          onAddValues={(values) => {
            const next = { ...block.palette };
            for (const v of values) {
              if (!next[v]) next[v] = FALLBACK_COLOR;
            }
            onChange({ ...block, palette: next });
          }}
        />
      )}

      <Stack
        direction="row"
        alignItems="center"
        justifyContent="space-between"
        spacing={2}>
        <Typography variant="body2">{t("full_width")}</Typography>
        <Switch
          size="small"
          checked={block.full_width}
          onChange={(_, v) => onChange({ ...block, full_width: v })}
        />
      </Stack>
    </Stack>
  );
}

function ColorSwatchButton({
  color,
  onPick,
}: {
  color: string;
  onPick: (hex: string) => void;
}) {
  const [open, setOpen] = useState(false);
  return (
    <ArrowPopper
      open={open}
      placement="bottom"
      arrow={false}
      disablePortal={false}
      isClickAwayEnabled={true}
      onClose={() => setOpen(false)}
      content={
        <Paper
          sx={{
            py: 2,
            boxShadow: "rgba(0, 0, 0, 0.16) 0px 6px 12px 0px",
            width: 235,
            maxHeight: 500,
          }}>
          <SingleColorSelector
            selectedColor={color}
            onSelectColor={(c) => {
              if (Array.isArray(c) && c.length === 3) {
                onPick(rgbToHex(c as RGBColor));
              }
            }}
          />
        </Paper>
      }>
      <Box
        onClick={() => setOpen((v) => !v)}
        role="button"
        tabIndex={0}
        onKeyDown={(e) => {
          if (e.key === "Enter" || e.key === " ") setOpen((v) => !v);
        }}
        sx={(theme) => ({
          height: 28,
          borderRadius: 1,
          bgcolor: color,
          cursor: "pointer",
          border: `1px solid ${alpha(theme.palette.text.primary, 0.1)}`,
          transition: "box-shadow 150ms",
          "&:hover": { boxShadow: theme.shadows[2] },
        })}
      />
    </ArrowPopper>
  );
}

function PerValuePalette({
  layerId,
  fieldName,
  palette,
  onRemove,
  onChangeColor,
  onAddValues,
}: {
  layerId: string;
  fieldName: string;
  palette: Record<string, string>;
  onRemove: (key: string) => void;
  onChangeColor: (key: string, color: string) => void;
  onAddValues: (values: string[]) => void;
}) {
  const { t } = useTranslation("common");
  const entries = Object.entries(palette);
  const addButtonRef = useRef<HTMLButtonElement | null>(null);
  const [pickerOpen, setPickerOpen] = useState(false);
  // Local selection inside the popper — what the user is currently
  // ticking. Committed to the palette in onDone (with the fallback
  // color). Kept separate from `palette` so closing the picker without
  // pressing Done discards the in-progress selection.
  const [pickerValues, setPickerValues] = useState<string[] | null>(null);

  const handleDone = () => {
    if (pickerValues && pickerValues.length > 0) {
      onAddValues(pickerValues);
    }
    setPickerValues(null);
    setPickerOpen(false);
  };

  const fieldDisabled = !fieldName;

  return (
    <Box>
      <MiniLabel>{t("value_colors")}</MiniLabel>
      <Stack spacing={0.75}>
        {entries.length === 0 ? (
          <Typography variant="caption" color="text.secondary">
            {t("badge_no_values_yet")}
          </Typography>
        ) : (
          entries.map(([val, color]) => (
            <Stack
              key={val}
              direction="row"
              spacing={1}
              alignItems="center"
              sx={{ minWidth: 0 }}>
              <ValueColorSwatch
                color={color}
                onPick={(hex) => onChangeColor(val, hex)}
              />
              <Typography
                variant="body2"
                sx={{
                  flex: 1,
                  minWidth: 0,
                  overflow: "hidden",
                  textOverflow: "ellipsis",
                  whiteSpace: "nowrap",
                }}
                title={val}>
                {val}
              </Typography>
              <IconButton
                size="small"
                disableRipple
                onClick={() => onRemove(val)}
                sx={(theme) => ({
                  p: 0.5,
                  color: alpha(theme.palette.text.primary, 0.55),
                  "&:hover": {
                    bgcolor: "transparent",
                    color: theme.palette.error.main,
                  },
                })}>
                <Icon iconName={ICON_NAME.TRASH} style={{ fontSize: 12 }} />
              </IconButton>
            </Stack>
          ))
        )}

        <Button
          ref={addButtonRef}
          size="small"
          variant="text"
          disabled={fieldDisabled}
          startIcon={
            <Icon iconName={ICON_NAME.PLUS} style={{ fontSize: 12 }} />
          }
          onClick={() => {
            setPickerValues(null);
            setPickerOpen(true);
          }}
          sx={{ alignSelf: "center", textTransform: "none", mt: 0.5 }}>
          {t("add_value")}
        </Button>
      </Stack>

      {pickerOpen && (
        <LayerValueSelectorPopper
          open={pickerOpen}
          layerId={layerId}
          fieldName={fieldName}
          selectedValues={pickerValues}
          onSelectedValuesChange={setPickerValues}
          anchorEl={addButtonRef.current}
          onDone={handleDone}
          // Drop the popper directly below the Add value button so it
          // sits inside / under the badge edit panel instead of flying
          // across the screen (the default offset is tuned for the wide
          // style panel).
          placement="bottom-start"
          offset={[0, 4]}
        />
      )}
    </Box>
  );
}

function ValueColorSwatch({
  color,
  onPick,
}: {
  color: string;
  onPick: (hex: string) => void;
}) {
  const [open, setOpen] = useState(false);
  return (
    <ArrowPopper
      open={open}
      placement="bottom-start"
      arrow={false}
      disablePortal={false}
      isClickAwayEnabled={true}
      onClose={() => setOpen(false)}
      content={
        <Paper
          sx={{
            py: 2,
            boxShadow: "rgba(0, 0, 0, 0.16) 0px 6px 12px 0px",
            width: 235,
            maxHeight: 500,
          }}>
          <SingleColorSelector
            selectedColor={color}
            onSelectColor={(c) => {
              if (Array.isArray(c) && c.length === 3) {
                onPick(rgbToHex(c as RGBColor));
              }
            }}
          />
        </Paper>
      }>
      <Box
        onClick={() => setOpen((v) => !v)}
        role="button"
        tabIndex={0}
        onKeyDown={(e) => {
          if (e.key === "Enter" || e.key === " ") setOpen((v) => !v);
        }}
        sx={(theme) => ({
          width: 22,
          height: 22,
          borderRadius: "50%",
          bgcolor: color,
          cursor: "pointer",
          flexShrink: 0,
          border: `1px solid ${alpha(theme.palette.text.primary, 0.15)}`,
          transition: "box-shadow 150ms",
          "&:hover": { boxShadow: theme.shadows[2] },
        })}
      />
    </ArrowPopper>
  );
}
