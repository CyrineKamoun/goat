import { Box, Stack, Typography } from "@mui/material";
import { useTranslation } from "react-i18next";

import type { LayerFieldType, PopupImageBlock } from "@/lib/validations/layer";

import type { SelectorItem } from "@/types/map/common";

import LayerFieldSelector from "@/components/map/common/LayerFieldSelector";
import type { LayerField } from "@/components/map/popover/formatFeatureProperties";
import Selector from "@/components/map/panels/common/Selector";
import SliderInput from "@/components/map/panels/common/SliderInput";
import TextFieldInput from "@/components/map/panels/common/TextFieldInput";

import { MiniLabel, PillToggleGroup } from "./_shared";

interface Props {
  block: PopupImageBlock;
  fields: LayerField[];
  onChange: (next: PopupImageBlock) => void;
}

export function ImageBlockEditor({ block, fields, onChange }: Props) {
  const { t } = useTranslation("common");

  // Only string-typed fields are reasonable URL holders. Coerce to the
  // LayerFieldType shape LayerFieldSelector expects.
  const urlFields = fields.filter((f) => f.type === "string") as LayerFieldType[];
  const selectedField = urlFields.find((f) => f.name === block.field);

  const aspectItems: SelectorItem[] = [
    { value: "16/9", label: "16:9" },
    { value: "4/3", label: "4:3" },
    { value: "3/2", label: "3:2" },
    { value: "1/1", label: "1:1" },
    { value: "2/3", label: "2:3" },
  ];
  const selectedAspect = aspectItems.find((i) => i.value === block.aspect);

  return (
    <Stack spacing={2} sx={{ px: 2, pb: 2 }}>
      <Box>
        <MiniLabel>{t("image_source")}</MiniLabel>
        <PillToggleGroup
          value={block.source}
          onChange={(v) => onChange({ ...block, source: v as PopupImageBlock["source"] })}
          options={[
            { value: "field", label: t("from_field") },
            { value: "static", label: t("static_url") },
          ]}
        />
      </Box>

      {block.source === "field" ? (
        <Box>
          <MiniLabel>{t("image_field")}</MiniLabel>
          <LayerFieldSelector
            fields={urlFields}
            selectedField={selectedField}
            setSelectedField={(field) =>
              onChange({ ...block, field: field?.name ?? undefined })
            }
          />
        </Box>
      ) : (
        <Box>
          <MiniLabel>{t("image_url")}</MiniLabel>
          <TextFieldInput
            placeholder="https://…"
            value={block.url ?? ""}
            onChange={(value) => onChange({ ...block, url: value })}
          />
        </Box>
      )}

      <Box>
        <MiniLabel>{t("image_sizing")}</MiniLabel>
        <PillToggleGroup
          value={block.sizing}
          onChange={(v) => onChange({ ...block, sizing: v as PopupImageBlock["sizing"] })}
          options={[
            { value: "fit", label: t("image_fit") },
            { value: "fixed", label: t("image_fixed_height") },
            { value: "aspect", label: t("image_aspect") },
          ]}
        />
      </Box>

      {block.sizing === "fixed" && (
        <Box>
          <MiniLabel>{t("height_px")}</MiniLabel>
          <Stack direction="row" spacing={1.5} alignItems="center">
            <Box sx={{ flex: 1 }}>
              <SliderInput
                isRange={false}
                min={40}
                max={400}
                step={10}
                value={block.height}
                onChange={(v) =>
                  typeof v === "number" && onChange({ ...block, height: v })
                }
              />
            </Box>
            <Typography
              variant="caption"
              sx={{ color: "text.secondary", flexShrink: 0 }}>
              px
            </Typography>
          </Stack>
          <Typography
            variant="caption"
            sx={{ color: "text.secondary", display: "block", mt: 0.5 }}>
            {t("image_fixed_height_help", {
              defaultValue: "Image fills the popup width and crops to this height.",
            })}
          </Typography>
        </Box>
      )}

      {block.sizing === "aspect" && (
        <Box>
          <MiniLabel>{t("aspect_ratio")}</MiniLabel>
          <Selector
            items={aspectItems}
            selectedItems={selectedAspect}
            setSelectedItems={(item) => {
              if (item && !Array.isArray(item)) {
                onChange({ ...block, aspect: item.value as PopupImageBlock["aspect"] });
              }
            }}
          />
        </Box>
      )}
    </Stack>
  );
}
