import { Box, Paper, Stack, Typography } from "@mui/material";
import { alpha } from "@mui/material/styles";
import { useState } from "react";
import { useTranslation } from "react-i18next";

import type { PopupButtonBlock } from "@/lib/validations/layer";

import { rgbToHex } from "@/lib/utils/helpers";

import type { RGBColor } from "@/types/map/color";

import { ArrowPopper } from "@/components/ArrowPoper";
import TextFieldInput from "@/components/map/panels/common/TextFieldInput";
import SingleColorSelector from "@/components/map/panels/style/color/SingleColorSelector";

import { MiniLabel, PillToggleGroup } from "./_shared";

interface Props {
  block: PopupButtonBlock;
  onChange: (next: PopupButtonBlock) => void;
}

const DEFAULT_COLOR = "#FFB400";

export function ButtonBlockEditor({ block, onChange }: Props) {
  const { t } = useTranslation("common");
  const [colorOpen, setColorOpen] = useState(false);

  const currentColor = block.color || DEFAULT_COLOR;

  return (
    <Stack spacing={2} sx={{ px: 2, pb: 2 }}>
      <Box>
        <MiniLabel>{t("label")}</MiniLabel>
        <TextFieldInput
          value={block.label}
          onChange={(value) => onChange({ ...block, label: value })}
        />
      </Box>

      <Box>
        <MiniLabel>{t("url_template")}</MiniLabel>
        <TextFieldInput
          placeholder="https://…/{{id}}"
          value={block.url_template}
          onChange={(value) => onChange({ ...block, url_template: value })}
        />
        <Typography
          variant="caption"
          sx={{ display: "block", color: "text.secondary", mt: 0.5 }}>
          {t("url_template_help")}
        </Typography>
      </Box>

      <Box>
        <MiniLabel>{t("button_style")}</MiniLabel>
        <PillToggleGroup
          value={block.style}
          onChange={(v) => onChange({ ...block, style: v as PopupButtonBlock["style"] })}
          options={[
            { value: "link", label: t("style_link") },
            { value: "outlined", label: t("style_outlined") },
            { value: "filled", label: t("style_filled") },
          ]}
        />
      </Box>

      <Box>
        <MiniLabel>{t("color", { defaultValue: "Color" })}</MiniLabel>
        {/* Wide color swatch — full-width clickable bar showing the
            current color. ArrowPopper opens SingleColorSelector
            anchored to it (callback-ref based, stable across re-renders
            triggered by color picks). */}
        <ArrowPopper
          open={colorOpen}
          placement="bottom"
          arrow={false}
          disablePortal={false}
          isClickAwayEnabled={true}
          onClose={() => setColorOpen(false)}
          content={
            <Paper
              sx={{
                py: 2,
                boxShadow: "rgba(0, 0, 0, 0.16) 0px 6px 12px 0px",
                width: 235,
                maxHeight: 500,
              }}>
              <SingleColorSelector
                selectedColor={currentColor}
                onSelectColor={(c) => {
                  if (Array.isArray(c) && c.length === 3) {
                    onChange({ ...block, color: rgbToHex(c as RGBColor) });
                  }
                }}
              />
            </Paper>
          }>
          <Box
            onClick={() => setColorOpen((v) => !v)}
            role="button"
            tabIndex={0}
            onKeyDown={(e) => {
              if (e.key === "Enter" || e.key === " ") setColorOpen((v) => !v);
            }}
            sx={(theme) => ({
              height: 28,
              borderRadius: 1,
              bgcolor: currentColor,
              cursor: "pointer",
              border: `1px solid ${alpha(theme.palette.text.primary, 0.1)}`,
              transition: "box-shadow 150ms",
              "&:hover": { boxShadow: theme.shadows[2] },
            })}
          />
        </ArrowPopper>
      </Box>
    </Stack>
  );
}
