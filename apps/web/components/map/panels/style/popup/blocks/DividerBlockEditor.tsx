import { Box, Paper, Stack, Typography } from "@mui/material";
import { alpha } from "@mui/material/styles";
import { useState } from "react";
import { useTranslation } from "react-i18next";

import type { PopupDividerBlock } from "@/lib/validations/layer";

import { rgbToHex } from "@/lib/utils/helpers";

import type { RGBColor } from "@/types/map/color";

import { ArrowPopper } from "@/components/ArrowPoper";
import SliderInput from "@/components/map/panels/common/SliderInput";
import SingleColorSelector from "@/components/map/panels/style/color/SingleColorSelector";

import { MiniLabel } from "./_shared";

interface Props {
  block: PopupDividerBlock;
  onChange: (next: PopupDividerBlock) => void;
}

// Neutral hairline used when the user hasn't picked a color. Matches the
// muted look of MUI's default divider in both light and dark themes
// without us having to read the theme here.
const DEFAULT_PREVIEW_COLOR = "#C4C4C4";

export function DividerBlockEditor({ block, onChange }: Props) {
  const { t } = useTranslation("common");
  const [colorOpen, setColorOpen] = useState(false);

  const previewColor = block.color || DEFAULT_PREVIEW_COLOR;

  return (
    <Stack spacing={2} sx={{ px: 2, pb: 2 }}>
      <Box>
        <MiniLabel>{t("thickness")}</MiniLabel>
        <Stack direction="row" spacing={1.5} alignItems="center">
          <Box sx={{ flex: 1 }}>
            <SliderInput
              isRange={false}
              min={1}
              max={8}
              step={1}
              value={block.thickness}
              onChange={(v) =>
                typeof v === "number" && onChange({ ...block, thickness: v })
              }
            />
          </Box>
          <Typography variant="caption" sx={{ color: "text.secondary", flexShrink: 0 }}>
            px
          </Typography>
        </Stack>
      </Box>

      <Box>
        <MiniLabel>{t("color")}</MiniLabel>
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
                selectedColor={previewColor}
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
              bgcolor: previewColor,
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
