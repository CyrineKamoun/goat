import { Box, Button, Menu, MenuItem, MenuList } from "@mui/material";
import { alpha } from "@mui/material/styles";
import type { Editor } from "@tiptap/core";
import { useMemo, useState } from "react";
import { useTranslation } from "react-i18next";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import type { PopupTextBlock } from "@/lib/validations/layer";

import FieldChip from "@/lib/extensions/field-chip";

import {
  DEFAULT_INLINE_EXTENSIONS,
  InlineRichTextEditor,
} from "@/components/common/InlineRichTextEditor";
import type { LayerField } from "@/components/map/popover/formatFeatureProperties";

interface Props {
  block: PopupTextBlock;
  fields: LayerField[];
  onChange: (next: PopupTextBlock) => void;
}

export function TextBlockEditor({ block, fields, onChange }: Props) {
  const { t } = useTranslation("common");
  const [fieldMenu, setFieldMenu] = useState<HTMLElement | null>(null);

  // Spread the editor's default extensions so toolbar commands (color,
  // font size, link, etc.) stay registered, then add FieldChip on top
  // for `{{field}}` token insertion.
  const extensions = useMemo(
    () => [...DEFAULT_INLINE_EXTENSIONS, FieldChip],
    [],
  );

  const renderInsertField = (editor: Editor) => (
    <>
      <Button
        size="small"
        variant="text"
        onClick={(e) => setFieldMenu(e.currentTarget)}
        startIcon={<Icon iconName={ICON_NAME.PLUS} style={{ fontSize: 11 }} />}
        sx={(theme) => ({
          textTransform: "none",
          fontSize: 11,
          fontWeight: "bold",
          color: "primary.main",
          bgcolor: alpha(theme.palette.primary.main, 0.1),
          border: `1px solid ${alpha(theme.palette.primary.main, 0.4)}`,
          borderRadius: 999,
          px: 1.25,
          py: 0.25,
          minWidth: 0,
          "&:hover": { bgcolor: alpha(theme.palette.primary.main, 0.18) },
        })}>
        {t("insert_field")}
      </Button>
      <Menu
        anchorEl={fieldMenu}
        open={Boolean(fieldMenu)}
        onClose={() => setFieldMenu(null)}
        slotProps={{ paper: { sx: { maxHeight: 280 } } }}>
        <MenuList dense>
          {fields.map((f) => (
            <MenuItem
              key={f.name}
              onClick={() => {
                editor.chain().focus().insertField(f.name).run();
                setFieldMenu(null);
              }}
              sx={{ fontFamily: "ui-monospace, monospace", fontSize: 12 }}>
              {f.name}
            </MenuItem>
          ))}
        </MenuList>
      </Menu>
    </>
  );

  return (
    // Outer padding so the editor's border isn't flush with the popper edge.
    <Box sx={{ px: 2, pb: 2 }}>
      <InlineRichTextEditor
        value={block.html}
        onChange={(html) => onChange({ ...block, html })}
        extensions={extensions}
        toolbarExtras={renderInsertField}
        contentSx={(theme) => ({
          // Pill-shaped field chip — primary-tinted, monospaced, sized to
          // read as a token inside running text.
          "& .field-chip": {
            display: "inline-block",
            px: 0.875,
            mx: 0.25,
            bgcolor: alpha(theme.palette.primary.main, 0.14),
            color: "primary.main",
            borderRadius: 1,
            fontSize: "0.9em",
            fontWeight: "bold",
            fontFamily: "ui-monospace, SFMono-Regular, Menlo, monospace",
            lineHeight: 1.5,
          },
        })}
      />
    </Box>
  );
}
