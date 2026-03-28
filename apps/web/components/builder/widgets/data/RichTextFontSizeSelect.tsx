import ArrowDropDownIcon from "@mui/icons-material/ArrowDropDown";
import { ListItemText, Menu, MenuItem, ToggleButton, Typography } from "@mui/material";
import type { Editor } from "@tiptap/react";
import { useEditorState } from "@tiptap/react";
import { useEffect, useState } from "react";

/** Pixel-based presets suitable for dashboard widgets */
const PX_PRESETS = [10, 12, 14, 16, 18, 20, 24, 28, 32, 36, 42, 48, 56, 64, 72];

interface RichTextFontSizeSelectProps {
  editor: Editor;
  onOpen?: () => void;
  onClose?: () => void;
  forceClose?: boolean;
}

function parsePxSize(value: string | null | undefined): number {
  if (!value) return 16;
  const match = value.match(/^(\d+(?:\.\d+)?)\s*px$/i);
  return match ? parseFloat(match[1]) : 16;
}

const RichTextFontSizeSelect: React.FC<RichTextFontSizeSelectProps> = ({
  editor,
  onOpen,
  onClose,
  forceClose,
}) => {
  const [anchorEl, setAnchorEl] = useState<null | HTMLElement>(null);
  const open = Boolean(anchorEl);

  const editorState = useEditorState({
    editor,
    selector: ({ editor: e }: { editor: Editor }) => {
      const fontSize = e.getAttributes("textStyle").fontSize as string | undefined;
      return { fontSize: fontSize || null };
    },
  });

  const currentSize = parsePxSize(editorState?.fontSize);

  useEffect(() => {
    if (forceClose && anchorEl) {
      setAnchorEl(null);
    }
  }, [forceClose, anchorEl]);

  const handleOpen = (event: React.MouseEvent<HTMLElement>) => {
    event.stopPropagation();
    if (anchorEl) {
      setAnchorEl(null);
    } else {
      onOpen?.();
      setAnchorEl(event.currentTarget);
    }
  };

  const handleClose = () => {
    setAnchorEl(null);
    onClose?.();
  };

  const handleSelect = (size: number) => {
    editor.chain().focus().setFontSize(`${size}px`).run();
    handleClose();
  };

  return (
    <>
      <ToggleButton
        value="fontSize"
        size="small"
        selected={open}
        onClick={handleOpen}
        sx={{ display: "flex", alignItems: "center", minWidth: 44 }}>
        <Typography variant="caption" sx={{ fontWeight: 600, lineHeight: 1, fontSize: "0.8rem" }}>
          {currentSize}
        </Typography>
        <ArrowDropDownIcon fontSize="small" />
      </ToggleButton>

      <Menu
        anchorEl={anchorEl}
        open={open}
        onClose={handleClose}
        anchorOrigin={{ vertical: "bottom", horizontal: "left" }}
        transformOrigin={{ vertical: "top", horizontal: "left" }}
        sx={{ zIndex: 1500 }}
        slotProps={{
          paper: {
            onMouseDown: (e: React.MouseEvent) => e.stopPropagation(),
            onPointerDown: (e: React.PointerEvent) => e.stopPropagation(),
            sx: { maxHeight: 350 },
          },
        }}>
        {PX_PRESETS.map((size) => (
          <MenuItem
            dense
            key={size}
            selected={currentSize === size}
            onClick={() => handleSelect(size)}>
            <ListItemText>{size}px</ListItemText>
          </MenuItem>
        ))}
      </Menu>
    </>
  );
};

export default RichTextFontSizeSelect;
