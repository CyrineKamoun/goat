import { Button, Popover, Stack, TextField } from "@mui/material";
import type { Editor } from "@tiptap/react";
import { useCallback, useEffect, useState } from "react";
import { useTranslation } from "react-i18next";

interface LinkPopoverProps {
  editor: Editor;
  anchorEl: HTMLElement | null;
  onClose: () => void;
}

/**
 * Popover for adding/editing a link on selected text.
 */
const LinkPopover = ({ editor, anchorEl, onClose }: LinkPopoverProps) => {
  const { t } = useTranslation("common");
  const existingUrl = editor.getAttributes("link").href || "";
  const [url, setUrl] = useState(existingUrl);

  useEffect(() => {
    setUrl(editor.getAttributes("link").href || "");
  }, [editor, anchorEl]);

  const handleSave = useCallback(() => {
    if (!url) {
      editor.chain().focus().extendMarkRange("link").unsetLink().run();
    } else {
      editor.chain().focus().extendMarkRange("link").setLink({ href: url, target: "_blank" }).run();
    }
    onClose();
  }, [editor, url, onClose]);

  const handleRemove = useCallback(() => {
    editor.chain().focus().extendMarkRange("link").unsetLink().run();
    onClose();
  }, [editor, onClose]);

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter") {
      e.preventDefault();
      handleSave();
    }
  };

  return (
    <Popover
      open={!!anchorEl}
      anchorEl={anchorEl}
      onClose={handleSave}
      anchorOrigin={{ vertical: "top", horizontal: "center" }}
      transformOrigin={{ vertical: "bottom", horizontal: "center" }}
      sx={{ zIndex: 1500 }}
      slotProps={{
        paper: {
          sx: { p: 1.5, width: 300, mb: 1 },
          onMouseDown: (e: React.MouseEvent) => e.stopPropagation(),
        },
      }}>
      <Stack direction="row" spacing={1} alignItems="center">
        <TextField
          autoFocus
          size="small"
          fullWidth
          placeholder="https://..."
          value={url}
          onChange={(e) => setUrl(e.target.value)}
          onKeyDown={handleKeyDown}
        />
        {existingUrl && (
          <Button size="small" color="error" onClick={handleRemove} sx={{ minWidth: "auto", px: 1 }}>
            {t("remove", { defaultValue: "Remove" })}
          </Button>
        )}
      </Stack>
    </Popover>
  );
};

export default LinkPopover;
