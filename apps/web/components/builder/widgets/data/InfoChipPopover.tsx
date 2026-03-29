import { Box, Button, Popover, Stack, TextField, Typography } from "@mui/material";

import type { Editor } from "@tiptap/react";
import { useCallback, useEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";

/**
 * Creates a stable virtual anchor from an element's bounding rect.
 * ProseMirror DOM nodes can get recreated, so we snapshot the position.
 */
function createVirtualAnchor(el: HTMLElement) {
  const rect = el.getBoundingClientRect();
  return {
    getBoundingClientRect: () => rect,
    nodeType: 1 as const,
  };
}

interface InfoChipEditPopoverProps {
  editor: Editor;
  anchorEl: HTMLElement | null;
  onClose: () => void;
}

/**
 * Popover for editing an info chip's text and URL.
 * Appears when a user clicks an info chip in edit mode.
 */
export const InfoChipEditPopover = ({ editor, anchorEl, onClose }: InfoChipEditPopoverProps) => {
  const { t } = useTranslation("common");
  const virtualAnchor = useRef(anchorEl ? createVirtualAnchor(anchorEl) : null);
  const node = editor.state.doc.nodeAt(editor.state.selection.from);
  const isInfoChip = node?.type.name === "infoChip";

  // Snapshot anchor position on open
  useEffect(() => {
    if (anchorEl) virtualAnchor.current = createVirtualAnchor(anchorEl);
  }, [anchorEl]);

  const [text, setText] = useState(isInfoChip ? (node?.attrs.text as string) || "" : "");
  const [url, setUrl] = useState(isInfoChip ? (node?.attrs.url as string) || "" : "");

  // Sync state when selection changes
  useEffect(() => {
    if (isInfoChip) {
      setText((node?.attrs.text as string) || "");
      setUrl((node?.attrs.url as string) || "");
    }
  }, [isInfoChip, node?.attrs.text, node?.attrs.url]);

  const handleSave = useCallback(() => {
    editor.chain().focus().updateInfoChip({ text, url }).run();
    onClose();
  }, [editor, text, url, onClose]);

  if (!isInfoChip) return null;

  return (
    <Popover
      open={!!anchorEl}
      anchorEl={virtualAnchor.current as unknown as HTMLElement}
      onClose={handleSave}
      anchorOrigin={{ vertical: "top", horizontal: "center" }}
      transformOrigin={{ vertical: "bottom", horizontal: "center" }}
      sx={{ zIndex: 1500 }}
      slotProps={{
        paper: {
          sx: { p: 2, width: 300, mb: 1 },
          onMouseDown: (e: React.MouseEvent) => e.stopPropagation(),
        },
      }}>
      <Stack spacing={1.5}>
        <Box>
          <Typography variant="caption" color="text.secondary" fontWeight={600} sx={{ mb: 0.5, display: "block" }}>
            {t("info_text", { defaultValue: "Info text" })}
          </Typography>
          <TextField
            multiline
            minRows={2}
            maxRows={4}
            size="small"
            fullWidth
            placeholder={t("enter_info_text", { defaultValue: "Enter explanation text..." })}
            value={text}
            onChange={(e) => setText(e.target.value)}
          />
        </Box>
        <Box>
          <Typography variant="caption" color="text.secondary" fontWeight={600} sx={{ mb: 0.5, display: "block" }}>
            {t("link_url", { defaultValue: "Link URL" })}{" "}
            <Typography component="span" variant="caption" color="text.disabled">
              ({t("optional", { defaultValue: "optional" })})
            </Typography>
          </Typography>
          <TextField
            size="small"
            fullWidth
            placeholder="https://..."
            value={url}
            onChange={(e) => setUrl(e.target.value)}
          />
        </Box>
        <Stack direction="row" justifyContent="flex-end">
          <Button size="small" variant="contained" onClick={handleSave}>
            {t("done", { defaultValue: "Done" })}
          </Button>
        </Stack>
      </Stack>
    </Popover>
  );
};

interface InfoChipViewPopoverProps {
  anchorEl: HTMLElement | null;
  text: string;
  url?: string;
  onClose: () => void;
}

/**
 * Popover shown in preview/public view when a user clicks an info chip.
 */
export const InfoChipViewPopover = ({ anchorEl, text, url, onClose }: InfoChipViewPopoverProps) => {
  const virtualAnchor = useRef(anchorEl ? createVirtualAnchor(anchorEl) : null);
  useEffect(() => {
    if (anchorEl) virtualAnchor.current = createVirtualAnchor(anchorEl);
  }, [anchorEl]);

  return (
    <Popover
      open={!!anchorEl}
      anchorEl={virtualAnchor.current as unknown as HTMLElement}
      onClose={onClose}
      anchorOrigin={{ vertical: "bottom", horizontal: "center" }}
      transformOrigin={{ vertical: "top", horizontal: "center" }}
      slotProps={{
        paper: {
          sx: { p: 2, maxWidth: 300, mt: 1 },
        },
      }}>
      <Typography variant="body2" sx={{ lineHeight: 1.5 }}>
        {text}
      </Typography>
      {url && (
        <Typography
          component="a"
          variant="body2"
          href={url}
          target="_blank"
          rel="noopener noreferrer"
          sx={{ mt: 1, display: "inline-flex", alignItems: "center", gap: 0.3, color: "primary.main", textDecoration: "none", fontSize: 13 }}>
          Learn more
          <Box component="span" sx={{ fontSize: 10 }}>↗</Box>
        </Typography>
      )}
    </Popover>
  );
};
