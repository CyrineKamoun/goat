import { html } from "@codemirror/lang-html";
import CodeMirror from "@uiw/react-codemirror";
import { Box, Chip, Stack, Typography } from "@mui/material";
import { useRef } from "react";
import { useTranslation } from "react-i18next";

import type { LayerField } from "@/components/map/popover/formatFeatureProperties";

interface Props {
  value: string;
  onChange: (next: string) => void;
  fields: LayerField[];
}

export function HtmlModeEditor({ value, onChange, fields }: Props) {
  const { t } = useTranslation("common");
  const insertAtCursor = useRef<(snippet: string) => void>(() => {});

  return (
    <Stack spacing={1}>
      <Typography variant="caption" color="text.secondary">
        {t("html_mode_help")}
      </Typography>
      <Box
        sx={{
          "& .cm-editor": { borderRadius: 1, border: 1, borderColor: "divider" },
        }}>
        <CodeMirror
          value={value}
          extensions={[html()]}
          minHeight="240px"
          onChange={onChange}
          onCreateEditor={(view) => {
            insertAtCursor.current = (snippet) => {
              const pos = view.state.selection.main.head;
              view.dispatch({
                changes: { from: pos, insert: snippet },
                selection: { anchor: pos + snippet.length },
              });
              view.focus();
            };
          }}
        />
      </Box>
      <Typography variant="caption" color="text.secondary">
        {t("insert_field_token")}
      </Typography>
      <Stack direction="row" flexWrap="wrap" useFlexGap spacing={0.5}>
        {fields.map((f) => (
          <Chip
            key={f.name}
            size="small"
            label={f.name}
            onClick={() => insertAtCursor.current(`{{${f.name}}}`)}
            sx={{ fontFamily: "ui-monospace, monospace", fontSize: 11 }}
          />
        ))}
      </Stack>
    </Stack>
  );
}
