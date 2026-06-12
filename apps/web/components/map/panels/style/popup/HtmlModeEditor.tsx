import { autocompletion, type CompletionContext } from "@codemirror/autocomplete";
import { html } from "@codemirror/lang-html";
import { Autocomplete, Box, Stack, TextField, Typography } from "@mui/material";
import CodeMirror from "@uiw/react-codemirror";
import { useMemo, useRef } from "react";
import { useTranslation } from "react-i18next";

import FormLabelHelper from "@/components/common/FormLabelHelper";

import type { LayerField } from "@/components/map/popover/formatFeatureProperties";

interface Props {
  value: string;
  onChange: (next: string) => void;
  fields: LayerField[];
}

export function HtmlModeEditor({ value, onChange, fields }: Props) {
  const { t } = useTranslation("common");
  const insertAtCursor = useRef<(snippet: string) => void>(() => {});

  // `{{`-triggered field completion. Fires when the two chars before the
  // cursor are "{{"; offers each field, inserting `name}}` to close the token.
  const fieldCompletion = useMemo(() => {
    const names = fields.map((f) => f.name);
    return autocompletion({
      override: [
        (ctx: CompletionContext) => {
          const before = ctx.matchBefore(/\{\{\w*/);
          if (!before) return null;
          const typed = before.text.slice(2);
          return {
            from: before.from + 2,
            // We pre-filter by prefix ourselves, so disable CodeMirror's
            // own fuzzy re-filter to keep the offered list predictable.
            filter: false,
            options: names
              .filter((n) => n.toLowerCase().startsWith(typed.toLowerCase()))
              .map((n) => ({ label: n, apply: `${n}}}` })),
          };
        },
      ],
    });
  }, [fields]);

  const fieldNames = useMemo(() => fields.map((f) => f.name), [fields]);

  return (
    // flex:1 (not height:100%) so the editor is bounded by the flex-column
    // pane it lives in — percentage height doesn't resolve through the chain,
    // which let the editor grow to fit content with no scrollbar.
    <Stack spacing={1} sx={{ flex: 1, minHeight: 0, height: "100%" }}>
      {/* Searchable field inserter — full-width, label-above (matching the
          style-panel Selector pattern). Type to filter, capped/scrollable
          list. The {{ autocomplete inside the editor is the primary path;
          this is the discoverable fallback. Stays empty (value=null) so it
          resets after each insert. */}
      <Stack spacing={1}>
        <FormLabelHelper label={t("insert_field")} color="inherit" />
        <Autocomplete
          size="small"
          fullWidth
          options={fieldNames}
          value={null}
          blurOnSelect
          clearOnBlur
          autoHighlight
          onChange={(_, name) => {
            if (name) insertAtCursor.current(`{{${name}}}`);
          }}
          renderInput={(params) => <TextField {...params} placeholder={t("select_field")} />}
          renderOption={(props, name) => (
            <li {...props} style={{ fontFamily: "ui-monospace, monospace", fontSize: 12 }}>
              {name}
            </li>
          )}
          ListboxProps={{ sx: { maxHeight: 240 } }}
        />
      </Stack>

      <Box
        sx={{
          flex: 1,
          minHeight: 0,
          // CodeMirror defaults to a cramped ~10px monospace; bump the size
          // and round the editor frame.
          "& .cm-editor": {
            fontSize: 13.5,
            borderRadius: 1,
            border: 1,
            borderColor: "divider",
          },
          "& .cm-editor.cm-focused": { outline: "none", borderColor: "primary.main" },
          "& .cm-gutters": { fontSize: 13.5 },
          "& .cm-scroller": { lineHeight: 1.6 },
        }}>
        {/* Explicit min/max height (same proven pattern as SqlCodeEditor) so
            the editor scrolls internally once content exceeds the visible
            area, rather than growing and getting clipped by the pane. */}
        <CodeMirror
          value={value}
          extensions={[html(), fieldCompletion]}
          minHeight="200px"
          maxHeight="calc(82vh - 260px)"
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
        {t("html_mode_help")}
      </Typography>
    </Stack>
  );
}
