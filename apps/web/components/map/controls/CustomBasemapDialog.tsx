import {
  Box,
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  FormControlLabel,
  FormHelperText,
  Radio,
  RadioGroup,
  Stack,
  Tab,
  Tabs,
  Typography,
} from "@mui/material";
import { useEffect, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";

import WidgetColorPicker from "@/components/builder/widgets/common/WidgetColorPicker";
import TextFieldInput from "@/components/map/panels/common/TextFieldInput";
import { customBasemapSchema, type CustomBasemap } from "@/lib/validations/project";

type DraftType = "vector" | "raster" | "solid";

type SubmitPayload =
  | {
      type: "vector";
      name: string;
      description: string | null;
      thumbnail_url: string | null;
      attribution: string | null;
      url: string;
    }
  | {
      type: "raster";
      name: string;
      description: string | null;
      thumbnail_url: string | null;
      attribution: string | null;
      url: string;
    }
  | {
      type: "solid";
      name: string;
      description: string | null;
      thumbnail_url: string | null;
      attribution: string | null;
      color: string;
    };

interface Props {
  open: boolean;
  initial?: CustomBasemap | null;
  onClose: () => void;
  onSubmit: (payload: SubmitPayload) => Promise<void> | void;
  onDelete?: () => Promise<void> | void;
}

export function CustomBasemapDialog({ open, initial, onClose, onSubmit, onDelete }: Props) {
  const { t } = useTranslation("common");

  const initialTab: "basemap" | "solid" =
    initial?.type === "solid" ? "solid" : "basemap";
  const initialKind: DraftType = initial?.type ?? "vector";

  const [tab, setTab] = useState<"basemap" | "solid">(initialTab);
  const [kind, setKind] = useState<DraftType>(initialKind);
  const [name, setName] = useState(initial?.name ?? "");
  const [description, setDescription] = useState(initial?.description ?? "");
  const [thumbnailUrl, setThumbnailUrl] = useState(initial?.thumbnail_url ?? "");
  const [url, setUrl] = useState(
    initial && initial.type !== "solid" ? initial.url : ""
  );
  const [attribution, setAttribution] = useState(initial?.attribution ?? "");
  const [color, setColor] = useState(
    initial?.type === "solid" ? initial.color : "#888888"
  );
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);

  // Reset state when re-opening with a different `initial`.
  useEffect(() => {
    if (!open) return;
    setTab(initial?.type === "solid" ? "solid" : "basemap");
    setKind(initial?.type ?? "vector");
    setName(initial?.name ?? "");
    setDescription(initial?.description ?? "");
    setThumbnailUrl(initial?.thumbnail_url ?? "");
    setUrl(initial && initial.type !== "solid" ? initial.url : "");
    setAttribution(initial?.attribution ?? "");
    setColor(initial?.type === "solid" ? initial.color : "#888888");
    setError(null);
  }, [open, initial]);

  const placeholder = useMemo(
    () =>
      kind === "vector"
        ? "https://example.com/style.json"
        : "https://example.com/{z}/{x}/{y}.png",
    [kind]
  );

  async function handleSubmit() {
    setError(null);
    const trimmed = name.trim();
    if (!trimmed) {
      setError(t("basemap_name_required"));
      return;
    }

    let payload: SubmitPayload;
    if (tab === "solid") {
      payload = {
        type: "solid",
        name: trimmed,
        description: description.trim() || null,
        thumbnail_url: thumbnailUrl.trim() || null,
        attribution: attribution.trim() || null,
        color,
      };
    } else if (kind === "vector") {
      payload = {
        type: "vector",
        name: trimmed,
        description: description.trim() || null,
        thumbnail_url: thumbnailUrl.trim() || null,
        attribution: attribution.trim() || null,
        url: url.trim(),
      };
    } else {
      payload = {
        type: "raster",
        name: trimmed,
        description: description.trim() || null,
        thumbnail_url: thumbnailUrl.trim() || null,
        attribution: attribution.trim() || null,
        url: url.trim(),
      };
    }

    const validation = customBasemapSchema.safeParse({
      ...payload,
      id: "00000000-0000-0000-0000-000000000000",
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
    });
    if (!validation.success) {
      setError(validation.error.issues[0]?.message ?? "Invalid input");
      return;
    }

    try {
      setSubmitting(true);
      await onSubmit(payload);
      onClose();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <Dialog open={open} onClose={onClose} fullWidth maxWidth="xs">
      <DialogTitle>{initial ? t("edit_basemap") : t("add_basemap")}</DialogTitle>
      <Box sx={{ borderBottom: 1, borderColor: "divider" }}>
        <Tabs value={tab} onChange={(_, v) => setTab(v)}>
          <Tab value="basemap" label={t("basemap_tab_label")} />
          <Tab value="solid" label={t("solid_color")} />
        </Tabs>
      </Box>
      <DialogContent sx={{ pt: 3 }}>
        {tab === "basemap" && (
          <Stack spacing={2}>
            <RadioGroup
              row
              value={kind}
              onChange={(_, v) => setKind(v as DraftType)}
              sx={{ "& .MuiFormControlLabel-label": { fontSize: "0.8125rem" } }}>
              <FormControlLabel
                value="vector"
                control={<Radio size="small" />}
                label={t("vector_style")}
              />
              <FormControlLabel
                value="raster"
                control={<Radio size="small" />}
                label={t("raster_tiles")}
              />
            </RadioGroup>
            <TextFieldInput
              label={t("basemap_url")}
              placeholder={placeholder}
              value={url}
              onChange={setUrl}
            />
            {kind === "raster" && (
              <Typography variant="caption" color="text.secondary" sx={{ mt: -1 }}>
                {t("raster_url_must_contain_placeholders")}
              </Typography>
            )}
            <TextFieldInput label={t("title")} value={name} onChange={setName} />
            <TextFieldInput
              label={t("attribution")}
              value={attribution}
              onChange={setAttribution}
            />
            <TextFieldInput
              label={t("short_description")}
              value={description}
              onChange={setDescription}
              multiline
              rows={2}
            />
            <TextFieldInput
              label={t("thumbnail_url_optional")}
              value={thumbnailUrl}
              onChange={setThumbnailUrl}
            />
          </Stack>
        )}

        {tab === "solid" && (
          <Stack spacing={2}>
            <WidgetColorPicker label={t("color")} color={color} onChange={setColor} />
            <TextFieldInput label={t("title")} value={name} onChange={setName} />
            <TextFieldInput
              label={t("attribution")}
              value={attribution}
              onChange={setAttribution}
            />
            <TextFieldInput
              label={t("short_description")}
              value={description}
              onChange={setDescription}
              multiline
              rows={2}
            />
            <TextFieldInput
              label={t("thumbnail_url_optional")}
              value={thumbnailUrl}
              onChange={setThumbnailUrl}
            />
          </Stack>
        )}

        {error && (
          <FormHelperText error sx={{ mt: 1 }}>
            {error}
          </FormHelperText>
        )}
      </DialogContent>
      <DialogActions disableSpacing sx={{ pb: 2, px: 2, justifyContent: "space-between" }}>
        <Box>
          {initial && onDelete && (
            <Button
              onClick={async () => {
                try {
                  setSubmitting(true);
                  await onDelete();
                  onClose();
                } finally {
                  setSubmitting(false);
                }
              }}
              variant="text"
              color="error"
              disabled={submitting}
              sx={{ borderRadius: 0 }}>
              <Typography variant="body2" fontWeight="bold" color="inherit">
                {t("delete")}
              </Typography>
            </Button>
          )}
        </Box>
        <Box sx={{ display: "flex" }}>
          <Button onClick={onClose} variant="text" sx={{ borderRadius: 0 }}>
            <Typography variant="body2" fontWeight="bold">
              {t("cancel")}
            </Typography>
          </Button>
          <Button
            onClick={handleSubmit}
            variant="text"
            color="primary"
            disabled={submitting}
            sx={{ borderRadius: 0 }}>
            <Typography variant="body2" fontWeight="bold" color="inherit">
              {initial ? t("save") : t("add_basemap")}
            </Typography>
          </Button>
        </Box>
      </DialogActions>
    </Dialog>
  );
}
