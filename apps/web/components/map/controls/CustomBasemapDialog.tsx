import ExpandLessIcon from "@mui/icons-material/ExpandLess";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import ParkIcon from "@mui/icons-material/Park";
import SearchIcon from "@mui/icons-material/Search";
import WaterIcon from "@mui/icons-material/Water";
import {
  Box,
  Button,
  CircularProgress,
  Collapse,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  Divider,
  FormControlLabel,
  FormHelperText,
  IconButton,
  InputAdornment,
  MenuItem,
  Radio,
  RadioGroup,
  Select,
  Stack,
  Switch,
  Tab,
  Tabs,
  TextField,
  Tooltip,
  Typography,
} from "@mui/material";
import { memo, useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { useMap } from "react-map-gl/maplibre";

import { Icon, ICON_NAME } from "@p4b/ui/components/Icon";

import { useAppDispatch } from "@/hooks/store/ContextHooks";
import { setBasemapLayerConfigOverride } from "@/lib/store/map/slice";
import WidgetColorPicker from "@/components/builder/widgets/common/WidgetColorPicker";
import TextFieldInput from "@/components/map/panels/common/TextFieldInput";
import { classifyBasemapLayers, resolveTarget, type BasemapCategory } from "@/lib/utils/map/basemapLayers";
import {
  customBasemapSchema,
  type BasemapLayerConfig,
  type BasemapLayerSetting,
  type CustomBasemap,
} from "@/lib/validations/project";

type DraftType = "vector" | "raster" | "solid";

type SubmitPayload =
  | {
      type: "vector";
      name: string;
      description: string | null;
      thumbnail_url: string | null;
      attribution: string | null;
      url: string;
      layer_config?: BasemapLayerConfig;
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
  projectLayers?: Array<{ id: number; name?: string | null }>;
}

const CATEGORY_ORDER: BasemapCategory[] = [
  "labels",
  "roads",
  "water",
  "landuse",
  "buildings",
  "poi",
  "other",
];

// Per-category accent color + icon (project icons where available, MUI fallback
// for water/land use). Rendered in a soft colored square next to the title.
const CATEGORY_META: Record<BasemapCategory, { color: string; icon: React.ReactNode }> = {
  labels: { color: "#3b82f6", icon: <Icon iconName={ICON_NAME.TEXT} fontSize="inherit" htmlColor="inherit" /> },
  roads: { color: "#f59e0b", icon: <Icon iconName={ICON_NAME.STREET_NETWORK} fontSize="inherit" htmlColor="inherit" /> },
  water: { color: "#06b6d4", icon: <WaterIcon sx={{ fontSize: 16 }} /> },
  landuse: { color: "#22c55e", icon: <ParkIcon sx={{ fontSize: 16 }} /> },
  buildings: { color: "#8b5cf6", icon: <Icon iconName={ICON_NAME.HOUSE} fontSize="inherit" htmlColor="inherit" /> },
  poi: { color: "#ef4444", icon: <Icon iconName={ICON_NAME.LOCATION_MARKER} fontSize="inherit" htmlColor="inherit" /> },
  other: { color: "#94a3b8", icon: <Icon iconName={ICON_NAME.LAYERS} fontSize="inherit" htmlColor="inherit" /> },
};

function CategoryIcon({ category }: { category: BasemapCategory }) {
  const meta = CATEGORY_META[category];
  return (
    <Box
      sx={{
        width: 28,
        height: 28,
        borderRadius: 1,
        flexShrink: 0,
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        fontSize: 16,
        color: meta.color,
        bgcolor: `${meta.color}26`, // ~15% alpha
      }}>
      {meta.icon}
    </Box>
  );
}

const DEFAULT_LAYER_SETTING: BasemapLayerSetting = {
  visible: true,
  relation: "below",
  target: "all",
};

interface LayerControlsProps {
  ids: string[];
  compact?: boolean;
  settingFor: (id: string) => BasemapLayerSetting;
  displayTarget: (s: BasemapLayerSetting) => string;
  setSetting: (ids: string[], patch: Partial<BasemapLayerSetting>) => void;
  projectLayers?: Array<{ id: number; name?: string | null }>;
  t: (key: string) => string;
}

const isDefaultSetting = (s: BasemapLayerSetting) =>
  s.visible && s.relation === "below" && s.target === "all";

// MUI Select for the "target" dropdown, but the full per-project-layer MenuItem
// list is only built while the menu is open. Closed, it renders a single
// MenuItem for the current value — so expanding a category with many rows
// doesn't pay N(layers) × rows MenuItem creations up front (which janks).
const TargetSelect = memo(function TargetSelect({
  value,
  compact,
  projectLayers,
  t,
  onChange,
}: {
  value: string;
  compact?: boolean;
  projectLayers?: Array<{ id: number; name?: string | null }>;
  t: (key: string) => string;
  onChange: (value: string) => void;
}) {
  const [open, setOpen] = useState(false);
  const label =
    value === ""
      ? "—"
      : value === "all"
        ? t("basemap_layer_all_my_layers")
        : ((projectLayers ?? []).find((l) => String(l.id) === value)?.name ??
          t("untitled_layer"));

  return (
    <Select
      size="small"
      value={value}
      displayEmpty
      open={open}
      onOpen={() => setOpen(true)}
      onClose={() => setOpen(false)}
      renderValue={() => label}
      onChange={(e) => onChange(e.target.value as string)}
      // Fixed width (not minWidth) so the control doesn't resize with the
      // selected value; long names ellipsize via MUI's default .MuiSelect-select.
      sx={{ fontSize: "0.75rem", width: compact ? 150 : 180, height: 28 }}>
      {open
        ? [
            <MenuItem key="all" value="all" sx={{ fontSize: "0.75rem" }}>
              {t("basemap_layer_all_my_layers")}
            </MenuItem>,
            ...(projectLayers ?? []).map((layer) => (
              <MenuItem key={layer.id} value={String(layer.id)} sx={{ fontSize: "0.75rem" }}>
                {layer.name ?? t("untitled_layer")}
              </MenuItem>
            )),
          ]
        : // Closed: a single MenuItem matching the current value (cheap; avoids
          // MUI's out-of-range-value warning).
          [
            <MenuItem key={value || "empty"} value={value} sx={{ fontSize: "0.75rem" }}>
              {label}
            </MenuItem>,
          ]}
    </Select>
  );
});

const LayerControls = memo(function LayerControls({
  ids,
  compact,
  settingFor,
  displayTarget,
  setSetting,
  projectLayers,
  t,
}: LayerControlsProps) {
  const settings = ids.map(settingFor);
  const allVisible = settings.every((s) => s.visible);
  const someVisible = settings.some((s) => s.visible);
  const isMixed = someVisible && !allVisible;

  const firstRelation = settings[0]?.relation ?? "below";
  const allSameRelation = settings.every((s) => s.relation === firstRelation);

  const firstTarget = displayTarget(settings[0] ?? DEFAULT_LAYER_SETTING);
  const allSameTarget = settings.every((s) => displayTarget(s) === firstTarget);

  // Fully hidden → the position dropdowns are meaningless; show a muted "Hidden"
  // label instead, keeping only the toggle to bring it back.
  const hidden = !someVisible;

  // Reset is offered only when something here actually deviates from default.
  const hasOverride = settings.some((s) => !isDefaultSetting(s));

  return (
    <Stack
      direction="row"
      alignItems="center"
      spacing={1}
      sx={{ ml: "auto", flexShrink: 0 }}>
      {hidden ? (
        <Typography
          variant="body2"
          color="text.disabled"
          noWrap
          sx={{
            fontStyle: "italic",
            // Span the width the two dropdowns occupy so the toggle stays put.
            minWidth: compact ? 238 : 284,
          }}>
          {t("basemap_layer_hidden")}
        </Typography>
      ) : (
        <>
          <Select
            size="small"
            value={allSameRelation ? firstRelation : ""}
            displayEmpty
            onChange={(e) => setSetting(ids, { relation: e.target.value as "above" | "below" })}
            sx={{ fontSize: "0.75rem", width: compact ? 80 : 96, height: 28 }}>
            {!allSameRelation && (
              <MenuItem value="" disabled sx={{ fontSize: "0.75rem" }}>
                —
              </MenuItem>
            )}
            <MenuItem value="above" sx={{ fontSize: "0.75rem" }}>
              {t("basemap_layer_above")}
            </MenuItem>
            <MenuItem value="below" sx={{ fontSize: "0.75rem" }}>
              {t("basemap_layer_below")}
            </MenuItem>
          </Select>
          <TargetSelect
            value={allSameTarget ? firstTarget : ""}
            compact={compact}
            projectLayers={projectLayers}
            t={t}
            onChange={(v) => setSetting(ids, { target: v })}
          />
        </>
      )}
      <Tooltip title={t("basemap_layer_reset")}>
        <span>
          <IconButton
            size="small"
            disabled={!hasOverride}
            onClick={() => setSetting(ids, { visible: true, relation: "below", target: "all" })}
            sx={{ p: 0.25, fontSize: 14, visibility: hasOverride ? "visible" : "hidden" }}>
            <Icon iconName={ICON_NAME.REFRESH} fontSize="inherit" htmlColor="inherit" />
          </IconButton>
        </span>
      </Tooltip>
      <Switch
        size="small"
        checked={isMixed ? false : allVisible}
        onChange={(_, checked) => setSetting(ids, { visible: checked })}
        sx={{ ml: 0.5, opacity: isMixed ? 0.5 : 1 }}
      />
    </Stack>
  );
});

export function CustomBasemapDialog({
  open,
  initial,
  onClose,
  onSubmit,
  onDelete,
  projectLayers,
}: Props) {
  const { t } = useTranslation("common");

  const initialTab: "basemap" | "solid" | "layers" =
    initial?.type === "solid" ? "solid" : "basemap";
  const initialKind: DraftType = initial?.type ?? "vector";

  const [tab, setTab] = useState<"basemap" | "solid" | "layers">(initialTab);
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
  const [layerConfig, setLayerConfig] = useState<BasemapLayerConfig>(() =>
    initial?.type === "vector" ? (initial.layer_config ?? {}) : {}
  );
  const [expandedCategories, setExpandedCategories] = useState<Set<BasemapCategory>>(
    new Set()
  );

  // Signature of the basemap's style-layer SET (sorted ids). Used to reclassify
  // only when the layer set changes (basemap switch / ephemeral-preview load),
  // NOT when layers are merely reordered by a stacking change — otherwise the
  // edited row would jump as the live style order shifts. Reset on open.
  const lastLayerSigRef = useRef("");

  // Reset state when re-opening with a different `initial`.
  useEffect(() => {
    if (!open) return;
    lastLayerSigRef.current = "";
    setTab(initial?.type === "solid" ? "solid" : "basemap");
    setKind(initial?.type ?? "vector");
    setName(initial?.name ?? "");
    setDescription(initial?.description ?? "");
    setThumbnailUrl(initial?.thumbnail_url ?? "");
    setUrl(initial && initial.type !== "solid" ? initial.url : "");
    setAttribution(initial?.attribution ?? "");
    setColor(initial?.type === "solid" ? initial.color : "#888888");
    setError(null);
    setLayerConfig(initial?.type === "vector" ? (initial.layer_config ?? {}) : {});
    setExpandedCategories(new Set());
  }, [open, initial]);

  const placeholder = useMemo(
    () =>
      kind === "vector"
        ? "https://example.com/style.json"
        : "https://example.com/{z}/{x}/{y}.png",
    [kind]
  );

  // --- Layers tab helpers ---
  const { map } = useMap();
  const dispatch = useAppDispatch();

  const [styleVersion, setStyleVersion] = useState(0);
  const [layerSearch, setLayerSearch] = useState("");

  // Live preview: push the in-progress layer_config into a Redux override so the
  // map renderer applies it immediately while editing. Always sets the object
  // (even {}) so clearing settings previews too. Cleared on close (below), which
  // reverts to the persisted config (cancel) or the just-saved one (save).
  useEffect(() => {
    if (!open || initial?.type !== "vector") return;
    dispatch(setBasemapLayerConfigOverride(layerConfig));
  }, [open, initial?.type, layerConfig, dispatch]);

  useEffect(() => {
    if (open) return;
    dispatch(setBasemapLayerConfigOverride(undefined));
  }, [open, dispatch]);

  // Also clear the live-preview override on unmount (e.g. navigating away with
  // the dialog still open), so it can't leak onto the next view's basemap.
  useEffect(() => {
    return () => {
      dispatch(setBasemapLayerConfigOverride(undefined));
    };
  }, [dispatch]);

  // Reclassify only when the basemap's layer SET changes (basemap switch / preview
  // load), not on every styledata tick. A stacking change reorders layers but
  // keeps the same set, so the sorted-id signature is unchanged → no reclassify →
  // the edited row stays put. lastLayerSigRef is declared above and reset on open.
  useEffect(() => {
    if (!open || tab !== "layers" || !map) return;
    const handler = () => {
      const ids = (map.getStyle()?.layers ?? []).map((l) => l.id);
      const sig = [...ids].sort().join("|");
      if (sig !== lastLayerSigRef.current) {
        lastLayerSigRef.current = sig;
        setStyleVersion((v) => v + 1);
      }
    };
    map.on("styledata", handler);
    return () => {
      map.off("styledata", handler);
    };
  }, [open, tab, map]);

  const basemapLayerInfos = useMemo(() => {
    if (!open || tab !== "layers" || !map) return [];
    const style = map.getStyle();
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const styleLayers = (style?.layers ?? []) as any[];
    // Exclude the user's own project layers — only basemap layers belong here.
    // A project layer's style id is its numeric id; its sublayers share its source.
    const projectIds = new Set((projectLayers ?? []).map((l) => String(l.id)));
    const projectSources = new Set(
      styleLayers
        .filter((l) => projectIds.has(l.id) && "source" in l)
        .map((l) => l.source)
    );
    const basemapOnly = styleLayers.filter(
      (l) => !projectIds.has(l.id) && !("source" in l && projectSources.has(l.source))
    );
    return classifyBasemapLayers(basemapOnly);
    // styleVersion is intentionally included so this recomputes when the style changes
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open, tab, map, styleVersion, projectLayers]);

  // Group layer infos by category, preserving style order, applying the search filter.
  const layersByCategory = useMemo(() => {
    const q = layerSearch.trim().toLowerCase();
    return CATEGORY_ORDER.reduce<Record<BasemapCategory, typeof basemapLayerInfos>>(
      (acc, cat) => {
        acc[cat] = basemapLayerInfos.filter(
          (info) =>
            info.category === cat &&
            (!q ||
              info.prettyName.toLowerCase().includes(q) ||
              info.id.toLowerCase().includes(q))
        );
        return acc;
      },
      {} as Record<BasemapCategory, typeof basemapLayerInfos>
    );
  }, [basemapLayerInfos, layerSearch]);

  const hasSearch = layerSearch.trim().length > 0;
  const hasVisibleLayers = CATEGORY_ORDER.some((cat) => layersByCategory[cat]?.length);

  const validTargetIds = useMemo(
    () => new Set((projectLayers ?? []).map((l) => String(l.id))),
    [projectLayers]
  );

  // Stable identities so memoized LayerControls rows don't re-render on every
  // keystroke/search (only when the data they depend on actually changes).
  const settingFor = useCallback(
    (id: string): BasemapLayerSetting => layerConfig[id] ?? { ...DEFAULT_LAYER_SETTING },
    [layerConfig]
  );

  const displayTarget = useCallback(
    (s: BasemapLayerSetting): string => resolveTarget(s.target, validTargetIds),
    [validTargetIds]
  );

  const setSetting = useCallback((ids: string[], patch: Partial<BasemapLayerSetting>) => {
    setLayerConfig((prev) => {
      const next = { ...prev };
      for (const id of ids) {
        const current = prev[id] ?? { ...DEFAULT_LAYER_SETTING };
        const merged = { ...current, ...patch };
        if (isDefaultSetting(merged)) delete next[id];
        else next[id] = merged;
      }
      return next;
    });
  }, []);

  // Only show the spinner on first load (before any layers are classified). Once
  // we have layers, never spin again — avoids flicker from styledata churn during
  // live edits (which transiently report the style as not loaded).
  const styleLoading =
    open && tab === "layers" && !!map && !map.isStyleLoaded() && basemapLayerInfos.length === 0;

  // --- Submit ---
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
        layer_config: Object.keys(layerConfig).length ? layerConfig : undefined,
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

  const toggleCategory = (cat: BasemapCategory) =>
    setExpandedCategories((prev) => {
      const next = new Set(prev);
      if (next.has(cat)) next.delete(cat);
      else next.add(cat);
      return next;
    });

  // Cancel/backdrop: clear the live-preview override synchronously (so it batches
  // with the parent's basemap restore in onClose — no one-frame stale-override
  // flicker), then close.
  const handleCancel = () => {
    dispatch(setBasemapLayerConfigOverride(undefined));
    onClose();
  };

  return (
    <Dialog open={open} onClose={handleCancel} fullWidth maxWidth="sm">
      <DialogTitle>{initial ? t("edit_basemap") : t("add_basemap")}</DialogTitle>
      <Box sx={{ borderBottom: 1, borderColor: "divider" }}>
        <Tabs value={tab} onChange={(_, v) => setTab(v)}>
          <Tab value="basemap" label={t("basemap_tab_label")} />
          <Tab value="solid" label={t("solid_color")} />
          {initial?.type === "vector" && (
            <Tab value="layers" label={t("basemap_layers_tab")} />
          )}
        </Tabs>
      </Box>
      <DialogContent
        sx={
          tab === "layers"
            ? { p: 0, display: "flex", flexDirection: "column", overflow: "hidden" }
            : { pt: 3 }
        }>
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

        {tab === "layers" && (
          <Box sx={{ display: "flex", flexDirection: "column", flex: 1, minHeight: 0 }}>
            {/* Fixed header: description + reset + search (does not scroll) */}
            <Box
              sx={{
                flexShrink: 0,
                px: 2.5,
                pt: 2,
                pb: 1.5,
                borderBottom: 1,
                borderColor: "divider",
              }}>
              <Stack
                direction="row"
                alignItems="flex-start"
                justifyContent="space-between"
                sx={{ mb: 1 }}>
                <Typography variant="body2" color="text.secondary" sx={{ flex: 1, pr: 2 }}>
                  {t("basemap_layers_description")}
                </Typography>
                <Button
                  size="small"
                  variant="text"
                  onClick={() => setLayerConfig({})}
                  sx={{ flexShrink: 0, fontSize: "0.75rem" }}>
                  {t("basemap_layer_reset")}
                </Button>
              </Stack>
              {(basemapLayerInfos.length > 0 || hasSearch) && (
                <TextField
                  size="small"
                  fullWidth
                  value={layerSearch}
                  onChange={(e) => setLayerSearch(e.target.value)}
                  placeholder={t("basemap_layer_search")}
                  InputProps={{
                    startAdornment: (
                      <InputAdornment position="start">
                        <SearchIcon fontSize="small" sx={{ color: "text.secondary" }} />
                      </InputAdornment>
                    ),
                  }}
                />
              )}
            </Box>

            {/* Scrolling body: only this area shows the vertical scrollbar */}
            <Box sx={{ flex: 1, minHeight: 0, overflowY: "auto" }}>
            {styleLoading ? (
              <Box sx={{ display: "flex", justifyContent: "center", py: 4 }}>
                <CircularProgress size={32} />
              </Box>
            ) : (
              <Box sx={{ pb: 1 }}>
                {CATEGORY_ORDER.map((cat) => {
                  const infos = layersByCategory[cat];
                  if (!infos || infos.length === 0) return null;
                  const catIds = infos.map((i) => i.id);
                  const isExpanded = expandedCategories.has(cat) || hasSearch;
                  const catTitle = t(`basemap_layer_categories.${cat}.title`);
                  const catSubtitle = t(`basemap_layer_categories.${cat}.subtitle`);
                  const catHidden = catIds.every((id) => !settingFor(id).visible);
                  const catVisible = catIds.filter((id) => settingFor(id).visible).length;

                  return (
                    <Box key={cat}>
                      <Divider />
                      <Box sx={{ px: 2.5, py: 0.5 }}>
                        {/* Category header row */}
                        <Stack direction="row" alignItems="center" spacing={1.75}>
                          <IconButton
                            size="small"
                            onClick={() => toggleCategory(cat)}
                            sx={{ flexShrink: 0 }}>
                            {isExpanded ? (
                              <ExpandLessIcon fontSize="small" />
                            ) : (
                              <ExpandMoreIcon fontSize="small" />
                            )}
                          </IconButton>
                          <Box sx={{ opacity: catHidden ? 0.5 : 1 }}>
                            <CategoryIcon category={cat} />
                          </Box>
                          <Box sx={{ flex: 1, minWidth: 0, opacity: catHidden ? 0.5 : 1 }}>
                            <Typography
                              variant="body2"
                              fontWeight="medium"
                              noWrap
                              color={catHidden ? "text.disabled" : undefined}
                              sx={{ lineHeight: 1.3 }}>
                              {catTitle}
                            </Typography>
                            <Typography
                              variant="caption"
                              color="text.secondary"
                              noWrap
                              sx={{ display: "block", lineHeight: 1.2 }}>
                              {catSubtitle} · {catVisible}/{infos.length}
                            </Typography>
                          </Box>
                          <LayerControls
                            ids={catIds}
                            settingFor={settingFor}
                            displayTarget={displayTarget}
                            setSetting={setSetting}
                            projectLayers={projectLayers}
                            t={t}
                          />
                        </Stack>

                        {/* Per-layer rows — tree line follows the children */}
                        <Collapse in={isExpanded} unmountOnExit>
                          <Stack
                            spacing={0.5}
                            sx={{
                              mt: 0.5,
                              ml: 2,
                              pl: 3,
                              borderLeft: "1px solid",
                              borderColor: "divider",
                            }}>
                            {infos.map((info) => {
                              const layerHidden = !settingFor(info.id).visible;
                              return (
                              <Stack
                                key={info.id}
                                direction="row"
                                alignItems="center"
                                spacing={1}
                                sx={{
                                  py: 0.25,
                                  position: "relative",
                                  // Horizontal connector from the vertical tree line to the row.
                                  "&::before": {
                                    content: '""',
                                    position: "absolute",
                                    left: (theme) => `-${theme.spacing(3)}`,
                                    width: (theme) => theme.spacing(2),
                                    top: "50%",
                                    borderTop: "1px solid",
                                    borderColor: "divider",
                                  },
                                }}>
                                <Tooltip title={info.id} placement="top-start" enterDelay={400}>
                                  <Box sx={{ flex: 1, minWidth: 0, pr: 1 }}>
                                    <Typography
                                      variant="body2"
                                      noWrap
                                      color={layerHidden ? "text.disabled" : undefined}>
                                      {info.prettyName}
                                    </Typography>
                                  </Box>
                                </Tooltip>
                                <LayerControls
                                  ids={[info.id]}
                                  compact
                                  settingFor={settingFor}
                                  displayTarget={displayTarget}
                                  setSetting={setSetting}
                                  projectLayers={projectLayers}
                                  t={t}
                                />
                              </Stack>
                              );
                            })}
                          </Stack>
                        </Collapse>
                      </Box>
                    </Box>
                  );
                })}

                {!hasVisibleLayers && (
                  <Typography
                    variant="body2"
                    color="text.secondary"
                    sx={{ textAlign: "center", py: 4 }}>
                    {hasSearch ? t("basemap_layer_no_matches") : t("basemap_layer_no_layers")}
                  </Typography>
                )}
              </Box>
            )}
            </Box>
          </Box>
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
          <Button onClick={handleCancel} variant="text" sx={{ borderRadius: 0 }}>
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
