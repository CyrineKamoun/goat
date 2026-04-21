import { Box, Button, FormControl, Link, MenuItem, Select, Stack, Switch, Typography } from "@mui/material";
import { useEffect, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";

import WidgetColorPicker from "@/components/builder/widgets/common/WidgetColorPicker";

import { languages } from "@/i18n/settings";

import type { InteractionRule } from "@/lib/validations/interaction";
import type { BuilderPanelSchema, Project, ProjectLayer, ProjectLayerGroup } from "@/lib/validations/project";
import { builderConfigSchema } from "@/lib/validations/project";

import InteractionsModal, { InteractionsEntryButton } from "@/components/builder/InteractionsModal";
import SettingsGroupHeader from "@/components/builder/widgets/common/SettingsGroupHeader";

/** Google Fonts available for dashboard display */
const DASHBOARD_FONTS = [
  { label: "Mulish", value: "Mulish, sans-serif" },
  { label: "Roboto", value: "Roboto, sans-serif" },
  { label: "Open Sans", value: "'Open Sans', sans-serif" },
  { label: "Lato", value: "Lato, sans-serif" },
  { label: "Inter", value: "Inter, sans-serif" },
  { label: "Poppins", value: "Poppins, sans-serif" },
  { label: "Montserrat", value: "Montserrat, sans-serif" },
  { label: "Nunito", value: "Nunito, sans-serif" },
  { label: "Source Sans 3", value: "'Source Sans 3', sans-serif" },
  { label: "Raleway", value: "Raleway, sans-serif" },
  { label: "PT Sans", value: "'PT Sans', sans-serif" },
  { label: "Merriweather", value: "Merriweather, serif" },
  { label: "Playfair Display", value: "'Playfair Display', serif" },
  { label: "Lora", value: "Lora, serif" },
];

const GOAT_GREEN = "#2BB381";

interface SettingsTabProps {
  settings: { [key: string]: unknown };
  onChange: (name: string, value: unknown) => void;
  onReset: () => void;
  project?: Project;
  projectLayers?: ProjectLayer[];
  projectLayerGroups?: ProjectLayerGroup[];
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  onProjectUpdate?: (key: string, value: any, refresh?: boolean) => void;
}

interface SettingGroup {
  group: string;
  label: string;
  options: { name: string; label: string }[];
}

const SettingsTab: React.FC<SettingsTabProps> = ({
  settings,
  onChange,
  onReset,
  project,
  projectLayers = [],
  projectLayerGroups = [],
  onProjectUpdate,
}) => {
  const { t } = useTranslation("common");

  // Load all Google Fonts for dropdown preview
  useEffect(() => {
    const id = "dashboard-font-preview";
    if (document.getElementById(id)) return;
    const families = DASHBOARD_FONTS.map((f) => `family=${encodeURIComponent(f.label)}:wght@400;700`).join("&");
    const link = document.createElement("link");
    link.id = id;
    link.rel = "stylesheet";
    link.href = `https://fonts.googleapis.com/css2?${families}&display=swap`;
    document.head.appendChild(link);
  }, []);

  const [showInteractionsModal, setShowInteractionsModal] = useState(false);

  const builderConfig = useMemo(() => {
    const parsed = builderConfigSchema.safeParse(project?.builder_config);
    return parsed.success ? parsed.data : undefined;
  }, [project]);

  const interactions = useMemo(
    () => (builderConfig?.interactions ?? []) as InteractionRule[],
    [builderConfig]
  );

  const panels = useMemo(
    () => (builderConfig?.interface ?? []) as BuilderPanelSchema[],
    [builderConfig]
  );

  const handleInteractionsChange = (newInteractions: InteractionRule[]) => {
    if (!builderConfig) return;
    const updatedConfig = { ...builderConfig, interactions: newInteractions };
    onProjectUpdate?.("builder_config", updatedConfig);
  };

  const settingsGroups: SettingGroup[] = [
    {
      group: "map_tools",
      label: t("map_tools"),
      options: [
        { name: "location", label: t("location_search") },
        { name: "find_my_location", label: t("find_my_location") },
        { name: "scalebar", label: t("scalebar") },
        { name: "measure", label: t("measure") },
      ],
    },
    {
      group: "map_controls",
      label: t("controls"),
      options: [
        { name: "zoom_controls", label: t("zoom_controls") },
        { name: "basemap", label: t("basemap") },
      ],
    },
    {
      group: "map_view",
      label: t("view"),
      options: [
        { name: "fullscreen", label: t("fullscreen") },
        { name: "toolbar", label: t("toolbar") },
        { name: "project_info", label: t("project_info") },
      ],
    },
  ];

  return (
    <Stack direction="column" spacing={2} justifyContent="space-between" sx={{ p: 3 }}>
      <Stack direction="column">
        {settingsGroups.map((group) => (
          <Box key={group.group} sx={{ mb: 6 }}>
            <SettingsGroupHeader label={t(group.label)} />
            <Stack spacing={1}>
              {group.options.map(
                (option) =>
                  settings?.hasOwnProperty(option.name) && (
                    <Stack direction="row" alignItems="center" key={option.name}>
                      <Switch
                        name={option.name}
                        checked={settings[option.name] as boolean}
                        onChange={(event: React.ChangeEvent<HTMLInputElement>) =>
                          onChange(option.name, event.target.checked)
                        }
                      />
                      <Typography variant="body2" fontWeight="bold" color="textSecondary">
                        {option.label}
                      </Typography>
                    </Stack>
                  )
              )}
            </Stack>
          </Box>
        ))}
        <Box sx={{ mb: 6 }}>
          <SettingsGroupHeader label={t("branding")} />
          <Stack spacing={2}>
            {/* Font */}
            <FormControl size="small" fullWidth>
              <Typography variant="caption" color="text.secondary" sx={{ mb: 0.5 }}>
                {t("font")}
              </Typography>
              <Select
                value={(settings?.font_family as string) || "Mulish, sans-serif"}
                onChange={(e) => onChange("font_family", e.target.value)}
                renderValue={(selected) => {
                  const font = DASHBOARD_FONTS.find((f) => f.value === selected);
                  return font?.label ?? "Mulish";
                }}>
                {DASHBOARD_FONTS.map((font) => (
                  <MenuItem key={font.value} value={font.value}>
                    <Typography sx={{ fontFamily: font.value }}>{font.label}</Typography>
                  </MenuItem>
                ))}
              </Select>
            </FormControl>

            {/* Primary Color */}
            <Box>
              <Stack direction="row" alignItems="center" justifyContent="space-between" sx={{ mb: 0.5 }}>
                <Typography variant="caption" color="text.secondary">
                  {t("primary_color")}
                </Typography>
                {!!settings?.primary_color && (
                  <Link
                    component="button"
                    variant="caption"
                    underline="always"
                    onClick={() => onChange("primary_color", undefined)}
                    sx={{ cursor: "pointer" }}>
                    {t("reset")}
                  </Link>
                )}
              </Stack>
              <WidgetColorPicker
                color={(settings?.primary_color as string) || GOAT_GREEN}
                onChange={(color) => onChange("primary_color", color)}
              />
            </Box>

            {/* Language */}
            <FormControl size="small" fullWidth>
              <Typography variant="caption" color="text.secondary" sx={{ mb: 0.5 }}>
                {t("language")}
              </Typography>
              <Select
                value={(settings?.language as string) ?? "auto"}
                onChange={(e) => onChange("language", e.target.value)}>
                <MenuItem value="auto">{t("auto_browser_default")}</MenuItem>
                {languages.map((lng) => (
                  <MenuItem key={lng} value={lng}>
                    {lng === "en" ? "English" : lng === "de" ? "Deutsch" : lng}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
          </Stack>
        </Box>
        {project && onProjectUpdate && (
          <Box sx={{ mb: 6 }}>
            <SettingsGroupHeader label={t("interactions")} />
            <InteractionsEntryButton
              interactionCount={interactions.filter((r) => r.enabled).length}
              onClick={() => setShowInteractionsModal(true)}
            />
          </Box>
        )}
      </Stack>
      <Stack>
        <Button onClick={onReset} fullWidth size="small" color="error">
          <Typography variant="body2" fontWeight="bold" color="inherit">
            {t("common:reset")}
          </Typography>
        </Button>
      </Stack>
      {showInteractionsModal && builderConfig && (
        <InteractionsModal
          open={showInteractionsModal}
          onClose={() => setShowInteractionsModal(false)}
          interactions={interactions}
          onChange={handleInteractionsChange}
          panels={panels}
          projectLayers={projectLayers}
          projectLayerGroups={projectLayerGroups}
        />
      )}
    </Stack>
  );
};

export default SettingsTab;
