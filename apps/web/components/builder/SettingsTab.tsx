import { Box, Button, FormControl, MenuItem, Select, Stack, Switch, Typography } from "@mui/material";
import { useEffect } from "react";
import { useTranslation } from "react-i18next";

import { languages } from "@/i18n/settings";

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

interface SettingsTabProps {
  settings: { [key: string]: unknown };
  onChange: (name: string, value: unknown) => void;
  onReset: () => void;
}

interface SettingGroup {
  group: string;
  label: string;
  options: { name: string; label: string }[];
}

const SettingsTab: React.FC<SettingsTabProps> = ({ settings, onChange, onReset }) => {
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
          <SettingsGroupHeader label={t("general", { defaultValue: "General" })} />
          <Stack spacing={2}>
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
            <FormControl size="small" fullWidth>
              <Typography variant="caption" color="text.secondary" sx={{ mb: 0.5 }}>
                {t("font", { defaultValue: "Font" })}
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
          </Stack>
        </Box>
      </Stack>
      <Stack>
        <Button onClick={onReset} fullWidth size="small" color="error">
          <Typography variant="body2" fontWeight="bold" color="inherit">
            {t("common:reset")}
          </Typography>
        </Button>
      </Stack>
    </Stack>
  );
};

export default SettingsTab;
