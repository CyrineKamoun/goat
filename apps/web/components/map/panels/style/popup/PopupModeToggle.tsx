import { ToggleButton, ToggleButtonGroup } from "@mui/material";
import { alpha } from "@mui/material/styles";
import { useTranslation } from "react-i18next";

export type PopupMode = "simple" | "html";

export function PopupModeToggle({
  value,
  onChange,
}: {
  value: PopupMode;
  onChange: (mode: PopupMode) => void;
}) {
  const { t } = useTranslation("common");
  return (
    <ToggleButtonGroup
      exclusive
      size="small"
      value={value}
      onChange={(_, v) => v && onChange(v)}
      sx={(theme) => ({
        // Pill-shaped primary-outlined toggle, mirroring the mockup.
        // Selected option fills with a soft primary tint; both options
        // share the primary text color so the affordance reads as a
        // single toggle rather than two unrelated buttons.
        border: `1px solid ${theme.palette.primary.main}`,
        borderRadius: 999,
        overflow: "hidden",
        "& .MuiToggleButton-root": {
          border: "none",
          borderRadius: 0,
          textTransform: "none",
          color: theme.palette.primary.main,
          fontSize: 11,
          fontWeight: 500,
          lineHeight: 1.3,
          py: 0.25,
          px: 1.5,
          whiteSpace: "nowrap",
          "&:hover": {
            backgroundColor: alpha(theme.palette.primary.main, 0.06),
          },
          "&.Mui-selected": {
            backgroundColor: alpha(theme.palette.primary.main, 0.14),
            color: theme.palette.primary.main,
            fontWeight: 600,
          },
          "&.Mui-selected:hover": {
            backgroundColor: alpha(theme.palette.primary.main, 0.22),
          },
          "&:not(:first-of-type)": {
            borderLeft: `1px solid ${theme.palette.primary.main}`,
          },
        },
      })}>
      <ToggleButton value="simple">{t("popup_mode_simple")}</ToggleButton>
      <ToggleButton value="html">{t("popup_mode_html")}</ToggleButton>
    </ToggleButtonGroup>
  );
}
