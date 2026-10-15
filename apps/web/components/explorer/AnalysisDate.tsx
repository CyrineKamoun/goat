/**
 * The Explorer's one date (02-panel-behaviour-spec › Viewers), set in the
 * bottom bar: a pill with the day, a step to the day before and after, and
 * on a click the platform's date picker — as the toolbox asks for a date
 * (DateInput) — bounded to the feed's service window. The panels show it, as
 * a pill on each section its figures depend on.
 */
import { Box, ButtonBase, IconButton, Popover, Stack, Typography } from "@mui/material";
import { useState } from "react";
import { useTranslation } from "react-i18next";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";
import TemporalPicker from "@p4b/ui/components/TemporalPicker";

import { useDayjsLocale } from "@/i18n/utils";

import { shortDate } from "@/lib/explorer/format";
import { setExplorerDate } from "@/lib/store/explorer/slice";

import { useAppDispatch, useAppSelector } from "@/hooks/store/ContextHooks";

import { useExplorer } from "@/components/explorer/ExplorerContext";

/** `YYYY-MM-DD` moved by `days`. */
function addDays(date: string, days: number): string {
  const d = new Date(`${date}T00:00:00Z`);
  d.setUTCDate(d.getUTCDate() + days);
  return d.toISOString().slice(0, 10);
}

export function AnalysisDate() {
  const { t, i18n } = useTranslation("common");
  const dispatch = useAppDispatch();
  const locale = useDayjsLocale();
  const date = useAppSelector((s) => s.explorer.date);
  const { window } = useExplorer();
  const [anchor, setAnchor] = useState<HTMLElement | null>(null);
  if (!date || !window) return null;
  const step = (days: number) => {
    const next = addDays(date, days);
    if (next >= window[0] && next <= window[1]) dispatch(setExplorerDate(next));
  };
  const stepSx = { width: 24, height: 24, color: "text.secondary" } as const;
  return (
    <Stack
      direction="row"
      alignItems="center"
      sx={{ height: 32, px: 0.25, borderRadius: "16px", bgcolor: "action.hover", flexShrink: 0 }}>
      <IconButton
        size="small"
        aria-label={t("explorer_day_before")}
        disabled={date <= window[0]}
        onClick={() => step(-1)}
        sx={stepSx}>
        <Icon iconName={ICON_NAME.CHEVRON_LEFT} style={{ fontSize: 10 }} />
      </IconButton>
      <ButtonBase
        onClick={(e) => setAnchor(e.currentTarget)}
        aria-label={t("explorer_pick_date")}
        sx={{
          gap: 0.75,
          px: 0.75,
          height: 28,
          borderRadius: "14px",
          "&:hover": { bgcolor: "action.selected" },
        }}>
        <Icon iconName={ICON_NAME.CALENDAR} style={{ fontSize: 12 }} htmlColor="inherit" />
        <Typography variant="body2" fontWeight={700} noWrap>
          {shortDate(date, i18n.language)}
        </Typography>
      </ButtonBase>
      <IconButton
        size="small"
        aria-label={t("explorer_day_after")}
        disabled={date >= window[1]}
        onClick={() => step(1)}
        sx={stepSx}>
        <Icon iconName={ICON_NAME.CHEVRON_RIGHT} style={{ fontSize: 10 }} />
      </IconButton>
      <Popover
        open={!!anchor}
        anchorEl={anchor}
        onClose={() => setAnchor(null)}
        anchorOrigin={{ vertical: "top", horizontal: "center" }}
        transformOrigin={{ vertical: "bottom", horizontal: "center" }}
        slotProps={{ paper: { sx: { mt: -1, borderRadius: 2 } } }}>
        <Box sx={{ p: 1.5, width: 220 }}>
          <TemporalPicker
            locale={locale}
            kind="date"
            bold
            value={date}
            onChange={(value) => {
              if (value) dispatch(setExplorerDate(value));
            }}
            min={window[0]}
            max={window[1]}
          />
        </Box>
      </Popover>
    </Stack>
  );
}
