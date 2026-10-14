/**
 * The Explorer's one date (02-panel-behaviour-spec › Viewers), set in the
 * bottom bar with the platform's date picker — as the toolbox asks for a
 * date (DateInput) — bounded to the feed's service window. The panels show
 * it, as a pill on each section its figures depend on.
 */
import { Stack } from "@mui/material";

import TemporalPicker from "@p4b/ui/components/TemporalPicker";

import { useDayjsLocale } from "@/i18n/utils";

import { setExplorerDate } from "@/lib/store/explorer/slice";

import { useAppDispatch, useAppSelector } from "@/hooks/store/ContextHooks";

import { useExplorer } from "@/components/explorer/ExplorerContext";

export function AnalysisDate() {
  const dispatch = useAppDispatch();
  const locale = useDayjsLocale();
  const date = useAppSelector((s) => s.explorer.date);
  const { window } = useExplorer();
  if (!date || !window) return null;
  return (
    <Stack>
      <TemporalPicker
        locale={locale}
        kind="date"
        bold
        value={date}
        onChange={(value) => value && dispatch(setExplorerDate(value))}
        min={window[0]}
        max={window[1]}
      />
    </Stack>
  );
}
