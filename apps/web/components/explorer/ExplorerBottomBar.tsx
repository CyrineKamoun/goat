/**
 * The Explorer's bottom bar (boards 3, 6, 8; 02-panel-behaviour-spec ›
 * Playback): one row that plays the network through its day — play/pause,
 * the time, the speed, the date, a scrubber over the day's service, how many
 * vehicles are out — and Finish editing, the only way out. With no changes to
 * save, Finish editing closes the Explorer straight away.
 */
import LogoutIcon from "@mui/icons-material/Logout";
import PauseIcon from "@mui/icons-material/Pause";
import PlayArrowIcon from "@mui/icons-material/PlayArrow";
import { Box, Button, ButtonBase, IconButton, Slider, Stack, Tooltip, Typography } from "@mui/material";
import { useSyncExternalStore } from "react";
import { useTranslation } from "react-i18next";

import { clock, count } from "@/lib/explorer/format";
import { playbackClock } from "@/lib/explorer/playback";
import {
  PLAYBACK_SPEEDS,
  closeExplorer,
  setPlaybackSpeed,
  setPlaybackTime,
  setPlaying,
} from "@/lib/store/explorer/slice";

import { useRoutes } from "@/hooks/explorer/useExplorerData";
import { usePlayback } from "@/hooks/explorer/usePlayback";
import { useAppDispatch, useAppSelector } from "@/hooks/store/ContextHooks";

import { FloatingPanel } from "@/components/common/FloatingPanel";
import { AnalysisDate } from "@/components/explorer/AnalysisDate";
import { useExplorer } from "@/components/explorer/ExplorerContext";

const HOUR = 3600;

/** A pill-shaped control on the bar's tinted ground. */
const pillSx = {
  height: 32,
  px: 1.25,
  borderRadius: 2,
  bgcolor: "action.hover",
  fontSize: 13,
  fontWeight: 700,
  fontVariantNumeric: "tabular-nums",
  flexShrink: 0,
  "&:hover": { bgcolor: "action.selected" },
} as const;

export function ExplorerBottomBar() {
  const { t, i18n } = useTranslation("common");
  const dispatch = useAppDispatch();
  const { db, complete } = useExplorer();
  const routes = useRoutes(db);
  const playing = useAppSelector((s) => s.explorer.playing);
  const speed = useAppSelector((s) => s.explorer.speed);
  const memory = usePlayback(complete ? db : undefined, routes.data);
  const frame = useSyncExternalStore(
    playbackClock.subscribe,
    playbackClock.getSnapshot,
    playbackClock.getSnapshot
  );
  const span = frame.span;
  const time = frame.time_s;
  const ready = complete && span !== null && time !== null;

  const ticks: { value: number }[] = [];
  if (span)
    for (let h = Math.ceil(span.first_s / HOUR / 3) * 3; h * HOUR <= span.last_s; h += 3)
      ticks.push({ value: h * HOUR });

  return (
    // The bar spans the room the layout gives it (DataProjectLayout).
    <FloatingPanel minHeight={0} sx={{ width: "100%" }}>
      <Stack direction="row" spacing={1.5} alignItems="center" sx={{ px: 1.5, py: 1 }}>
        {/* Short of memory, playback is off: the rest of the Explorer comes first. */}
        <Tooltip title={memory.ok ? "" : t(`explorer_playback_memory_${memory.reason}`)}>
          <span>
            <IconButton
              aria-label={playing ? t("explorer_pause") : t("explorer_play")}
              disabled={!ready || (!memory.ok && !playing)}
              onClick={() => dispatch(setPlaying(!playing))}
              sx={{
                width: 40,
                height: 40,
                flexShrink: 0,
                bgcolor: "primary.main",
                color: "primary.contrastText",
                "&:hover": { bgcolor: "primary.dark" },
                "&.Mui-disabled": { bgcolor: "action.disabledBackground" },
              }}>
              {playing ? <PauseIcon /> : <PlayArrowIcon />}
            </IconButton>
          </span>
        </Tooltip>
        {/* Past midnight the clock counts on, as a timetable does (24:48). */}
        <Typography
          fontWeight={700}
          sx={{ fontSize: 22, fontVariantNumeric: "tabular-nums", width: 64, flexShrink: 0 }}>
          {time !== null ? clock(time) : "--:--"}
        </Typography>
        <ButtonBase
          aria-label={t("explorer_speed")}
          title={t("explorer_speed")}
          onClick={() =>
            dispatch(
              setPlaybackSpeed(PLAYBACK_SPEEDS[(PLAYBACK_SPEEDS.indexOf(speed) + 1) % PLAYBACK_SPEEDS.length])
            )
          }
          sx={pillSx}>
          {speed}×
        </ButtonBase>
        <Box sx={{ width: 150, flexShrink: 0 }}>
          <AnalysisDate />
        </Box>
        <Slider
          size="small"
          aria-label={t("explorer_playback_time")}
          disabled={!ready}
          min={span?.first_s ?? 0}
          max={span?.last_s ?? 1}
          step={60}
          marks={ticks}
          value={time ?? 0}
          valueLabelDisplay="auto"
          valueLabelFormat={(v) => clock(v)}
          // Dragging stops playback: the time is the user's then.
          onChange={(_e, v) => {
            if (playing) dispatch(setPlaying(false));
            dispatch(setPlaybackTime(v as number));
          }}
          sx={{ flex: 1, minWidth: 120, mx: 1 }}
        />
        <Typography variant="body2" color="text.secondary" noWrap sx={{ flexShrink: 0, minWidth: 92 }}>
          {ready
            ? t("explorer_vehicles_running", {
                count: frame.vehicles.features.length,
                n: count(frame.vehicles.features.length, i18n.language),
              })
            : ""}
        </Typography>
        <Button
          variant="contained"
          color="primary"
          startIcon={<LogoutIcon fontSize="small" />}
          onClick={() => dispatch(closeExplorer())}
          sx={{ textTransform: "none", whiteSpace: "nowrap", flexShrink: 0 }}>
          {t("explorer_finish_editing")}
        </Button>
      </Stack>
    </FloatingPanel>
  );
}
