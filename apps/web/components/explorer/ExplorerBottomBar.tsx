/**
 * The Explorer's bottom bar (boards 3, 6, 8; 02-panel-behaviour-spec ›
 * Playback): one row that plays the network through its day — play/pause,
 * the speed, the date, the time, a scrubber over the day's service, how many
 * vehicles are out — and Exit Explorer, the only way out. With no changes to
 * save, it closes the Explorer straight away.
 */
import LogoutIcon from "@mui/icons-material/Logout";
import PauseIcon from "@mui/icons-material/Pause";
import PlayArrowIcon from "@mui/icons-material/PlayArrow";
import { Button, ButtonBase, Divider, IconButton, Slider, Stack, Tooltip, Typography } from "@mui/material";
import { useEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";

import { clock, count } from "@/lib/explorer/format";
import {
  PLAYBACK_SPEEDS,
  closeExplorer,
  setPlaybackSpeed,
  setPlaybackTime,
  setPlaying,
} from "@/lib/store/explorer/slice";

import { useRoutes } from "@/hooks/explorer/useExplorerData";
import { usePlayback } from "@/hooks/explorer/usePlayback";
import { usePlaybackValue } from "@/hooks/explorer/usePlaybackValue";
import { useAppDispatch, useAppSelector } from "@/hooks/store/ContextHooks";

import { FloatingPanel } from "@/components/common/FloatingPanel";
import { AnalysisDate } from "@/components/explorer/AnalysisDate";
import { useExplorer } from "@/components/explorer/ExplorerContext";

const HOUR = 3600;
/** How near the drawn time must come to where the scrubber was let go for
 * the thumb to follow it again, and how long it waits at most. */
const SETTLED_S = 120;
const SETTLE_MS = 2000;
/** Extra room (px) beside the round buttons at the bar's two ends. */
const END_ROOM = 8;

/** `compact`: squeezed between the side panels, the bar keeps one row with
 * less of each (02-panel-behaviour-spec › Viewers): one speed button that
 * cycles, the date without its day steps, no vehicle count, and Exit as its
 * icon alone. */
export function ExplorerBottomBar({ compact = false }: { compact?: boolean }) {
  const { t } = useTranslation("common");
  const dispatch = useAppDispatch();
  const { db, complete } = useExplorer();
  const routes = useRoutes(db);
  const playing = useAppSelector((s) => s.explorer.playing);
  const speed = useAppSelector((s) => s.explorer.speed);
  const memory = usePlayback(complete ? db : undefined, routes.data);
  // Not every frame: the span, and the minute the clock and the scrubber
  // show (a scrubber step is a minute). What changes every frame, the
  // vehicle count, renders on its own (VehicleCount).
  const span = usePlaybackValue((f) => f.span);
  const minute = usePlaybackValue((f) => (f.time_s === null ? null : Math.floor(f.time_s / 60) * 60));
  // While the scrubber is dragged only the clock follows it; the map is
  // drawn for the time it is let go at, and playback, if it ran, goes on
  // from there. Let go, the thumb stays where it was left until the map has
  // caught up, rather than flicking back to the time last drawn.
  const [drag, setDrag] = useState<{ t: number; let_go: boolean } | null>(null);
  const resume = useRef(false);
  useEffect(() => {
    if (!drag?.let_go) return;
    if (minute !== null && Math.abs(minute - drag.t) <= SETTLED_S) {
      setDrag(null);
      return;
    }
    // A frame that never comes (a window that failed to read) does not hold the thumb.
    const timer = window.setTimeout(() => setDrag(null), SETTLE_MS);
    return () => window.clearTimeout(timer);
  }, [drag, minute]);
  const time = drag?.t ?? minute;
  const ready = complete && span !== null && time !== null;

  const ticks: { value: number }[] = [];
  if (span)
    for (let h = Math.ceil(span.first_s / HOUR / 3) * 3; h * HOUR <= span.last_s; h += 3)
      ticks.push({ value: h * HOUR });

  return (
    // The bar spans the room the layout gives it (DataProjectLayout). One
    // row: how to play · the day and the time in it · what is out · the way
    // out. Fully rounded to follow the round buttons at its ends, with as
    // much room beside them as above and below.
    <FloatingPanel minHeight={0} sx={{ width: "100%", borderRadius: "28px" }}>
      <Stack direction="row" alignItems="center" spacing={1.5} sx={{ px: "10px", height: 56 }}>
        {/* Short of memory, playback is off: the rest of the Explorer comes
            first. Play leads the bar, with a little more room after it, as
            Exit Explorer has before it. */}
        <Tooltip title={memory.ok ? "" : t(`explorer_playback_memory_${memory.reason}`)}>
          <span style={{ marginRight: END_ROOM }}>
            <IconButton
              aria-label={playing ? t("explorer_pause") : t("explorer_play")}
              disabled={!ready || (!memory.ok && !playing)}
              onClick={() => dispatch(setPlaying(!playing))}
              sx={{
                width: 36,
                height: 36,
                flexShrink: 0,
                bgcolor: "primary.main",
                color: "primary.contrastText",
                "&:hover": { bgcolor: "primary.dark" },
                "&.Mui-disabled": { bgcolor: "action.disabledBackground" },
              }}>
              {playing ? <PauseIcon fontSize="small" /> : <PlayArrowIcon fontSize="small" />}
            </IconButton>
          </span>
        </Tooltip>
        {/* Speed: every choice in view, the one in use raised (board 3);
            compact, one button stepping through them. */}
        {compact ? (
          <Tooltip title={t("explorer_speed")}>
            <ButtonBase
              aria-label={t("explorer_speed")}
              onClick={() =>
                dispatch(
                  setPlaybackSpeed(
                    PLAYBACK_SPEEDS[(PLAYBACK_SPEEDS.indexOf(speed) + 1) % PLAYBACK_SPEEDS.length]
                  )
                )
              }
              sx={{
                height: 32,
                minWidth: 48,
                px: 1,
                borderRadius: "16px",
                bgcolor: "action.hover",
                fontSize: 12,
                fontWeight: 700,
                fontVariantNumeric: "tabular-nums",
                flexShrink: 0,
              }}>
              {speed}×
            </ButtonBase>
          </Tooltip>
        ) : (
          <Stack
            direction="row"
            role="radiogroup"
            aria-label={t("explorer_speed")}
            sx={{ p: 0.25, borderRadius: "16px", bgcolor: "action.hover", flexShrink: 0 }}>
            {PLAYBACK_SPEEDS.map((s) => (
              <ButtonBase
                key={s}
                role="radio"
                aria-checked={s === speed}
                onClick={() => dispatch(setPlaybackSpeed(s))}
                sx={{
                  height: 28,
                  minWidth: 44,
                  px: 1,
                  borderRadius: "14px",
                  fontSize: 12,
                  fontWeight: 700,
                  fontVariantNumeric: "tabular-nums",
                  color: s === speed ? "text.primary" : "text.secondary",
                  bgcolor: s === speed ? "background.paper" : "transparent",
                  boxShadow: s === speed ? "0 1px 2px rgba(0, 0, 0, 0.12)" : "none",
                }}>
                {s}×
              </ButtonBase>
            ))}
          </Stack>
        )}
        <Divider orientation="vertical" flexItem sx={{ my: 1.5 }} />
        <AnalysisDate compact={compact} />
        {/* The time the scrubber beside it stands at; past midnight the clock
            counts on, as a timetable does (24:48). */}
        <Typography
          fontWeight={700}
          sx={{ fontSize: 18, fontVariantNumeric: "tabular-nums", width: 52, flexShrink: 0 }}>
          {time !== null ? clock(time) : "--:--"}
        </Typography>
        <Slider
          size="small"
          aria-label={t("explorer_playback_time")}
          disabled={!ready}
          min={span?.first_s ?? 0}
          max={span?.last_s ?? 1}
          step={60}
          marks={ticks}
          value={time ?? 0}
          // The clock beside the play button reads the dragged time.
          valueLabelDisplay="off"
          // Playback holds while the scrubber is dragged, and resumes from
          // where it is let go.
          onChange={(_e, v) => {
            if (!drag || drag.let_go) {
              resume.current = playing;
              if (playing) dispatch(setPlaying(false));
            }
            setDrag({ t: v as number, let_go: false });
          }}
          onChangeCommitted={(_e, v) => {
            setDrag({ t: v as number, let_go: true });
            dispatch(setPlaybackTime(v as number));
            if (resume.current && memory.ok) dispatch(setPlaying(true));
            resume.current = false;
          }}
          sx={{
            flex: 1,
            minWidth: compact ? 48 : 120,
            mx: 1,
            "& .MuiSlider-rail": { opacity: 1, bgcolor: "action.selected" },
            "& .MuiSlider-mark": { width: 2, height: 2, borderRadius: "50%", bgcolor: "text.disabled" },
            "& .MuiSlider-thumb": { width: 14, height: 14 },
            // Dragged, the thumb is under the pointer: no easing behind it.
            ...(drag && { "& .MuiSlider-thumb, & .MuiSlider-track": { transition: "none" } }),
          }}
        />
        {!compact && <Divider orientation="vertical" flexItem sx={{ my: 1.5 }} />}
        {!compact && <VehicleCount ready={ready} />}
        {compact ? (
          <Tooltip title={t("explorer_exit")}>
            <IconButton
              aria-label={t("explorer_exit")}
              onClick={() => dispatch(closeExplorer())}
              // Inline, as the play button's: the row's spacing resets margins set in sx.
              style={{ marginLeft: END_ROOM }}
              sx={{
                width: 36,
                height: 36,
                flexShrink: 0,
                bgcolor: "primary.main",
                color: "primary.contrastText",
                "&:hover": { bgcolor: "primary.dark" },
              }}>
              <LogoutIcon fontSize="small" />
            </IconButton>
          </Tooltip>
        ) : (
          <Button
            variant="contained"
            color="primary"
            size="small"
            startIcon={<LogoutIcon fontSize="small" />}
            onClick={() => dispatch(closeExplorer())}
            sx={{
              textTransform: "none",
              whiteSpace: "nowrap",
              flexShrink: 0,
              borderRadius: "18px",
              height: 36,
              px: 2,
            }}>
            {t("explorer_exit")}
          </Button>
        )}
      </Stack>
    </FloatingPanel>
  );
}

/** Vehicles out at the time: the figure bold, its unit quiet, as the left
 * panel's figures are; the figure's width fixed so it does not jostle the bar
 * as it plays. Apart from the bar, as it changes with every frame. */
function VehicleCount({ ready }: { ready: boolean }) {
  const { t, i18n } = useTranslation("common");
  const n = usePlaybackValue((f) => f.vehicles.n);
  return (
    <Stack
      direction="row"
      alignItems="baseline"
      spacing={0.5}
      // Inline, as the play button's: the row's spacing resets margins set in sx.
      style={{ marginRight: END_ROOM }}
      sx={{ flexShrink: 0, visibility: ready ? "visible" : "hidden" }}>
      <Typography
        variant="body2"
        fontWeight={700}
        sx={{ fontVariantNumeric: "tabular-nums", minWidth: "5ch", textAlign: "right" }}>
        {count(n, i18n.language)}
      </Typography>
      <Typography variant="caption" color="text.secondary">
        {t("explorer_vehicles_out", { count: n })}
      </Typography>
    </Stack>
  );
}
