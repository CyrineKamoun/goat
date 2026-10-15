/**
 * A route's Calendar tab (board 11): its services in plain words — the day
 * types it runs, with the dates each skips or adds — and month by month which
 * one runs. Exceptions are dotted; the Explorer's date is outlined, and a day
 * clicked becomes it.
 */
import { Box, ButtonBase, Stack, Typography } from "@mui/material";
import { alpha, useTheme } from "@mui/material/styles";
import { useEffect, useMemo, useRef } from "react";
import { useTranslation } from "react-i18next";

import { ICON_NAME } from "@p4b/ui/components/Icon";

import { type DayType, isoWeekday } from "@/lib/explorer/derive/serviceCalendar";
import { count, dayMonth, weekdayList, weekdayName } from "@/lib/explorer/format";
import { setExplorerDate } from "@/lib/store/explorer/slice";

import { type RouteRow, useRouteCalendar } from "@/hooks/explorer/useExplorerData";
import { useAppDispatch, useAppSelector } from "@/hooks/store/ContextHooks";

import { useExplorer } from "@/components/explorer/ExplorerContext";
import { SectionHeader } from "@/components/explorer/SectionHeader";

/** The first day types' colours, then as many more as a feed needs: hues a
 * golden angle apart, so each new one lands far from those before it. */
const TYPE_COLORS = ["#2E7D32", "#1565C0", "#8E24AA", "#EF6C00", "#00838F"];
function typeColor(i: number): string {
  return TYPE_COLORS[i] ?? `hsl(${Math.round((i * 137.508 + 40) % 360)}, 55%, 42%)`;
}
/** Dates listed before "and n more". */
const LISTED = 3;

function useDates() {
  const { t, i18n } = useTranslation("common");
  return (dates: string[]) => {
    const shown = dates.slice(0, LISTED).map((d) => dayMonth(d, i18n.language));
    const rest = dates.length - shown.length;
    return rest > 0 ? `${shown.join(", ")} ${t("explorer_and_more", { count: rest })}` : shown.join(", ");
  };
}

function TypeRow({ type, color }: { type: DayType; color: string }) {
  const { t, i18n } = useTranslation("common");
  const theme = useTheme();
  const dates = useDates();
  const lang = i18n.language;
  const words = !type.weekdays.length
    ? t("explorer_on_days", { count: type.days.length })
    : (weekdayList(type.weekdays, lang) ?? t("explorer_daily"));
  return (
    <Stack direction="row" spacing={1.25} alignItems="flex-start" sx={{ py: 0.75 }}>
      <Box
        sx={{
          mt: "3px",
          width: 12,
          height: 12,
          borderRadius: 0.75,
          flexShrink: 0,
          bgcolor: alpha(color, 0.25),
          border: `1.5px solid ${color}`,
        }}
      />
      <Box sx={{ flex: 1, minWidth: 0 }}>
        <Typography variant="body2" fontWeight={600} sx={{ lineHeight: 1.4 }}>
          {words}
          {!!type.weekdays.length && (
            <Typography component="span" variant="caption" color="text.secondary">
              {" · "}
              {t("explorer_n_days", { count: type.days.length })}
            </Typography>
          )}
        </Typography>
        {/* What the calendar changes: amber, as the board marks exceptions. */}
        {type.weekdays.length > 0 && type.except.length > 0 && (
          <Typography
            variant="caption"
            display="block"
            sx={{ color: theme.palette.warning.dark, lineHeight: 1.4 }}>
            {t("explorer_except_dates", { dates: dates(type.except) })}
          </Typography>
        )}
        {type.weekdays.length > 0 && type.adds.length > 0 && (
          <Typography
            variant="caption"
            display="block"
            sx={{ color: theme.palette.warning.dark, lineHeight: 1.4 }}>
            {t("explorer_adds_dates", { dates: dates(type.adds) })}
          </Typography>
        )}
        {!type.weekdays.length && (
          <Typography variant="caption" color="text.secondary" display="block" sx={{ lineHeight: 1.4 }}>
            {dates(type.days)}
          </Typography>
        )}
      </Box>
      <Typography variant="caption" color="text.secondary" sx={{ flexShrink: 0, mt: "2px" }}>
        {t("explorer_trips_a_day", { count: type.trips })}
      </Typography>
    </Stack>
  );
}

function Month({
  year,
  month,
  dayColor,
  exceptions,
  selected,
  onPick,
  monthRef,
}: {
  year: number;
  month: number;
  dayColor: Map<string, string | null>;
  exceptions: Set<string>;
  selected: string | null;
  onPick: (date: string) => void;
  monthRef?: React.Ref<HTMLDivElement>;
}) {
  const { i18n } = useTranslation("common");
  const theme = useTheme();
  const lang = i18n.language;
  const first = new Date(Date.UTC(year, month, 1));
  const length = new Date(Date.UTC(year, month + 1, 0)).getUTCDate();
  const lead = isoWeekday(first.toISOString().slice(0, 10)) - 1;
  const title = new Intl.DateTimeFormat(lang, { month: "long", year: "numeric", timeZone: "UTC" }).format(
    first
  );
  return (
    <Box ref={monthRef} sx={{ mb: 2 }}>
      <Typography variant="body2" fontWeight={700} sx={{ mb: 0.75 }}>
        {title}
      </Typography>
      <Box sx={{ display: "grid", gridTemplateColumns: "repeat(7, 1fr)", gap: "3px" }}>
        {[1, 2, 3, 4, 5, 6, 7].map((w) => (
          <Typography
            key={w}
            variant="caption"
            color="text.secondary"
            sx={{ textAlign: "center", fontSize: 10 }}>
            {weekdayName(w, lang, "narrow")}
          </Typography>
        ))}
        {Array.from({ length: lead }, (_, i) => (
          <Box key={`lead${i}`} />
        ))}
        {Array.from({ length }, (_, i) => {
          const date = `${year}-${String(month + 1).padStart(2, "0")}-${String(i + 1).padStart(2, "0")}`;
          const inWindow = dayColor.has(date);
          const color = dayColor.get(date) ?? null;
          const isSelected = date === selected;
          return (
            <ButtonBase
              key={date}
              disabled={!inWindow}
              onClick={() => onPick(date)}
              aria-label={date}
              aria-pressed={isSelected}
              sx={{
                position: "relative",
                aspectRatio: "1",
                borderRadius: 1,
                fontSize: 11,
                fontWeight: isSelected ? 700 : 500,
                color: inWindow ? (color ? "text.primary" : "text.secondary") : "text.disabled",
                bgcolor: color ? alpha(color, 0.18) : "transparent",
                border: isSelected ? `2px solid ${theme.palette.text.primary}` : "2px solid transparent",
                "&:hover": { bgcolor: color ? alpha(color, 0.3) : "action.hover" },
              }}>
              {i + 1}
              {exceptions.has(date) && (
                <Box
                  sx={{
                    position: "absolute",
                    bottom: 3,
                    width: 4,
                    height: 4,
                    borderRadius: "50%",
                    bgcolor: theme.palette.warning.main,
                  }}
                />
              )}
            </ButtonBase>
          );
        })}
      </Box>
    </Box>
  );
}

export function RouteCalendar({ route }: { route: RouteRow }) {
  const { t, i18n } = useTranslation("common");
  const theme = useTheme();
  const dispatch = useAppDispatch();
  const { db, window } = useExplorer();
  const selected = useAppSelector((s) => s.explorer.date);
  const calendar = useRouteCalendar(db, route, window);
  const selectedMonth = useRef<HTMLDivElement>(null);
  const lang = i18n.language;
  const data = calendar.data;

  const { dayColor, exceptions } = useMemo(() => {
    const colorOf = new Map((data?.types ?? []).map((ty, i) => [ty.key, typeColor(i)]));
    const dayColor = new Map<string, string | null>(
      (data?.days ?? []).map((d) => [d.date, d.key ? (colorOf.get(d.key) ?? null) : null])
    );
    const exceptions = new Set((data?.days ?? []).filter((d) => d.exception).map((d) => d.date));
    return { dayColor, exceptions };
  }, [data]);

  // Open on the month of the Explorer's date.
  useEffect(() => {
    selectedMonth.current?.scrollIntoView({ block: "nearest" });
  }, [data]);

  if (!window || !data) {
    return (
      <Typography variant="body2" color="text.secondary" sx={{ mt: 2 }}>
        {calendar.error ?? t("explorer_calculating")}
      </Typography>
    );
  }

  const months: [number, number][] = [];
  const [y0, m0] = window[0].split("-").map(Number);
  const [y1, m1] = window[1].split("-").map(Number);
  for (let y = y0, m = m0 - 1; y < y1 || (y === y1 && m <= m1 - 1); m === 11 ? (y++, (m = 0)) : m++)
    months.push([y, m]);
  const selectedKey = selected?.slice(0, 7);

  return (
    <Box sx={{ pt: 1, pb: 2 }}>
      <SectionHeader px={0} icon={ICON_NAME.CALENDAR} label={t("explorer_services")} />
      <Box sx={{ bgcolor: "action.hover", borderRadius: 1.5, px: 1.5, py: 0.5 }}>
        {data.types.map((ty, i) => (
          <TypeRow key={ty.key} type={ty} color={typeColor(i)} />
        ))}
        {!data.types.length && (
          <Typography variant="body2" color="text.secondary" sx={{ py: 1 }}>
            {t("explorer_no_service_window")}
          </Typography>
        )}
      </Box>
      <Typography variant="caption" color="text.secondary" display="block" sx={{ mt: 1 }}>
        {t("explorer_calendar_source", {
          first: dayMonth(window[0], lang),
          last: `${dayMonth(window[1], lang)} ${window[1].slice(0, 4)}`,
          services: count(data.from_calendar, lang),
          exceptions: count(data.exceptions, lang),
        })}
      </Typography>

      <Box sx={{ mt: 2 }}>
        <SectionHeader
          px={0}
          icon={ICON_NAME.CLOCK}
          label={t("explorer_which_service_runs")}
          aside={
            <Stack direction="row" spacing={0.5} alignItems="center">
              <Box sx={{ width: 6, height: 6, borderRadius: "50%", bgcolor: theme.palette.warning.main }} />
              <Typography variant="caption" color="text.secondary">
                {t("explorer_exception")}
              </Typography>
            </Stack>
          }
        />
      </Box>
      {months.map(([y, m]) => {
        const key = `${y}-${String(m + 1).padStart(2, "0")}`;
        return (
          <Month
            key={key}
            year={y}
            month={m}
            dayColor={dayColor}
            exceptions={exceptions}
            selected={selected}
            onPick={(date) => dispatch(setExplorerDate(date))}
            monthRef={key === selectedKey ? selectedMonth : undefined}
          />
        );
      })}
    </Box>
  );
}
