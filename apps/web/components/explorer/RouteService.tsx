/**
 * A route's Service tab (board 22), read-only for now: per direction, how
 * often it runs in each time period, from when to when, and how long a trip
 * takes. In the editing phase these become the fields that change service;
 * the layout is theirs already.
 */
import { Box, Stack, Typography } from "@mui/material";
import type { ReactNode } from "react";
import { useTranslation } from "react-i18next";

import { ICON_NAME } from "@p4b/ui/components/Icon";

import { clock } from "@/lib/explorer/format";

import type { RouteSummaryData } from "@/hooks/explorer/useExplorerData";
import { useAppSelector } from "@/hooks/store/ContextHooks";

import { useExplorer } from "@/components/explorer/ExplorerContext";
import { EditPeriodsButton } from "@/components/explorer/ExplorerSettingsDialog";
import { DateChip, SectionHeader } from "@/components/explorer/SectionHeader";
import { directionLabel, directionPlace } from "@/components/explorer/directionWords";

const COL = 76;

/** A period × direction table on the tinted ground, as the Overview's. */
function PeriodTable({
  data,
  cell,
}: {
  data: RouteSummaryData;
  cell: (d: RouteSummaryData["directions"][number], period: string) => { text: string; muted: boolean };
}) {
  const { t } = useTranslation("common");
  const { periods } = useExplorer().settings;
  const cols = data.directions;
  return (
    <Box sx={{ bgcolor: "action.hover", borderRadius: 1.5, overflow: "hidden", px: 1.5, py: 0.5 }}>
      <Stack direction="row" columnGap={1.5} sx={{ py: 1 }}>
        <Box sx={{ flex: 1 }}>
          <EditPeriodsButton />
        </Box>
        {cols.map((c) => (
          <Typography
            key={c.direction.key}
            variant="caption"
            color="text.secondary"
            fontWeight={600}
            noWrap
            title={directionLabel(c.direction.name, t)}
            sx={{ width: COL, flexShrink: 0, textAlign: "right" }}>
            {directionPlace(c.direction.name, t)}
          </Typography>
        ))}
      </Stack>
      {periods.map((p) => (
        <Stack
          key={p.key}
          direction="row"
          columnGap={1.5}
          alignItems="center"
          sx={{ py: 0.75, borderTop: 1, borderColor: "divider" }}>
          <Box sx={{ flex: 1, minWidth: 0 }}>
            <Typography variant="body2" fontWeight={600} noWrap sx={{ lineHeight: 1.3 }}>
              {t(`explorer_period_${p.key}`)}
            </Typography>
            <Typography
              variant="caption"
              color="text.secondary"
              display="block"
              noWrap
              sx={{ lineHeight: 1.3 }}>
              {p.start_s ? clock(p.start_s) : ""}–{p.end_s < 30 * 3600 ? clock(p.end_s) : ""}
            </Typography>
          </Box>
          {cols.map((c) => {
            const { text, muted } = cell(c, p.key);
            return (
              <Typography
                key={c.direction.key}
                variant="body2"
                fontWeight={muted ? 400 : 600}
                color={muted ? "text.secondary" : "text.primary"}
                sx={{ width: COL, flexShrink: 0, textAlign: "right", fontVariantNumeric: "tabular-nums" }}>
                {text}
              </Typography>
            );
          })}
        </Stack>
      ))}
    </Box>
  );
}

function Section({ icon, label, children }: { icon: ICON_NAME; label: string; children: ReactNode }) {
  const date = useAppSelector((s) => s.explorer.date) as string;
  return (
    <Box sx={{ mt: 1 }}>
      <SectionHeader px={0} icon={icon} label={label} aside={<DateChip date={date} />} />
      {children}
    </Box>
  );
}

export function RouteService({ data }: { data: RouteSummaryData }) {
  const { t } = useTranslation("common");
  if (!data.operations.runs) {
    return (
      <Typography variant="body2" color="text.secondary" sx={{ mt: 2 }}>
        {t("explorer_no_service_today")}
      </Typography>
    );
  }
  return (
    <Box sx={{ pb: 2 }}>
      <Typography variant="caption" color="text.secondary" display="block" sx={{ mt: 2 }}>
        {t("explorer_service_read_only")}
      </Typography>

      <Section icon={ICON_NAME.CLOCK} label={t("explorer_headways")}>
        <PeriodTable
          data={data}
          cell={(d, period) => {
            const h = d.headways.get(period);
            if (!h || h.departures === 0) return { text: "–", muted: true };
            if (h.minutes === null) return { text: t("explorer_one_trip"), muted: true };
            return { text: `${h.minutes}′`, muted: false };
          }}
        />
      </Section>

      <Section icon={ICON_NAME.CALENDAR} label={t("explorer_span")}>
        <Box sx={{ bgcolor: "action.hover", borderRadius: 1.5, px: 1.5, py: 0.5 }}>
          {data.directions.map((d, i) => (
            <Stack
              key={d.direction.key}
              direction="row"
              columnGap={1.5}
              alignItems="center"
              sx={{ py: 0.75, borderTop: i ? 1 : 0, borderColor: "divider" }}>
              <Typography
                variant="body2"
                fontWeight={600}
                noWrap
                title={directionLabel(d.direction.name, t)}
                sx={{ flex: 1, minWidth: 0 }}>
                {directionLabel(d.direction.name, t)}
              </Typography>
              <Typography variant="body2" sx={{ fontVariantNumeric: "tabular-nums", flexShrink: 0 }}>
                {d.operations.runs
                  ? `${clock(d.operations.first_dep_s)} – ${clock(d.operations.last_dep_s)}`
                  : "–"}
              </Typography>
            </Stack>
          ))}
        </Box>
        <Typography variant="caption" color="text.secondary" display="block" sx={{ mt: 0.75 }}>
          {t("explorer_span_note")}
        </Typography>
      </Section>

      <Section icon={ICON_NAME.CLOCK} label={t("explorer_running_time")}>
        <PeriodTable
          data={data}
          cell={(d, period) => {
            const m = d.running.get(period);
            return m === null || m === undefined
              ? { text: "–", muted: true }
              : { text: `${m} min`, muted: false };
          }}
        />
        <Typography variant="caption" color="text.secondary" display="block" sx={{ mt: 0.75 }}>
          {t("explorer_running_time_note")}
        </Typography>
      </Section>
    </Box>
  );
}
