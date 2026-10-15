/**
 * The assumptions behind the Explorer's figures, in one dialog: cost rates per
 * operator and route type (board 15), the time periods headways and running
 * times are given for, and the layover that estimates vehicles where a feed
 * has no blocks. Edited as a draft; Save keeps them for this bundle in this
 * browser and every figure follows.
 */
import { Box, ButtonBase, Stack, Tab, Tabs, TextField, Typography } from "@mui/material";
import { useEffect, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import { clock, count, currencySymbol } from "@/lib/explorer/format";
import { modeOf } from "@/lib/explorer/modes";
import {
  CURRENCIES,
  type ExplorerSettings,
  PERIOD_KEYS,
  SERVICE_DAY_END_S,
  defaultRate,
  periodsFrom,
  rateKey,
  settingsProblems,
} from "@/lib/explorer/settings";
import { type ExplorerSettingsTab, openExplorerSettings } from "@/lib/store/explorer/slice";

import { useRoutes, useServiceDays } from "@/hooks/explorer/useExplorerData";
import { useAppDispatch, useAppSelector } from "@/hooks/store/ContextHooks";

import AppDialog, { AppDialogFooter } from "@/components/common/AppDialog";
import { useExplorer } from "@/components/explorer/ExplorerContext";
import { modeIcon } from "@/components/explorer/modeIcon";
import Selector from "@/components/map/panels/common/Selector";

/** A number field's text, so it can be blank or half-typed while edited. */
type RateDraft = Record<string, { km: string; hour: string }>;

interface Draft {
  currency: string;
  rates: RateDraft;
  /** The periods' starts after the first, as `HH:MM`. */
  starts: string[];
  layover_min: string;
}

const numberSx = {
  width: 96,
  "& .MuiInputBase-input": { px: 1, py: 0.75, textAlign: "right", fontSize: "0.8125rem" },
};

/** `HH:MM` as seconds; hours may pass 24, as a feed's times do. */
function parseClock(text: string): number | null {
  const m = /^(\d{1,2}):([0-5]\d)$/.exec(text.trim());
  return m ? Number(m[1]) * 3600 + Number(m[2]) * 60 : null;
}

function toDraft(settings: ExplorerSettings): Draft {
  const rates: RateDraft = {};
  for (const [key, r] of Object.entries(settings.rates))
    rates[key] = { km: String(r.per_km), hour: r.per_hour === null ? "" : String(r.per_hour) };
  return {
    currency: settings.currency,
    rates,
    starts: settings.periods.slice(1).map((p) => clock(p.start_s)),
    layover_min: String(Math.round(settings.layover_s / 60)),
  };
}

/** The draft as settings, or null where a field does not parse. */
function fromDraft(draft: Draft, base: ExplorerSettings): ExplorerSettings | null {
  const rates: ExplorerSettings["rates"] = {};
  for (const [key, r] of Object.entries(draft.rates)) {
    // A blank km rate is no rate: the route type keeps its default.
    if (!r.km.trim()) continue;
    const per_km = Number(r.km.replace(",", "."));
    const per_hour = r.hour.trim() ? Number(r.hour.replace(",", ".")) : null;
    if (Number.isNaN(per_km) || (per_hour !== null && Number.isNaN(per_hour))) return null;
    rates[key] = { per_km, per_hour };
  }
  const starts = draft.starts.map(parseClock);
  const layover = Number(draft.layover_min);
  if (starts.some((s) => s === null) || Number.isNaN(layover)) return null;
  return {
    ...base,
    currency: draft.currency,
    rates,
    periods: periodsFrom(starts as number[]),
    layover_s: Math.round(layover * 60),
  };
}

function RatesTab({ draft, setDraft }: { draft: Draft; setDraft: (d: Draft) => void }) {
  const { t, i18n } = useTranslation("common");
  const { db, window } = useExplorer();
  const routes = useRoutes(db);
  const days = useServiceDays(db, window);
  const lang = i18n.language;
  const symbol = currencySymbol(draft.currency, lang);

  // The operators, each with the route types it runs and how many routes.
  const agencies = useMemo(() => {
    const byAgency = new Map<
      string,
      { id: string | null; name: string; types: Map<number | null, number>; n: number }
    >();
    for (const r of routes.data ?? []) {
      const id = r.agency_id;
      const key = id ?? "";
      const a = byAgency.get(key) ?? {
        id,
        name: r.agency ?? t("explorer_unknown_agency"),
        types: new Map(),
        n: 0,
      };
      a.types.set(r.route_type, (a.types.get(r.route_type) ?? 0) + 1);
      a.n += 1;
      byAgency.set(key, a);
    }
    return [...byAgency.values()].sort((a, b) => b.n - a.n || a.name.localeCompare(b.name));
  }, [routes.data, t]);
  const [agencyKey, setAgencyKey] = useState<string | null>(null);
  const agency = agencies.find((a) => (a.id ?? "") === agencyKey) ?? agencies[0];

  const hasRates = (agencyId: string | null) =>
    Object.entries(draft.rates).some(
      ([key, r]) => key.startsWith(`${agencyId ?? ""}|`) && r.km.trim() !== ""
    );
  const setRate = (key: string, field: "km" | "hour", value: string) => {
    const current = draft.rates[key] ?? { km: "", hour: "" };
    setDraft({ ...draft, rates: { ...draft.rates, [key]: { ...current, [field]: value } } });
  };

  return (
    <Stack spacing={2}>
      {/* A feed may have one operator or fifty: a searchable list, as the
          toolbox picks from many. Operators with rates set say so. Beside
          it, the currency every rate is in. */}
      <Stack direction="row" spacing={1.5} alignItems="flex-end">
        {agencies.length > 1 && (
          <Box sx={{ flex: 1, minWidth: 0 }}>
            <Selector
              label={t("explorer_filter_agency")}
              enableSearch={agencies.length > 8}
              items={agencies.map((a) => ({
                value: a.id ?? "",
                label: `${a.name} · ${t("explorer_n_routes", { count: a.n })}${
                  hasRates(a.id) ? ` · ${t("explorer_rates_set")}` : ""
                }`,
              }))}
              selectedItems={
                agency
                  ? {
                      value: agency.id ?? "",
                      label: `${agency.name} · ${t("explorer_n_routes", { count: agency.n })}`,
                    }
                  : undefined
              }
              setSelectedItems={(item) => {
                if (item && !Array.isArray(item)) setAgencyKey(String(item.value));
              }}
            />
          </Box>
        )}
        <Box sx={{ width: agencies.length > 1 ? 112 : 160, flexShrink: 0 }}>
          <Selector
            label={t("explorer_currency")}
            items={CURRENCIES.map((c) => ({ value: c, label: c }))}
            selectedItems={{ value: draft.currency, label: draft.currency }}
            setSelectedItems={(item) => {
              if (item && !Array.isArray(item)) setDraft({ ...draft, currency: String(item.value) });
            }}
          />
        </Box>
      </Stack>
      {agency && (
        <Box component="table" sx={{ width: "100%", borderCollapse: "collapse" }}>
          <thead>
            <Box
              component="tr"
              sx={{ "& th": { fontSize: 12, fontWeight: 600, color: "text.secondary", pb: 1 } }}>
              <Box component="th" sx={{ textAlign: "left" }}>
                {t("explorer_filter_route_type")}
              </Box>
              <Box component="th" sx={{ textAlign: "right" }}>
                {t("explorer_rate_km_head", { currency: symbol })}
              </Box>
              <Box component="th" sx={{ textAlign: "right", pl: 1.5 }}>
                {t("explorer_rate_hour_head", { currency: symbol })}
              </Box>
            </Box>
          </thead>
          <tbody>
            {[...agency.types.keys()]
              .sort((a, b) => (a ?? 0) - (b ?? 0))
              .map((type) => {
                const key = rateKey(agency.id, type);
                const mode = modeOf(type);
                const fallback = defaultRate(type);
                const value = draft.rates[key] ?? { km: "", hour: "" };
                return (
                  <Box component="tr" key={key} sx={{ borderTop: 1, borderColor: "divider" }}>
                    <Box component="td" sx={{ py: 1 }}>
                      <Stack direction="row" spacing={1} alignItems="center">
                        <Icon iconName={modeIcon(mode)} style={{ fontSize: 13 }} htmlColor="inherit" />
                        <Typography variant="body2" fontWeight={600}>
                          {t(`explorer_mode_${mode}`)}
                        </Typography>
                        <Typography variant="caption" color="text.secondary">
                          {t("explorer_n_routes", { count: agency.types.get(type) ?? 0 })}
                        </Typography>
                      </Stack>
                    </Box>
                    <Box component="td" sx={{ textAlign: "right" }}>
                      <TextField
                        size="small"
                        inputMode="decimal"
                        value={value.km}
                        // The default shows where none is set: what applies now.
                        placeholder={String(fallback.per_km)}
                        onChange={(e) => setRate(key, "km", e.target.value)}
                        inputProps={{ "aria-label": t("explorer_rate_per_km", { currency: draft.currency }) }}
                        sx={numberSx}
                      />
                    </Box>
                    <Box component="td" sx={{ textAlign: "right", pl: 1.5 }}>
                      <TextField
                        size="small"
                        inputMode="decimal"
                        value={value.hour}
                        placeholder="—"
                        onChange={(e) => setRate(key, "hour", e.target.value)}
                        inputProps={{
                          "aria-label": t("explorer_rate_per_hour", { currency: draft.currency }),
                        }}
                        sx={numberSx}
                      />
                    </Box>
                  </Box>
                );
              })}
          </tbody>
        </Box>
      )}
      <Typography variant="caption" color="text.secondary">
        {t("explorer_rates_note")}
        {days.data &&
          ` ${t("explorer_rates_days_counted", {
            days: count(days.data.weekdays + days.data.saturdays + days.data.sundays, lang),
          })}`}
      </Typography>
    </Stack>
  );
}

function PeriodsTab({ draft, setDraft }: { draft: Draft; setDraft: (d: Draft) => void }) {
  const { t } = useTranslation("common");
  const starts = ["00:00", ...draft.starts];
  return (
    <Stack spacing={1.5}>
      <Box sx={{ bgcolor: "action.hover", borderRadius: 1.5, px: 1.5, py: 0.5 }}>
        {PERIOD_KEYS.map((key, i) => {
          const end = i < PERIOD_KEYS.length - 1 ? starts[i + 1] : clock(SERVICE_DAY_END_S);
          return (
            <Stack
              key={key}
              direction="row"
              alignItems="center"
              spacing={1.5}
              sx={{ py: 0.75, borderTop: i ? 1 : 0, borderColor: "divider" }}>
              <Typography variant="body2" fontWeight={600} sx={{ flex: 1 }}>
                {t(`explorer_period_${key}`)}
              </Typography>
              {/* The first period starts with the service day; each other
                  starts where it is set, and ends where the next starts. */}
              {i === 0 ? (
                <Typography
                  variant="body2"
                  color="text.secondary"
                  sx={{ width: 96, textAlign: "right", pr: 1 }}>
                  00:00
                </Typography>
              ) : (
                <TextField
                  size="small"
                  value={draft.starts[i - 1]}
                  onChange={(e) => {
                    const next = [...draft.starts];
                    next[i - 1] = e.target.value;
                    setDraft({ ...draft, starts: next });
                  }}
                  error={parseClock(draft.starts[i - 1]) === null}
                  inputProps={{ "aria-label": t(`explorer_period_${key}`) }}
                  sx={numberSx}
                />
              )}
              <Typography variant="body2" color="text.secondary" sx={{ width: 64 }}>
                – {end}
              </Typography>
            </Stack>
          );
        })}
      </Box>
      <Typography variant="caption" color="text.secondary">
        {t("explorer_periods_note")}
      </Typography>
    </Stack>
  );
}

function VehiclesTab({ draft, setDraft }: { draft: Draft; setDraft: (d: Draft) => void }) {
  const { t } = useTranslation("common");
  return (
    <Stack spacing={1.5}>
      <Stack direction="row" spacing={1.5} alignItems="center">
        <Typography variant="body2" fontWeight={600} sx={{ flex: 1 }}>
          {t("explorer_layover")}
        </Typography>
        <TextField
          size="small"
          inputMode="numeric"
          value={draft.layover_min}
          onChange={(e) => setDraft({ ...draft, layover_min: e.target.value })}
          inputProps={{ "aria-label": t("explorer_layover") }}
          sx={numberSx}
        />
        <Typography variant="body2" color="text.secondary" sx={{ width: 40 }}>
          {t("explorer_minutes_short")}
        </Typography>
      </Stack>
      <Typography variant="caption" color="text.secondary">
        {t("explorer_layover_note")}
      </Typography>
    </Stack>
  );
}

export function ExplorerSettingsDialog() {
  const { t } = useTranslation("common");
  const dispatch = useAppDispatch();
  const tab = useAppSelector((s) => s.explorer.settingsTab);
  const { settings, setSettings } = useExplorer();
  const [draft, setDraft] = useState<Draft>(() => toDraft(settings));
  const [saveFailed, setSaveFailed] = useState(false);

  // Each opening starts from what is saved.
  useEffect(() => {
    if (tab) {
      setDraft(toDraft(settings));
      setSaveFailed(false);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tab !== null]);

  const next = fromDraft(draft, settings);
  const problems = next ? settingsProblems(next) : ["explorer_settings_invalid"];
  const close = () => dispatch(openExplorerSettings(null));

  return (
    <AppDialog
      open={tab !== null}
      onClose={close}
      icon={ICON_NAME.SLIDERS}
      title={t("explorer_settings")}
      maxWidth={600}
      notice={
        problems.length || saveFailed ? (
          <Typography variant="body2" color="error">
            {saveFailed ? t("explorer_settings_not_kept") : t(problems[0])}
          </Typography>
        ) : undefined
      }
      footer={
        <AppDialogFooter
          onCancel={close}
          primaryLabel={t("save")}
          primaryDisabled={problems.length > 0}
          onPrimary={() => {
            if (!next) return;
            if (setSettings(next)) close();
            else setSaveFailed(true);
          }}
          extra={
            <Typography variant="caption" color="text.secondary">
              {t("explorer_settings_where")}
            </Typography>
          }
        />
      }>
      <Tabs
        value={tab ?? "rates"}
        onChange={(_e, value: ExplorerSettingsTab) => dispatch(openExplorerSettings(value))}
        sx={{ minHeight: 36, mb: 2, borderBottom: 1, borderColor: "divider" }}>
        <Tab value="rates" label={t("explorer_cost_rates")} sx={{ minHeight: 36, textTransform: "none" }} />
        <Tab
          value="periods"
          label={t("explorer_time_periods")}
          sx={{ minHeight: 36, textTransform: "none" }}
        />
        <Tab value="vehicles" label={t("explorer_vehicles")} sx={{ minHeight: 36, textTransform: "none" }} />
      </Tabs>
      {tab === "rates" && <RatesTab draft={draft} setDraft={setDraft} />}
      {tab === "periods" && <PeriodsTab draft={draft} setDraft={setDraft} />}
      {tab === "vehicles" && <VehiclesTab draft={draft} setDraft={setDraft} />}
    </AppDialog>
  );
}

/** In the corner of a table by time period: what the periods are, and the way
 * to change them. */
export function EditPeriodsButton() {
  const { t } = useTranslation("common");
  const dispatch = useAppDispatch();
  return (
    <ButtonBase
      onClick={() => dispatch(openExplorerSettings("periods"))}
      title={t("explorer_edit_time_periods")}
      sx={{ gap: 0.75, px: 0.5, ml: -0.5, borderRadius: 1, color: "text.secondary" }}>
      <Icon iconName={ICON_NAME.SLIDERS} style={{ fontSize: 11 }} htmlColor="inherit" />
      <Typography variant="caption" fontWeight={600}>
        {t("explorer_time_periods")}
      </Typography>
    </ButtonBase>
  );
}
