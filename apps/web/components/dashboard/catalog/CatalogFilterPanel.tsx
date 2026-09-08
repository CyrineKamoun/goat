"use client";

import CheckIcon from "@mui/icons-material/Check";
import { Box, Collapse, InputBase, Paper, Stack, Typography, useTheme } from "@mui/material";
import { useState } from "react";
import { useTranslation } from "react-i18next";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import { surfaceShadows } from "@/components/dashboard/common/surfaceShadows";
import TextFieldInput from "@/components/map/panels/common/TextFieldInput";

/** The filter sidebar: a "Filters / Clear (n)" header, then favourites, the spatial filter, a date
 * range and the facets, each a collapsible section. */

/** Header + collapsible body, the shape every section in the panel shares. */
const Section = ({
  inset = 4,
  icon,
  label,
  defaultOpen = false,
  children,
}: {
  /** Horizontal padding in theme units; 0 flushes the rows to the rail. */
  inset?: number;
  icon: ICON_NAME;
  label: string;
  defaultOpen?: boolean;
  children: React.ReactNode;
}) => {
  const theme = useTheme();
  const [open, setOpen] = useState(defaultOpen);
  return (
    <Box sx={{ borderBottom: `1px solid ${theme.palette.divider}` }}>
      <Box
        component="button"
        type="button"
        onClick={() => setOpen((value) => !value)}
        sx={{
          display: "flex",
          alignItems: "center",
          gap: 2.5,
          width: "100%",
          px: inset,
          py: 3,
          background: "transparent",
          border: "none",
          cursor: "pointer",
          font: "inherit",
          color: theme.palette.text.primary,
        }}>
        <Icon iconName={icon} style={{ fontSize: 14 }} htmlColor={theme.palette.text.secondary} />
        <Typography variant="body2" fontWeight={600} sx={{ flex: 1, textAlign: "left" }}>
          {label}
        </Typography>
        {/* One chevron that turns, rather than two that swap: swapping the glyph has
            nothing to animate between. */}
        <Box
          sx={{
            display: "flex",
            transform: open ? "rotate(180deg)" : "none",
            transition: theme.transitions.create("transform", {
              duration: theme.transitions.duration.shortest,
            }),
          }}>
          <Icon
            iconName={ICON_NAME.CHEVRON_DOWN}
            style={{ fontSize: 14 }}
            htmlColor={theme.palette.text.secondary}
          />
        </Box>
      </Box>
      {/* `unmountOnExit`, so a closed section costs nothing: a panel holds several,
          each with every bucket the facet offers. */}
      <Collapse in={open} unmountOnExit>
        <Box sx={{ px: inset ? 2 : 0, pb: 3 }}>{children}</Box>
      </Collapse>
    </Box>
  );
};

/** The panel's checkbox. */
const FilterCheckbox = ({
  checked,
  disabled,
  onChange,
}: {
  checked: boolean;
  disabled?: boolean;
  onChange: () => void;
}) => {
  const theme = useTheme();
  return (
    <Box sx={{ position: "relative", width: 15, height: 15, flexShrink: 0, display: "inline-flex" }}>
      <Box
        component="input"
        type="checkbox"
        checked={checked}
        disabled={disabled}
        onChange={onChange}
        sx={{
          appearance: "none",
          width: 15,
          height: 15,
          m: 0,
          borderRadius: "3px",
          border: `1.5px solid ${checked ? theme.palette.primary.main : theme.palette.divider}`,
          backgroundColor: checked ? theme.palette.primary.main : "transparent",
          cursor: disabled ? "not-allowed" : "pointer",
        }}
      />
      {checked && (
        <CheckIcon
          sx={{
            position: "absolute",
            inset: 0,
            m: "auto",
            fontSize: 11,
            color: theme.palette.common.white,
            pointerEvents: "none",
          }}
        />
      )}
    </Box>
  );
};

export type FacetOption = { value: string; label: string; count: number };

/** How a long facet is bounded, and the size at which it also gains a search field. */
const MAX_LIST_HEIGHT = 268;
const SCROLLABLE_FROM = 9;
const SEARCHABLE_FROM = 12;

/** One facet's options: all of them, scrolling inside the section once there are more than a handful, with a search field above once there are many. */
const FacetOptions = ({
  options,
  selected,
  onToggle,
}: {
  options: FacetOption[];
  selected: string[];
  onToggle: (value: string) => void;
}) => {
  const { t } = useTranslation("common");
  const theme = useTheme();
  const [query, setQuery] = useState("");

  const searchable = options.length >= SEARCHABLE_FROM;
  const needle = query.trim().toLowerCase();
  // A selected value always survives the search text — a filter you cannot see is
  // a filter you cannot remove.
  const visible = needle
    ? options.filter(
        (option) => option.label.toLowerCase().includes(needle) || selected.includes(option.value)
      )
    : options;
  const scrolls = visible.length > SCROLLABLE_FROM;

  return (
    <Stack>
      {searchable && (
        <Stack
          direction="row"
          alignItems="center"
          spacing={2}
          sx={{
            mx: 2,
            mb: 1.5,
            px: 2.5,
            py: 1.5,
            borderRadius: 1.5,
            border: `1px solid ${theme.palette.divider}`,
            backgroundColor: theme.palette.action.hover,
          }}>
          <Icon
            iconName={ICON_NAME.SEARCH}
            style={{ fontSize: 12 }}
            htmlColor={theme.palette.text.secondary}
          />
          <InputBase
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            placeholder={t("catalog_filter_values")}
            sx={{ flex: 1, fontSize: 12.5 }}
          />
          {query && (
            <Box
              component="button"
              type="button"
              aria-label={t("clear")}
              onClick={() => setQuery("")}
              sx={{
                display: "flex",
                p: 0,
                border: "none",
                background: "transparent",
                cursor: "pointer",
              }}>
              <Icon
                iconName={ICON_NAME.XCLOSE}
                style={{ fontSize: 10 }}
                htmlColor={theme.palette.text.secondary}
              />
            </Box>
          )}
        </Stack>
      )}
      <Box
        sx={
          scrolls
            ? {
                maxHeight: MAX_LIST_HEIGHT,
                overflowY: "auto",
                // Room for the scrollbar so it never sits on top of the counts.
                pr: 1,
                // Hairlines say "there is more" without needing a label.
                borderTop: `1px solid ${theme.palette.divider}`,
                borderBottom: `1px solid ${theme.palette.divider}`,
              }
            : undefined
        }>
        {visible.map((option) => {
          const on = selected.includes(option.value);
          const disabled = option.count === 0 && !on;
          return (
            <Box
              key={option.value}
              component="label"
              sx={{
                display: "flex",
                alignItems: "center",
                gap: 2.5,
                px: 2,
                py: 1.5,
                borderRadius: 1,
                cursor: disabled ? "not-allowed" : "pointer",
                opacity: disabled ? 0.45 : 1,
                backgroundColor: on ? theme.palette.action.selected : "transparent",
                "&:hover": disabled || on ? undefined : { backgroundColor: theme.palette.action.hover },
              }}>
              <FilterCheckbox checked={on} disabled={disabled} onChange={() => onToggle(option.value)} />
              <Typography variant="body2" sx={{ flex: 1, minWidth: 0 }} noWrap title={option.label}>
                {option.label}
              </Typography>
              <Typography variant="caption" color="text.secondary">
                {option.count}
              </Typography>
            </Box>
          );
        })}
      </Box>
      {visible.length === 0 && (
        <Typography variant="caption" color="text.secondary" sx={{ px: 2 }}>
          {needle ? t("no_results") : "—"}
        </Typography>
      )}
    </Stack>
  );
};

export type CatalogFacetSection = {
  /** Query parameter this facet narrows (`goat:filter_param`). */
  param: string;
  label: string;
  icon: ICON_NAME;
  options: FacetOption[];
};

type Props = {
  facets: CatalogFacetSection[];
  selected: Record<string, string[]>;
  onToggleFacet: (param: string, value: string) => void;
  activeFilterCount: number;
  onClearAll: () => void;
  /** Omitted by hosts that offer favourites some other way (the Add Layer picker
   * has a tab for them). */
  favouritesOnly?: boolean;
  onToggleFavourites?: () => void;
  dateFrom?: string | null;
  dateTo?: string | null;
  /** Omitted by hosts with no date filter — the Add Layer picker. */
  onChangeDates?: (range: { from?: string | null; to?: string | null }) => void;
  /** The spatial control, injected so this panel stays presentational. */
  spatialFilter?: React.ReactNode;
  /** Rendered in the header beside "Clear". */
  headerAction?: React.ReactNode;
  /** Drop the card framing, for a panel that already sits inside one. */
  flush?: boolean;
  /**
   * Horizontal padding of the panel's rows, in theme units. The rows themselves always
   * span the full width, so this indents their contents without shortening the rules
   * between them — which is what lets the Add Layer picker sit the panel flush against
   * the modal's frame and still keep its labels off the edge.
   */
  inset?: number;
};

const CatalogFilterPanel = ({
  facets,
  selected,
  onToggleFacet,
  activeFilterCount,
  onClearAll,
  favouritesOnly,
  onToggleFavourites,
  dateFrom,
  dateTo,
  onChangeDates,
  spatialFilter,
  headerAction,
  flush,
  inset = 4,
}: Props) => {
  const { t } = useTranslation("common");
  const theme = useTheme();

  return (
    <Paper
      elevation={0}
      sx={{
        borderRadius: flush ? 0 : 2.5,
        border: flush ? "none" : `1px solid ${theme.palette.divider}`,
        boxShadow: flush ? "none" : surfaceShadows(theme).rest,
        overflow: "hidden",
        backgroundColor: flush ? "transparent" : undefined,
      }}>
      <Stack
        direction="row"
        alignItems="center"
        justifyContent="space-between"
        sx={{ px: inset, py: 3.5, borderBottom: `1px solid ${theme.palette.divider}` }}>
        <Typography variant="body2" fontWeight={700}>
          {t("filter")}
        </Typography>
        <Stack direction="row" alignItems="center" spacing={3}>
          {activeFilterCount > 0 && (
            <Typography
              component="button"
              variant="caption"
              onClick={onClearAll}
              sx={{
                background: "transparent",
                border: "none",
                cursor: "pointer",
                color: theme.palette.primary.main,
                fontWeight: 600,
                // `fontFamily`, never the `font` shorthand: the shorthand also resets
                // font-size, which threw away caption's 12px and made this row grow.
                fontFamily: "inherit",
                p: 0,
              }}>
              {t("catalog_clear_n", { count: activeFilterCount })}
            </Typography>
          )}
          {headerAction}
        </Stack>
      </Stack>

      {/* Favourites: a filter, not a facet — it has no buckets to count. */}
      {onToggleFavourites && (
        <Stack
          direction="row"
          alignItems="center"
          spacing={2.5}
          // A label, like the facet rows: the row click forwards to the checkbox
          // natively, so there is exactly one toggle — a row onClick PLUS the
          // checkbox's own onChange fired twice and cancelled out.
          component="label"
          sx={{
            px: inset,
            py: 3,
            cursor: "pointer",
            borderBottom: `1px solid ${theme.palette.divider}`,
            backgroundColor: favouritesOnly ? theme.palette.action.selected : "transparent",
          }}>
          <FilterCheckbox checked={!!favouritesOnly} onChange={onToggleFavourites} />
          <Icon
            iconName={ICON_NAME.STAR}
            style={{ fontSize: 14 }}
            htmlColor={favouritesOnly ? theme.palette.primary.main : theme.palette.text.secondary}
          />
          <Typography variant="body2" fontWeight={600} sx={{ flex: 1 }}>
            {t("catalog_show_my_favourites")}
          </Typography>
        </Stack>
      )}

      {spatialFilter}

      {facets.map((facet, index) => (
        <Section
          key={facet.param}
          icon={facet.icon}
          label={facet.label}
          // The first couple of facets open by default.
          // the panel should show its shape without a click, but not be a wall.
          defaultOpen={index < 2}
          inset={inset}>
          <FacetOptions
            options={facet.options}
            selected={selected[facet.param] ?? []}
            onToggle={(value) => onToggleFacet(facet.param, value)}
          />
        </Section>
      ))}
      {onChangeDates && (
        <Section icon={ICON_NAME.CALENDAR} label={t("catalog_datetime")} inset={inset}>
          {/* The app's own input (`TextFieldInput`, as the attribute-field editor uses) rather than a bare MUI `TextField`: same 40px height, same floating label treatment, same clear affordance as every other field in the product. */}
          <Stack spacing={2} sx={{ px: inset ? 2 : 0 }}>
            <TextFieldInput
              type="date"
              label={t("catalog_date_from")}
              value={dateFrom ?? ""}
              onChange={(value) => onChangeDates({ from: value || null, to: dateTo })}
            />
            <TextFieldInput
              type="date"
              label={t("catalog_date_to")}
              value={dateTo ?? ""}
              onChange={(value) => onChangeDates({ from: dateFrom, to: value || null })}
            />
          </Stack>
        </Section>
      )}
    </Paper>
  );
};

export default CatalogFilterPanel;
