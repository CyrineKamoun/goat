"use client";

import { IconButton, InputBase, Paper, alpha, useTheme } from "@mui/material";
import type { InputBaseProps } from "@mui/material";
import type { FocusEventHandler, KeyboardEvent, ReactNode } from "react";
import { forwardRef, useRef, useState } from "react";
import { useTranslation } from "react-i18next";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import { surfaceShadows } from "@/components/dashboard/common/surfaceShadows";

/** `hero` is the tall field the Home page leads with, `toolbar` the compact one
 * the Content and Catalog tool rows carry. */
export type SearchInputSize = "hero" | "toolbar";

interface SearchInputProps {
  /** The text on screen. The field is controlled: a caller that debounces what
   * it does with the query keeps its own immediate copy and passes it here. */
  value: string;
  onChange: (value: string) => void;
  /** Runs when the clear button empties the field. Falls back to `onChange("")`. */
  onClear?: () => void;
  placeholder: string;
  autoFocus?: boolean;
  size?: SearchInputSize;
  /** The mark left of the input. */
  startIcon?: ICON_NAME;
  /** Rendered right of the clear slot — Home's ⌘K hint. */
  endAdornment?: ReactNode;
  inputProps?: InputBaseProps["inputProps"];
  /** Names the field for assistive tech; the placeholder when unset. */
  ariaLabel?: string;
  onKeyDown?: (event: KeyboardEvent<HTMLInputElement>) => void;
  onFocus?: FocusEventHandler<HTMLInputElement | HTMLTextAreaElement>;
  onBlur?: FocusEventHandler<HTMLInputElement | HTMLTextAreaElement>;
  /** Takes a row of its own rather than sharing one with the tool pills. */
  fullWidth?: boolean;
  /** Phone layout: the field starts as an icon button and expands over the row
   * once it is tapped, unless it already holds a query. */
  collapsible?: boolean;
}

/** Per-size metrics; everything else — border, hover and focus colour, the
 * search mark, the clear button — is shared, so no two search fields in the
 * dashboard drift apart. Radius follows the row each size sits in: the hero
 * field matches `SurfaceCard`, the toolbar field matches `ToolPill`. */
const METRICS: Record<SearchInputSize, { px: string; py: string; radius: string; font: number }> = {
  hero: { px: "16px", py: "13px", radius: "12px", font: 14.5 },
  toolbar: { px: "16px", py: "10px", radius: "999px", font: 14 },
};

/**
 * The dashboard's one search field. Every surface that searches — Home's hero,
 * the Content feed's toolbar, the Catalog's toolbar — renders this, so the
 * radius, the border treatment, the mark and the clear affordance are decided
 * once here rather than per page.
 */
const SearchInput = forwardRef<HTMLInputElement, SearchInputProps>(function SearchInput(
  {
    value,
    onChange,
    onClear,
    placeholder,
    autoFocus,
    size = "toolbar",
    startIcon = ICON_NAME.SEARCH,
    endAdornment,
    inputProps,
    ariaLabel,
    onKeyDown,
    onFocus,
    onBlur,
    fullWidth,
    collapsible,
  },
  ref
) {
  const { t } = useTranslation("common");
  const theme = useTheme();
  const [focused, setFocused] = useState(false);
  const [expanded, setExpanded] = useState(!!value);
  // Kept alongside the forwarded ref so the collapsed button can hand the
  // caret over on the same tap that expands the field.
  const input = useRef<HTMLInputElement | null>(null);

  const metrics = METRICS[size];

  const clear = () => {
    if (onClear) onClear();
    else onChange("");
    if (collapsible) setExpanded(false);
  };

  if (collapsible && !expanded) {
    return (
      <IconButton
        aria-label={ariaLabel ?? placeholder}
        onClick={() => {
          setExpanded(true);
          // The field is what the tap asked for, so it takes the caret too.
          requestAnimationFrame(() => input.current?.focus());
        }}
        sx={{
          // The same 38px square the tool pills beside it are.
          width: 38,
          height: 38,
          flexShrink: 0,
          border: `1px solid ${theme.palette.divider}`,
          backgroundColor: theme.palette.background.paper,
        }}>
        <Icon iconName={startIcon} style={{ fontSize: 15 }} htmlColor={theme.palette.text.secondary} />
      </IconButton>
    );
  }

  const borderColor = focused ? theme.palette.primary.main : theme.palette.divider;

  return (
    <Paper
      elevation={0}
      sx={{
        display: "flex",
        alignItems: "center",
        gap: "10px",
        flex: fullWidth ? "1 1 100%" : "1 1 220px",
        minWidth: 0,
        px: metrics.px,
        py: metrics.py,
        borderRadius: metrics.radius,
        border: `1px solid ${borderColor}`,
        boxShadow: focused
          ? `0 0 0 3px ${alpha(theme.palette.primary.main, 0.12)}`
          : surfaceShadows(theme).rest,
        transition: theme.transitions.create(["border-color", "box-shadow"], { duration: 140 }),
        "&:hover": {
          borderColor: focused ? theme.palette.primary.main : alpha(theme.palette.text.primary, 0.24),
        },
      }}>
      <Icon
        iconName={startIcon}
        style={{
          fontSize: 15,
          color: value ? theme.palette.primary.main : theme.palette.text.secondary,
        }}
      />
      <InputBase
        inputRef={(node: HTMLInputElement | null) => {
          input.current = node;
          if (typeof ref === "function") ref(node);
          else if (ref) ref.current = node;
        }}
        value={value}
        autoFocus={autoFocus}
        onChange={(event) => onChange(event.target.value)}
        onFocus={(event) => {
          setFocused(true);
          onFocus?.(event);
        }}
        onBlur={(event) => {
          setFocused(false);
          onBlur?.(event);
        }}
        onKeyDown={onKeyDown}
        placeholder={placeholder}
        sx={{ flex: 1, minWidth: 0, fontSize: metrics.font }}
        inputProps={{ "aria-label": ariaLabel ?? placeholder, ...inputProps }}
      />
      {!!value || collapsible ? (
        <IconButton size="small" aria-label={t("clear")} onClick={clear}>
          <Icon
            iconName={ICON_NAME.XCLOSE}
            style={{ fontSize: 12 }}
            htmlColor={theme.palette.text.secondary}
          />
        </IconButton>
      ) : (
        endAdornment
      )}
    </Paper>
  );
});

export default SearchInput;
