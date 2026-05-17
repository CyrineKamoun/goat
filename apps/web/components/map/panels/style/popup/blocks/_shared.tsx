import { ToggleButton, ToggleButtonGroup, Typography } from "@mui/material";
import { alpha } from "@mui/material/styles";
import type { ReactNode } from "react";

/**
 * Uppercase mini-label used above each control in the popup block
 * editors (LABEL / URL TEMPLATE / STYLE / COLOR / SOURCE / SIZING / ...).
 */
export function MiniLabel({ children }: { children: ReactNode }) {
  return (
    <Typography
      variant="caption"
      sx={{
        display: "block",
        color: "text.secondary",
        textTransform: "uppercase",
        letterSpacing: 0.6,
        fontSize: 10,
        fontWeight: "bold",
        mb: 0.75,
      }}>
      {children}
    </Typography>
  );
}

/**
 * Primary-tinted full-width pill toggle. Same Felt-style look used by the
 * popup mode toggle (Simple / HTML) — used inside block editors for
 * 2- or 3-option choices like SOURCE, SIZING, STYLE.
 */
export function PillToggleGroup<T extends string>({
  value,
  onChange,
  options,
}: {
  value: T;
  onChange: (next: T) => void;
  options: Array<{ value: T; label: string }>;
}) {
  return (
    <ToggleButtonGroup
      exclusive
      size="small"
      fullWidth
      value={value}
      onChange={(_, v) => v && onChange(v as T)}
      sx={(theme) => ({
        border: `1px solid ${theme.palette.primary.main}`,
        borderRadius: 999,
        overflow: "hidden",
        "& .MuiToggleButton-root": {
          border: "none",
          borderRadius: 0,
          textTransform: "none",
          color: theme.palette.primary.main,
          fontSize: 13,
          fontWeight: "bold",
          lineHeight: 1.4,
          py: 1.25,
          px: 2,
          whiteSpace: "nowrap",
          "&:hover": {
            backgroundColor: alpha(theme.palette.primary.main, 0.06),
          },
          "&.Mui-selected": {
            backgroundColor: alpha(theme.palette.primary.main, 0.14),
            color: theme.palette.primary.main,
            fontWeight: "bold",
          },
          "&.Mui-selected:hover": {
            backgroundColor: alpha(theme.palette.primary.main, 0.22),
          },
          "&:not(:first-of-type)": {
            borderLeft: `1px solid ${theme.palette.primary.main}`,
          },
        },
      })}>
      {options.map((o) => (
        <ToggleButton key={o.value} value={o.value}>
          {o.label}
        </ToggleButton>
      ))}
    </ToggleButtonGroup>
  );
}
