/**
 * A section's header across the Explorer's left panel: its icon, its name,
 * and at the right what it holds — a count, a note such as the day it shows —
 * and, for a section that lists things, a fold toggle.
 */
import { Box, IconButton, Stack, Typography } from "@mui/material";
import type { ReactElement, ReactNode } from "react";
import { useTranslation } from "react-i18next";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import { shortDate } from "@/lib/explorer/format";

/** Side padding of the panel's rows (board 3). */
export const PX = 2.5;

export function SectionHeader({
  icon,
  label,
  amount,
  note,
  aside,
  folded,
  onFold,
  px = PX,
}: {
  /** A font icon, or a drawn marker such as the map's stop. */
  icon: ICON_NAME | ReactElement;
  label: string;
  /** A count, shown as a pill. */
  amount?: ReactNode;
  /** Plain text at the right. */
  note?: string;
  /** Anything else at the right, e.g. the date the figures are for. */
  aside?: ReactNode;
  folded?: boolean;
  onFold?: () => void;
  /** Side padding; 0 inside a section that is padded already. */
  px?: number;
}) {
  const { t } = useTranslation("common");
  return (
    <Stack direction="row" alignItems="center" spacing={1.25} sx={{ px, minHeight: 40 }}>
      {typeof icon === "string" ? (
        <Icon iconName={icon} style={{ fontSize: 14 }} htmlColor="inherit" />
      ) : (
        icon
      )}
      <Typography variant="body2" fontWeight="bold" noWrap sx={{ flex: 1 }}>
        {label}
      </Typography>
      {note && (
        <Typography variant="caption" color="text.secondary" noWrap>
          {note}
        </Typography>
      )}
      {aside}
      {amount !== undefined && (
        <Box
          component="span"
          sx={{
            px: 1,
            py: 0.25,
            borderRadius: 1,
            bgcolor: "action.hover",
            color: "text.secondary",
            fontSize: 12,
            fontWeight: 600,
          }}>
          {amount}
        </Box>
      )}
      {onFold && (
        <IconButton
          size="small"
          aria-label={folded ? t("explorer_unfold") : t("explorer_fold")}
          aria-expanded={!folded}
          onClick={onFold}
          sx={{ mr: -0.75 }}>
          <Icon iconName={folded ? ICON_NAME.CHEVRON_DOWN : ICON_NAME.CHEVRON_UP} style={{ fontSize: 11 }} />
        </IconButton>
      )}
    </Stack>
  );
}

/** The Explorer's date, set in the bottom bar, as a pill on each section its
 * figures depend on: every figure names its period (05-viewers-and-cost.md). */
export function DateChip({ date }: { date: string }) {
  const { i18n } = useTranslation("common");
  return (
    <Stack
      direction="row"
      alignItems="center"
      spacing={0.75}
      sx={{
        px: 1,
        py: 0.25,
        borderRadius: 1,
        bgcolor: "action.hover",
        color: "text.primary",
        flexShrink: 0,
      }}>
      <Icon iconName={ICON_NAME.CALENDAR} style={{ fontSize: 11 }} htmlColor="inherit" />
      <Typography variant="caption" fontWeight={700} noWrap>
        {shortDate(date, i18n.language)}
      </Typography>
    </Stack>
  );
}
