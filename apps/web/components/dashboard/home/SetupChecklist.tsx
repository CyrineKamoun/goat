"use client";

import { Box, Button, LinearProgress, Typography, useTheme } from "@mui/material";
import { useState } from "react";
import { useTranslation } from "react-i18next";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import type { SetupStepId } from "@/hooks/dashboard/home/useHomeStage";
import { SETUP_STEPS, SETUP_STEP_META } from "@/hooks/dashboard/home/useHomeStage";

import SurfaceCard from "@/components/dashboard/common/SurfaceCard";

/** How many open steps the compact card shows before "Show all" (H9). */
const COMPACT_OPEN_STEPS = 3;

interface SetupChecklistProps {
  /** Steps the caller's onboarding facts already satisfy. */
  done: SetupStepId[];
  /** Caps the open steps shown to `COMPACT_OPEN_STEPS`, behind a
   * "Show all" toggle, for the card's place on Home (New leads the page,
   * Getting started sits under the hero). */
  compact?: boolean;
  onStep: (step: SetupStepId) => void;
}

/**
 * H9's "Set up your workspace" card: a header with the done count and a
 * progress bar, then every step in order — done ones struck through with no
 * action, open ones with their body copy and a call to action, the first
 * open step's action made primary so there is always one obvious next click.
 */
const SetupChecklist = ({ done, compact, onStep }: SetupChecklistProps) => {
  const { t } = useTranslation("common");
  const theme = useTheme();
  const [expanded, setExpanded] = useState(false);

  const openSteps = SETUP_STEPS.filter((step) => !done.includes(step));
  const firstOpenStep = openSteps[0];
  const collapse = Boolean(compact) && !expanded && openSteps.length > COMPACT_OPEN_STEPS;
  const visibleOpenSteps = collapse ? openSteps.slice(0, COMPACT_OPEN_STEPS) : openSteps;
  const visibleOpenIds = new Set(visibleOpenSteps);

  return (
    <SurfaceCard hoverable={false} sx={{ p: "20px" }}>
      <Box sx={{ display: "flex", alignItems: "center", justifyContent: "space-between", mb: "10px" }}>
        <Typography component="h2" sx={{ fontSize: 16, fontWeight: 700 }}>
          {t("set_up_workspace")}
        </Typography>
        <Typography sx={{ fontSize: 13, color: theme.palette.text.secondary }}>
          {t("n_of_m_done", { n: done.length, m: SETUP_STEPS.length })}
        </Typography>
      </Box>
      <LinearProgress
        variant="determinate"
        value={(done.length / SETUP_STEPS.length) * 100}
        sx={{ height: 6, borderRadius: "999px", mb: "16px" }}
      />

      <Box sx={{ display: "flex", flexDirection: "column", gap: "2px" }}>
        {SETUP_STEPS.map((step) => {
          const isDone = done.includes(step);
          if (!isDone && !visibleOpenIds.has(step)) return null;
          const meta = SETUP_STEP_META[step];

          if (isDone) {
            return (
              <Box
                key={step}
                sx={{ display: "flex", alignItems: "center", gap: "10px", py: "8px", opacity: 0.55 }}>
                <Icon
                  iconName={ICON_NAME.CIRCLECHECK}
                  style={{ fontSize: 16, color: theme.palette.success.main }}
                />
                <Typography sx={{ fontSize: 14, textDecoration: "line-through" }}>
                  {t(meta.titleKey)}
                </Typography>
              </Box>
            );
          }

          return (
            <Box
              key={step}
              sx={{
                display: "flex",
                alignItems: "flex-start",
                gap: "12px",
                py: "10px",
                "&:not(:last-of-type)": { borderBottom: `1px solid ${theme.palette.divider}` },
              }}>
              <Icon
                iconName={meta.icon}
                style={{ fontSize: 16, marginTop: 3, color: theme.palette.primary.main }}
              />
              <Box sx={{ flex: 1, minWidth: 0 }}>
                <Typography sx={{ fontSize: 14, fontWeight: 600 }}>{t(meta.titleKey)}</Typography>
                <Typography sx={{ fontSize: 13, color: theme.palette.text.secondary, mt: "2px" }}>
                  {t(meta.bodyKey)}
                </Typography>
              </Box>
              <Button
                size="small"
                variant={step === firstOpenStep ? "contained" : "outlined"}
                onClick={() => onStep(step)}
                sx={{ borderRadius: "999px", textTransform: "none", fontWeight: 600, flexShrink: 0 }}>
                {t(meta.ctaKey)}
              </Button>
            </Box>
          );
        })}
      </Box>

      {collapse && (
        <Button
          size="small"
          variant="text"
          onClick={() => setExpanded(true)}
          sx={{ mt: "8px", textTransform: "none", fontWeight: 600 }}>
          {t("show_all_steps", { m: SETUP_STEPS.length })}
        </Button>
      )}
    </SurfaceCard>
  );
};

export default SetupChecklist;
