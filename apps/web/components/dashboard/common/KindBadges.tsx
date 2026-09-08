"use client";

import { Box, Tooltip, useTheme } from "@mui/material";
import { useTranslation } from "react-i18next";

import { Icon } from "@p4b/ui/components/Icon";

import { TEMPLATE_KIND_ICON } from "@/lib/utils/templates";
import type { TemplateKind } from "@/lib/validations/template";

interface KindBadgesProps {
  kinds: TemplateKind[];
  /** Circle diameter in px — the glyph itself is drawn at half that. */
  size?: number;
}

const DEFAULT_SIZE = 22;

/** A template's kinds (T1), one small circle per kind with its name in the
 * tooltip. Shared by `StarterCard` (over a thumbnail) and the Content
 * card/details panel (over plain paper) — a solid `background.paper` disc
 * with a rest shadow, the same treatment `ContentCard`'s pin button uses to
 * stay legible on either ground, rather than the dark scrim `TypeTag` draws
 * for a label that only ever sits on imagery. */
const KindBadges = ({ kinds, size = DEFAULT_SIZE }: KindBadgesProps) => {
  const { t } = useTranslation("common");
  const theme = useTheme();

  return (
    <Box sx={{ display: "flex", gap: "4px" }}>
      {kinds.map((kind) => (
        <Tooltip key={kind} title={t(`template_kind_${kind}`)} placement="top" disableInteractive>
          <Box
            sx={{
              width: size,
              height: size,
              flexShrink: 0,
              borderRadius: "50%",
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              backgroundColor: theme.palette.background.paper,
              boxShadow: theme.shadows[1],
            }}>
            <Icon
              iconName={TEMPLATE_KIND_ICON[kind]}
              style={{ fontSize: size * 0.5 }}
              htmlColor={theme.palette.secondary.main}
            />
          </Box>
        </Tooltip>
      ))}
    </Box>
  );
};

export default KindBadges;
