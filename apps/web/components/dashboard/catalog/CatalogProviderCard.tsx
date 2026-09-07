"use client";

import { Box, Link as MuiLink, Stack, Typography, useTheme } from "@mui/material";
import { useTranslation } from "react-i18next";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import type { CatalogCollection } from "@/lib/validations/catalog";

import { SectionCard } from "@/components/dashboard/common/DetailChrome";

/** Who produced the dataset, and where its own metadata record lives. */
const CatalogProviderCard = ({
  providers,
  publisher,
  sourceHref,
}: {
  providers?: CatalogCollection["providers"];
  publisher?: string | null;
  sourceHref?: string;
}) => {
  const { t } = useTranslation("common");
  const theme = useTheme();

  const producer = providers?.find((provider) => provider.roles?.includes("producer")) ?? providers?.[0];
  const name = producer?.name ?? publisher;
  if (!name) return null;

  return (
    <Box sx={{ mt: 4 }}>
      <SectionCard title={t("metadata.headings.publisher")} pad={4.5}>
        <Stack direction="row" spacing={2.5} alignItems="flex-start">
          <Box
            sx={{
              width: 28,
              height: 28,
              mt: 0.25,
              borderRadius: 1.5,
              flexShrink: 0,
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              backgroundColor: theme.palette.action.hover,
            }}>
            <Icon
              iconName={ICON_NAME.ORGANIZATION}
              style={{ fontSize: 15 }}
              htmlColor={theme.palette.primary.main}
            />
          </Box>
          <Stack spacing={1} sx={{ minWidth: 0 }}>
            {producer?.url ? (
              <MuiLink
                href={producer.url}
                target="_blank"
                rel="noreferrer noopener"
                underline="hover"
                sx={{ fontSize: 14, fontWeight: 600, color: "text.primary", lineHeight: 1.35 }}>
                {name}
              </MuiLink>
            ) : (
              <Typography sx={{ fontSize: 14, fontWeight: 600, lineHeight: 1.35 }}>{name}</Typography>
            )}
            {/* The provider's own description often carries the contact address, which is the only route to a correction. */}
            {producer?.description && (
              <Typography variant="caption" color="text.secondary" sx={{ lineHeight: 1.45 }}>
                {producer.description}
              </Typography>
            )}
            {sourceHref && (
              <MuiLink
                href={sourceHref}
                target="_blank"
                rel="noreferrer noopener"
                underline="none"
                sx={{
                  display: "inline-flex",
                  alignItems: "center",
                  gap: 1.25,
                  fontSize: 12.5,
                  fontWeight: 600,
                }}>
                {t("catalog_source_metadata")}
                <Icon
                  iconName={ICON_NAME.EXTERNAL_LINK}
                  style={{ fontSize: 11 }}
                  htmlColor={theme.palette.primary.main}
                />
              </MuiLink>
            )}
          </Stack>
        </Stack>
      </SectionCard>
    </Box>
  );
};

export default CatalogProviderCard;
