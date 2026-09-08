import { Box, Chip, Paper, Skeleton, Stack, Typography, useTheme } from "@mui/material";
import dynamic from "next/dynamic";
import React, { useState } from "react";
import { useTranslation } from "react-i18next";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import type { BundleMember } from "@/lib/api/bundles";

const ContentPreviewDialog = dynamic(() => import("@/components/dashboard/content/ContentPreviewDialog"), {
  ssr: false,
});

interface BundleLayersProps {
  members?: BundleMember[];
  isLoading?: boolean;
}

const geometryIcons: Record<string, ICON_NAME> = {
  point: ICON_NAME.POINT_FEATURE,
  line: ICON_NAME.LINE_FEATURE,
  polygon: ICON_NAME.POLYGON_FEATURE,
};

const memberIcon = (member: BundleMember): ICON_NAME => {
  if (member.type === "table") return ICON_NAME.TABLE;
  if (member.feature_layer_geometry_type) {
    return geometryIcons[member.feature_layer_geometry_type] ?? ICON_NAME.LAYERS;
  }
  return ICON_NAME.LAYERS;
};

/** Member layers of a bundle. Membership is fixed by the bundle's spec, so this
 *  lists rather than edits — each row opens the layer in the preview dialog. */
const BundleLayers: React.FC<BundleLayersProps> = ({ members, isLoading }) => {
  const theme = useTheme();
  const { t } = useTranslation("common");
  const [previewLayerId, setPreviewLayerId] = useState<string | null>(null);

  if (isLoading) {
    return (
      <Stack spacing={2}>
        {Array.from(new Array(3)).map((_, index) => (
          <Skeleton key={index} variant="rectangular" height={56} />
        ))}
      </Stack>
    );
  }

  if (!members?.length) {
    return (
      <Typography variant="body2" color="text.secondary">
        {t("no_datasets_found")}
      </Typography>
    );
  }

  return (
    <Stack spacing={2}>
      {members.map((member) => (
        <Paper
          key={member.layer_id}
          elevation={1}
          onClick={() => setPreviewLayerId(member.layer_id)}
          sx={{
            p: 3,
            cursor: "pointer",
            "&:hover": { backgroundColor: theme.palette.action.hover },
          }}>
          <Stack direction="row" spacing={3} alignItems="center">
            <Icon
              iconName={memberIcon(member)}
              style={{ fontSize: 16, flexShrink: 0 }}
              htmlColor={theme.palette.text.secondary}
            />
            <Box sx={{ minWidth: 0, flexGrow: 1 }}>
              <Typography variant="body2" fontWeight="bold" noWrap>
                {member.name ?? member.layer_id}
              </Typography>
            </Box>
            {member.role && <Chip size="small" label={member.role.replace(/_/g, " ")} />}
          </Stack>
        </Paper>
      ))}
      {previewLayerId && (
        <ContentPreviewDialog layerId={previewLayerId} onClose={() => setPreviewLayerId(null)} />
      )}
    </Stack>
  );
};

export default BundleLayers;
