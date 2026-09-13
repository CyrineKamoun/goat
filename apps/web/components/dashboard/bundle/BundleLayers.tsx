import { Skeleton, Stack, Typography } from "@mui/material";
import dynamic from "next/dynamic";
import React, { useState } from "react";
import { useTranslation } from "react-i18next";

import type { BundleMember } from "@/lib/api/bundles";
import type { MarkKind } from "@/lib/catalog/kind";

import { useCatalogLabels } from "@/hooks/catalog/useCatalogLabels";

import BundleItemRow from "@/components/dashboard/bundle/BundleItemRow";

const ContentPreviewDialog = dynamic(() => import("@/components/dashboard/content/ContentPreviewDialog"), {
  ssr: false,
});

interface BundleLayersProps {
  members?: BundleMember[];
  isLoading?: boolean;
}

/** A member's own type, in the mark's vocabulary. */
const markKindOfMember = (member: BundleMember): MarkKind => {
  if (member.type === "table") return "table";
  if (member.type === "raster") return "raster";
  if (member.type === "feature") return "vector";
  return "unknown";
};

/** Member layers of a bundle. Membership is fixed by the bundle's spec, so this
 *  lists rather than edits — each row opens the layer in the preview dialog.
 *
 *  The role follows the type on the second line, the way the feed's own rows
 *  append a location there: it is the only thing telling two members of the
 *  same shape apart — a street network's edges and its nodes are both lines and
 *  points, and "edges" is what says which is which. */
const BundleLayers: React.FC<BundleLayersProps> = ({ members, isLoading }) => {
  const { t } = useTranslation("common");
  const labels = useCatalogLabels();
  const [previewLayerId, setPreviewLayerId] = useState<string | null>(null);

  if (isLoading) {
    return (
      <Stack spacing={2}>
        {Array.from(new Array(3)).map((_, index) => (
          <Skeleton key={index} variant="rectangular" height={56} sx={{ borderRadius: 2 }} />
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
    <>
      <Stack spacing={2}>
        {members.map((member) => {
          const typeLabel =
            member.type === "table"
              ? t("table")
              : labels.geometryLabel(member.feature_layer_geometry_type ?? undefined);
          const role = member.role ? member.role.replace(/_/g, " ") : "";
          return (
            <BundleItemRow
              key={member.layer_id}
              kind={markKindOfMember(member)}
              geometryType={member.feature_layer_geometry_type}
              title={member.name ?? member.layer_id}
              subtitle={[typeLabel, role].filter(Boolean).join(" · ")}
              onClick={() => setPreviewLayerId(member.layer_id)}
            />
          );
        })}
      </Stack>
      {previewLayerId && (
        <ContentPreviewDialog layerId={previewLayerId} onClose={() => setPreviewLayerId(null)} />
      )}
    </>
  );
};

export default BundleLayers;
