import { Stack, Typography } from "@mui/material";
import React from "react";
import { useTranslation } from "react-i18next";

import type { BundleDependency } from "@/lib/api/bundles";

import BundleItemRow from "@/components/dashboard/bundle/BundleItemRow";

interface BundleDependenciesProps {
  dependencies?: BundleDependency[];
  /** Reads the dependency in place of this bundle. Without it the rows are
   * inert: a dependency the caller offers no way of opening should not look
   * like it can be. */
  onOpenBundle?: (bundleId: string) => void;
}

/** The bundles this one is built against — a PT network's street network, say.
 *  Written as the content feed writes a bundle, so they read as bundles rather
 *  than as a list invented for this tab. */
const BundleDependencies: React.FC<BundleDependenciesProps> = ({ dependencies, onOpenBundle }) => {
  const { t, i18n } = useTranslation("common");

  if (!dependencies?.length) {
    return (
      <Typography variant="body2" color="text.secondary">
        {t("no_bundle_dependencies")}
      </Typography>
    );
  }

  return (
    <Stack spacing={2}>
      {dependencies.map((dependency) => (
        <BundleItemRow
          key={`${dependency.dependency_kind}-${dependency.depends_on_bundle_id}`}
          kind="bundle"
          title={dependency.depends_on_name}
          subtitle={
            i18n.exists(`common:${dependency.depends_on_type}`)
              ? t(dependency.depends_on_type)
              : dependency.depends_on_type
          }
          onClick={onOpenBundle ? () => onOpenBundle(dependency.depends_on_bundle_id) : undefined}
        />
      ))}
    </Stack>
  );
};

export default BundleDependencies;
