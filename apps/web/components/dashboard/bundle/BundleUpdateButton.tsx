import { Button } from "@mui/material";
import React, { useState } from "react";
import { useTranslation } from "react-i18next";
import { toast } from "react-toastify";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import { rebuildBundleArtifact } from "@/lib/api/bundleEdits";
import type { BundleRead } from "@/lib/api/bundles";
import { setRunningJobIds } from "@/lib/store/jobs/slice";
import { bundleIsBuilding, bundleNeedsRebuild } from "@/lib/utils/bundleMeta";

import { useAppDispatch, useAppSelector } from "@/hooks/store/ContextHooks";

/**
 * Rebuilding a bundle's derived artifacts.
 *
 * Shown only where it would do something — `bundleNeedsRebuild` holds the rule,
 * which is why this can sit anywhere a bundle is read without each caller
 * deciding when a rebuild is possible.
 *
 * A bundle whose artifact has failed is what a user comes to the detail view to
 * fix, so the button belongs wherever the status that prompts it is shown. The
 * job is tracked like any other, so progress and completion appear in the jobs
 * popper rather than going silent.
 */
const BundleUpdateButton = ({
  bundle,
  size,
}: {
  bundle: BundleRead;
  /** `small` where the button sits in a panel column rather than a header. */
  size?: "small" | "medium";
}) => {
  const { t } = useTranslation("common");
  const dispatch = useAppDispatch();
  const runningJobIds = useAppSelector((state) => state.jobs.runningJobIds);
  const [isRebuilding, setIsRebuilding] = useState(false);

  if (!bundleNeedsRebuild(bundle)) return null;

  return (
    <Button
      variant="outlined"
      size={size}
      sx={{ textTransform: "none" }}
      disabled={isRebuilding || bundleIsBuilding(bundle)}
      startIcon={<Icon iconName={ICON_NAME.REFRESH} style={{ fontSize: 13 }} />}
      onClick={async () => {
        setIsRebuilding(true);
        try {
          const job = await rebuildBundleArtifact(bundle.id);
          if (job?.jobID) dispatch(setRunningJobIds([...runningJobIds, job.jobID]));
          toast.success(t("bundle_update_started"));
        } catch {
          toast.error(t("bundle_update_failed_to_start"));
        } finally {
          setIsRebuilding(false);
        }
      }}>
      {t("update_bundle")}
    </Button>
  );
};

export default BundleUpdateButton;
