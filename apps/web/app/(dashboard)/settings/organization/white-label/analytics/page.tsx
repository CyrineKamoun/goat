"use client";

import { zodResolver } from "@hookform/resolvers/zod";
import { LoadingButton } from "@mui/lab";
import {
  Alert,
  Box,
  Divider,
  MenuItem,
  Skeleton,
  Stack,
  TextField,
  Typography,
  useTheme,
} from "@mui/material";
import React, { useEffect, useMemo, useState } from "react";
import { useForm } from "react-hook-form";
import { useTranslation } from "react-i18next";
import { toast } from "react-toastify";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import {
  deleteOrganizationAnalytics,
  upsertOrganizationAnalytics,
  useOrganizationAnalytics,
} from "@/lib/api/organizationAnalytics";
import { useOrganization } from "@/lib/api/users";
import type { OrganizationAnalyticsCreate } from "@/lib/validations/organizationAnalytics";
import { organizationAnalyticsCreateSchema } from "@/lib/validations/organizationAnalytics";

import ConfirmModal from "@/components/modals/Confirm";

const WhiteLabelAnalyticsPage = () => {
  const theme = useTheme();
  const { t } = useTranslation("common");
  const { organization, isLoading: isOrgLoading } = useOrganization();
  const orgId = organization?.id;
  const { analytics, isLoading: isAnalyticsLoading, mutate } =
    useOrganizationAnalytics(orgId);

  const [isSaveBusy, setIsSaveBusy] = useState(false);
  const [isRemoveBusy, setIsRemoveBusy] = useState(false);
  const [confirmRemoveOpen, setConfirmRemoveOpen] = useState(false);

  const defaultValues = useMemo<OrganizationAnalyticsCreate>(() => {
    if (analytics) {
      return {
        provider: "matomo",
        config: {
          provider: "matomo",
          url: (analytics.config as { url?: string }).url ?? "",
          site_id: (analytics.config as { site_id?: string }).site_id ?? "",
        },
      };
    }
    return {
      provider: "matomo",
      config: { provider: "matomo", url: "", site_id: "" },
    };
  }, [analytics]);

  const {
    register,
    handleSubmit,
    reset,
    formState: { errors, isDirty, isValid },
  } = useForm<OrganizationAnalyticsCreate>({
    mode: "onChange",
    resolver: zodResolver(organizationAnalyticsCreateSchema),
    defaultValues,
  });

  useEffect(() => {
    reset(defaultValues);
  }, [defaultValues, reset]);

  const onSubmit = async (data: OrganizationAnalyticsCreate) => {
    if (!orgId) return;
    setIsSaveBusy(true);
    try {
      await upsertOrganizationAnalytics(orgId, data);
      toast.success(
        t("white_label_analytics_save_success", "Analytics configuration saved")
      );
      reset({}, { keepValues: true });
      await mutate();
    } catch {
      toast.error(t("white_label_analytics_save_error", "Save failed"));
    } finally {
      setIsSaveBusy(false);
    }
  };

  const handleRemove = async () => {
    if (!orgId) return;
    setIsRemoveBusy(true);
    try {
      await deleteOrganizationAnalytics(orgId);
      toast.success(
        t("white_label_analytics_remove_success", "Analytics configuration removed")
      );
      setConfirmRemoveOpen(false);
      await mutate(null, { revalidate: false });
      reset({
        provider: "matomo",
        config: { provider: "matomo", url: "", site_id: "" },
      });
    } catch {
      toast.error(t("white_label_analytics_remove_error", "Remove failed"));
    } finally {
      setIsRemoveBusy(false);
    }
  };

  const isLoading = isOrgLoading || isAnalyticsLoading;
  const hasConfig = analytics !== null && analytics !== undefined;

  return (
    <Box sx={{ p: 4 }}>
      <Box component="form" onSubmit={handleSubmit(onSubmit)}>
        <Stack spacing={theme.spacing(6)}>
          <Divider />
          <Box>
            <Typography variant="body1" fontWeight="bold">
              {t("white_label_analytics_title", "Analytics")}
            </Typography>
            <Typography variant="caption">
              {t(
                "white_label_analytics_description",
                "Configure analytics tracking for your published dashboards. Per-project opt-in via the Share dialog."
              )}
            </Typography>
          </Box>
          <Divider />

          {isLoading ? (
            <>
              <Skeleton variant="rectangular" height={56} />
              <Skeleton variant="rectangular" height={56} />
              <Skeleton variant="rectangular" height={56} />
            </>
          ) : (
            <>
              <Alert severity="info" icon={<Icon iconName={ICON_NAME.INFO} fontSize="small" />}>
                {t(
                  "white_label_analytics_setup_hints",
                  "Add each of your custom domains to your Matomo site's URL list, and configure Custom Dimension 1 as \"Project ID\" so per-dashboard breakdown works in Matomo."
                )}
              </Alert>

              <TextField
                select
                label={t("white_label_analytics_provider", "Provider")}
                size="medium"
                defaultValue="matomo"
                disabled={isSaveBusy}
                {...register("provider")}>
                <MenuItem value="matomo">Matomo</MenuItem>
              </TextField>

              <TextField
                required
                label={t("white_label_analytics_matomo_url", "Matomo URL")}
                placeholder="https://matomo.example.org/"
                size="medium"
                disabled={isSaveBusy}
                helperText={
                  errors.config?.url?.message ??
                  t(
                    "white_label_analytics_matomo_url_help",
                    "Your Matomo instance, including trailing slash."
                  )
                }
                error={Boolean(errors.config?.url)}
                {...register("config.url")}
              />

              <TextField
                required
                label={t("white_label_analytics_site_id", "Site ID")}
                placeholder="5"
                size="medium"
                disabled={isSaveBusy}
                helperText={
                  errors.config?.site_id?.message ??
                  t(
                    "white_label_analytics_site_id_help",
                    "Found in Matomo → Administration → Websites."
                  )
                }
                error={Boolean(errors.config?.site_id)}
                {...register("config.site_id")}
              />

              <Stack direction="row" alignItems="center" justifyContent="flex-end" spacing={2}>
                {hasConfig && (
                  <LoadingButton
                    variant="outlined"
                    color="error"
                    startIcon={<Icon iconName={ICON_NAME.TRASH} fontSize="small" />}
                    loading={isRemoveBusy}
                    disabled={isSaveBusy}
                    onClick={() => setConfirmRemoveOpen(true)}>
                    {t("white_label_analytics_remove", "Remove")}
                  </LoadingButton>
                )}
                <LoadingButton
                  type="submit"
                  variant="contained"
                  startIcon={<Icon iconName={ICON_NAME.SAVE} fontSize="small" />}
                  loading={isSaveBusy}
                  disabled={isSaveBusy || isRemoveBusy || !isDirty || !isValid}>
                  {t("save", "Save")}
                </LoadingButton>
              </Stack>
            </>
          )}
        </Stack>
      </Box>

      <ConfirmModal
        open={confirmRemoveOpen}
        title={t("white_label_analytics_remove", "Remove")}
        body={t(
          "white_label_analytics_remove_confirm",
          "Remove analytics configuration? Tracking will stop on all published dashboards in this organization."
        )}
        onClose={() => setConfirmRemoveOpen(false)}
        onConfirm={handleRemove}
        closeText={t("cancel", "Cancel")}
        confirmText={t("white_label_analytics_remove", "Remove")}
      />
    </Box>
  );
};

export default WhiteLabelAnalyticsPage;
