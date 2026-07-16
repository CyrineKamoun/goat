"use client";

import { zodResolver } from "@hookform/resolvers/zod";
import { LoadingButton } from "@mui/lab";
import {
  Alert,
  Box,
  Button,
  Checkbox,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  Divider,
  FormControlLabel,
  FormGroup,
  IconButton,
  List,
  ListItem,
  ListItemSecondaryAction,
  ListItemText,
  MenuItem,
  Skeleton,
  Stack,
  TextField,
  Tooltip,
  Typography,
  useTheme,
} from "@mui/material";
import React, { useMemo, useState } from "react";
import { useForm } from "react-hook-form";
import { useTranslation } from "react-i18next";
import { toast } from "react-toastify";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import {
  createOrganizationAnalytics,
  deleteOrganizationAnalytics,
  setAnalyticsDashboards,
  updateOrganizationAnalytics,
  useOrganizationAnalytics,
  useOrganizationAnalyticsDashboards,
} from "@/lib/api/organizationAnalytics";
import { useOrganization } from "@/lib/api/users";
import type {
  OrganizationAnalytics,
  OrganizationAnalyticsCreate,
} from "@/lib/validations/organizationAnalytics";
import { organizationAnalyticsCreateSchema } from "@/lib/validations/organizationAnalytics";

import ConfirmModal from "@/components/modals/Confirm";

const EMPTY_FORM: OrganizationAnalyticsCreate = {
  name: "",
  provider: "matomo",
  config: { provider: "matomo", url: "", site_id: "" },
};

const WhiteLabelAnalyticsPage = () => {
  const theme = useTheme();
  const { t } = useTranslation("common");
  const { organization, isLoading: isOrgLoading } = useOrganization();
  const orgId = organization?.id;
  const { analyticsList, isLoading: isAnalyticsLoading, mutate } =
    useOrganizationAnalytics(orgId);

  const [formOpen, setFormOpen] = useState(false);
  const [editing, setEditing] = useState<OrganizationAnalytics | null>(null);
  const [removing, setRemoving] = useState<OrganizationAnalytics | null>(null);
  const [isSaveBusy, setIsSaveBusy] = useState(false);
  const [isRemoveBusy, setIsRemoveBusy] = useState(false);

  const {
    dashboards,
    isLoading: isDashboardsLoading,
    mutate: mutateDashboards,
  } = useOrganizationAnalyticsDashboards(orgId);
  const [managing, setManaging] = useState<OrganizationAnalytics | null>(null);
  const [checkedIds, setCheckedIds] = useState<string[]>([]);
  const [dashboardFilter, setDashboardFilter] = useState("");
  const [isDashboardsSaveBusy, setIsDashboardsSaveBusy] = useState(false);

  const instanceNameById = useMemo(() => {
    const map = new Map<string, string>();
    analyticsList.forEach((a) => map.set(a.id, a.name));
    return map;
  }, [analyticsList]);

  const visibleDashboards = useMemo(() => {
    const f = dashboardFilter.trim().toLowerCase();
    if (!f) return dashboards;
    return dashboards.filter((d) => d.name.toLowerCase().includes(f));
  }, [dashboards, dashboardFilter]);

  const {
    register,
    handleSubmit,
    reset,
    formState: { errors, isValid },
  } = useForm<OrganizationAnalyticsCreate>({
    mode: "onChange",
    resolver: zodResolver(organizationAnalyticsCreateSchema),
    defaultValues: EMPTY_FORM,
  });

  const openCreate = () => {
    setEditing(null);
    reset(EMPTY_FORM);
    setFormOpen(true);
  };

  const openEdit = (instance: OrganizationAnalytics) => {
    setEditing(instance);
    reset({
      name: instance.name,
      provider: "matomo",
      config: {
        provider: "matomo",
        url: (instance.config as { url?: string }).url ?? "",
        site_id: (instance.config as { site_id?: string }).site_id ?? "",
      },
    });
    setFormOpen(true);
  };

  const dismissForm = () => {
    setFormOpen(false);
    setEditing(null);
  };

  const closeForm = () => {
    if (isSaveBusy) return;
    dismissForm();
  };

  const onSubmit = async (data: OrganizationAnalyticsCreate) => {
    if (!orgId) return;
    setIsSaveBusy(true);
    try {
      if (editing) {
        await updateOrganizationAnalytics(orgId, editing.id, data);
      } else {
        await createOrganizationAnalytics(orgId, data);
      }
      toast.success(
        t("white_label_analytics_save_success", "Analytics configuration saved")
      );
      await mutate();
      dismissForm();
    } catch {
      toast.error(t("white_label_analytics_save_error", "Save failed"));
    } finally {
      setIsSaveBusy(false);
    }
  };

  const handleRemove = async () => {
    if (!orgId || !removing || isRemoveBusy) return;
    setIsRemoveBusy(true);
    try {
      await deleteOrganizationAnalytics(orgId, removing.id);
      toast.success(
        t("white_label_analytics_remove_success", "Analytics configuration removed")
      );
      setRemoving(null);
      await mutate();
    } catch {
      toast.error(t("white_label_analytics_remove_error", "Remove failed"));
    } finally {
      setIsRemoveBusy(false);
    }
  };

  const openManage = (instance: OrganizationAnalytics) => {
    setManaging(instance);
    setDashboardFilter("");
    setCheckedIds(
      dashboards.filter((d) => d.analytics_id === instance.id).map((d) => d.project_id)
    );
  };

  const closeManage = () => {
    if (isDashboardsSaveBusy) return;
    setManaging(null);
  };

  const toggleDashboard = (projectId: string) => {
    setCheckedIds((prev) =>
      prev.includes(projectId) ? prev.filter((id) => id !== projectId) : [...prev, projectId]
    );
  };

  const handleManageSave = async () => {
    if (!orgId || !managing || isDashboardsSaveBusy) return;
    setIsDashboardsSaveBusy(true);
    try {
      const updated = await setAnalyticsDashboards(orgId, managing.id, checkedIds);
      await mutateDashboards(updated, { revalidate: false });
      await mutate();
      toast.success(
        t("white_label_analytics_dashboards_save_success", "Dashboard assignments saved")
      );
      setManaging(null);
    } catch {
      toast.error(
        t("white_label_analytics_dashboards_save_error", "Failed to save dashboard assignments")
      );
    } finally {
      setIsDashboardsSaveBusy(false);
    }
  };

  const isLoading = isOrgLoading || isAnalyticsLoading;

  return (
    <Box sx={{ p: 4 }}>
      <Stack spacing={theme.spacing(6)}>
        <Divider />
        <Stack direction="row" justifyContent="space-between" alignItems="center">
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
          <Button
            variant="contained"
            disableElevation
            startIcon={<Icon iconName={ICON_NAME.PLUS} fontSize="small" />}
            onClick={openCreate}>
            {t("white_label_analytics_add", "Add analytics")}
          </Button>
        </Stack>
        <Divider />

        {isLoading ? (
          <>
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

            {analyticsList.length === 0 && (
              <Typography variant="body2" color="text.secondary">
                {t(
                  "white_label_analytics_empty",
                  "No analytics instances yet. Add one to enable tracking on your published dashboards."
                )}
              </Typography>
            )}

            {analyticsList.length > 0 && (
              <List disablePadding sx={{ bgcolor: "background.paper" }}>
                {analyticsList.map((instance) => (
                  <Stack key={instance.id}>
                    <ListItem>
                      <ListItemText
                        primary={instance.name}
                        secondary={
                          <>
                            {(instance.config as { url?: string }).url}
                            {" · "}
                            {t("white_label_analytics_site_id", "Site ID")}{" "}
                            {(instance.config as { site_id?: string }).site_id}
                            {instance.usage_count > 0 && (
                              <>
                                {" — "}
                                {t(
                                  "white_label_analytics_in_use",
                                  "Used by {{count}} published dashboard(s)",
                                  { count: instance.usage_count }
                                )}
                              </>
                            )}
                          </>
                        }
                      />
                      <ListItemSecondaryAction>
                        <Tooltip
                          title={t("white_label_analytics_manage_dashboards", "Manage dashboards")}>
                          <span>
                            <IconButton
                              size="small"
                              disabled={isDashboardsLoading}
                              onClick={() => openManage(instance)}>
                              <Icon iconName={ICON_NAME.MAP} style={{ fontSize: 16 }} />
                            </IconButton>
                          </span>
                        </Tooltip>
                        <Tooltip title={t("edit", "Edit")}>
                          <IconButton size="small" onClick={() => openEdit(instance)}>
                            <Icon iconName={ICON_NAME.EDIT} style={{ fontSize: 16 }} />
                          </IconButton>
                        </Tooltip>
                        <Tooltip title={t("white_label_analytics_remove", "Remove")}>
                          <IconButton
                            size="small"
                            color="error"
                            onClick={() => setRemoving(instance)}>
                            <Icon iconName={ICON_NAME.TRASH} style={{ fontSize: 16 }} />
                          </IconButton>
                        </Tooltip>
                      </ListItemSecondaryAction>
                    </ListItem>
                    <Divider />
                  </Stack>
                ))}
              </List>
            )}
          </>
        )}
      </Stack>

      <Dialog open={formOpen} onClose={closeForm} fullWidth maxWidth="sm">
        <Box component="form" onSubmit={handleSubmit(onSubmit)}>
          <DialogTitle>
            {editing
              ? t("edit", "Edit")
              : t("white_label_analytics_add", "Add analytics")}
          </DialogTitle>
          <DialogContent>
            <Stack spacing={theme.spacing(6)} sx={{ mt: 2 }}>
              <TextField
                required
                label={t("white_label_analytics_name", "Name")}
                placeholder="Client XY Matomo"
                size="medium"
                disabled={isSaveBusy}
                helperText={
                  errors.name?.message ??
                  t(
                    "white_label_analytics_name_help",
                    'A label to tell instances apart, e.g. "Client XY Matomo".'
                  )
                }
                error={Boolean(errors.name)}
                {...register("name")}
              />
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
            </Stack>
          </DialogContent>
          <DialogActions disableSpacing sx={{ pb: 2 }}>
            <Button onClick={closeForm} variant="text" disabled={isSaveBusy} sx={{ borderRadius: 0 }}>
              <Typography variant="body2" fontWeight="bold">
                {t("cancel", "Cancel")}
              </Typography>
            </Button>
            <LoadingButton type="submit" variant="text" loading={isSaveBusy} disabled={!isValid}>
              <Typography variant="body2" fontWeight="bold" color="inherit">
                {t("save", "Save")}
              </Typography>
            </LoadingButton>
          </DialogActions>
        </Box>
      </Dialog>

      <ConfirmModal
        open={removing !== null}
        title={t("white_label_analytics_remove", "Remove")}
        body={
          removing && removing.usage_count > 0
            ? `${t("white_label_analytics_remove_confirm", "Remove this analytics instance?")} ${t(
                "white_label_analytics_remove_confirm_used",
                "It is used by {{count}} published dashboard(s) — tracking will stop on them.",
                { count: removing.usage_count }
              )}`
            : t("white_label_analytics_remove_confirm", "Remove this analytics instance?")
        }
        onClose={() => {
          if (!isRemoveBusy) setRemoving(null);
        }}
        onConfirm={handleRemove}
        closeText={t("cancel", "Cancel")}
        confirmText={t("white_label_analytics_remove", "Remove")}
      />

      <Dialog open={managing !== null} onClose={closeManage} fullWidth maxWidth="sm">
        <DialogTitle>
          {t("white_label_analytics_manage_dashboards", "Manage dashboards")}
          {managing ? ` — ${managing.name}` : ""}
        </DialogTitle>
        <DialogContent sx={{ display: "flex", flexDirection: "column", overflow: "hidden" }}>
          {dashboards.length === 0 && (
            <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
              {t(
                "white_label_analytics_dashboards_empty",
                "No published dashboards yet. Publish a project to assign analytics."
              )}
            </Typography>
          )}
          {dashboards.length > 0 && (
            <>
              <TextField
                size="small"
                placeholder={t("white_label_analytics_dashboards_filter", "Filter by name…")}
                value={dashboardFilter}
                onChange={(e) => setDashboardFilter(e.target.value)}
                sx={{ mt: 1, mb: 4, flexShrink: 0 }}
              />
              <Box sx={{ overflowY: "auto" }}>
                <FormGroup>
                  {visibleDashboards.map((d) => {
                    const otherName =
                      d.analytics_id && managing && d.analytics_id !== managing.id
                        ? instanceNameById.get(d.analytics_id)
                        : undefined;
                    return (
                      <FormControlLabel
                        key={d.project_id}
                        control={
                          <Checkbox
                            checked={checkedIds.includes(d.project_id)}
                            disabled={isDashboardsSaveBusy}
                            onChange={() => toggleDashboard(d.project_id)}
                          />
                        }
                        label={
                          <Stack>
                            <Typography variant="body2">{d.name}</Typography>
                            {otherName && (
                              <Typography variant="caption" color="text.secondary">
                                {t(
                                  "white_label_analytics_currently_assigned",
                                  "currently: {{name}}",
                                  { name: otherName }
                                )}
                              </Typography>
                            )}
                          </Stack>
                        }
                      />
                    );
                  })}
                </FormGroup>
              </Box>
            </>
          )}
        </DialogContent>
        <DialogActions disableSpacing sx={{ pb: 2 }}>
          <Button
            onClick={closeManage}
            variant="text"
            disabled={isDashboardsSaveBusy}
            sx={{ borderRadius: 0 }}>
            <Typography variant="body2" fontWeight="bold">
              {t("cancel", "Cancel")}
            </Typography>
          </Button>
          <LoadingButton
            variant="text"
            loading={isDashboardsSaveBusy}
            disabled={dashboards.length === 0}
            onClick={handleManageSave}>
            <Typography variant="body2" fontWeight="bold" color="inherit">
              {t("save", "Save")}
            </Typography>
          </LoadingButton>
        </DialogActions>
      </Dialog>
    </Box>
  );
};

export default WhiteLabelAnalyticsPage;
