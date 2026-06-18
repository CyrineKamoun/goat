import KeyboardArrowDownIcon from "@mui/icons-material/KeyboardArrowDown";
import { LoadingButton } from "@mui/lab";
import {
  Avatar,
  Box,
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  Divider,
  FormControl,
  IconButton,
  List,
  ListItem,
  ListItemAvatar,
  ListItemSecondaryAction,
  ListItemText,
  Menu,
  MenuItem,
  MenuList,
  Select,
  Stack,
  Switch,
  Tab,
  Tabs,
  Tooltip,
  Typography,
  useTheme,
} from "@mui/material";
import type { SelectChangeEvent } from "@mui/material";
import { formatDistance } from "date-fns";
import { useEffect, useMemo, useRef, useState } from "react";
import { Trans, useTranslation } from "react-i18next";
import { toast } from "react-toastify";
import { mutate } from "swr";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";
import { Loading } from "@p4b/ui/components/Loading";

import { useDateFnsLocale } from "@/i18n/utils";

import {
  assignDomainToProject,
  unassignDomainFromProject,
  useOrganizationDomains,
} from "@/lib/api/customDomains";
import {
  setProjectTrackingEnabled,
  setProjectTrackingRequireConsent,
  useOrganizationAnalytics,
} from "@/lib/api/organizationAnalytics";
import {
  FOLDERS_API_BASE_URL,
  deleteFolderGrant,
  shareFolderGrant,
  useFolderGrants,
} from "@/lib/api/folders";
import { LAYERS_API_BASE_URL } from "@/lib/api/layers";
import {
  PROJECTS_API_BASE_URL,
  publishProject,
  unpublishProject,
  usePublicProject,
} from "@/lib/api/projects";
import { shareLayer, shareProject } from "@/lib/api/share";
import { useTeams } from "@/lib/api/teams";
import { useOrganization } from "@/lib/api/users";
import { ACCOUNTS_DISABLED } from "@/lib/constants";
import { type Layer, layerShareRoleEnum } from "@/lib/validations/layer";
import { type Folder, folderShareRoleEnum } from "@/lib/validations/folder";
import { type Project, projectShareRoleEnum } from "@/lib/validations/project";

import CopyField from "@/components/common/CopyField";
import { CustomTabPanel, a11yProps } from "@/components/common/CustomTabPanel";

interface ShareProps {
  open: boolean;
  onClose?: () => void;
  type: "layer" | "project" | "folder";
  content: Layer | Project | Folder;
}

interface Item {
  id: string;
  name: string;
  avatar: string;
  role: string;
  inheritedRole?: string;
}

interface ShareWithItemsTabProps {
  items: Item[];
  roleOptions: string[];
  onRoleChange: (id: string, role: string) => void;
  disableInherited?: boolean;
}

interface ShareWithPublicTabProps {
  project: Project;
}

const ShareWithItemsTab: React.FC<ShareWithItemsTabProps> = ({ items, roleOptions, onRoleChange, disableInherited = false }) => {
  const [anchorEl, setAnchorEl] = useState<null | HTMLElement>(null);
  const [selectedItemId, setSelectedItemId] = useState<string | null>(null);
  const open = Boolean(anchorEl);
  const { t } = useTranslation("common");

  const handleClick = (event: React.MouseEvent<HTMLButtonElement>, itemId: string) => {
    setAnchorEl(event.currentTarget);
    setSelectedItemId(itemId);
  };

  const handleClose = () => {
    setAnchorEl(null);
    setSelectedItemId(null);
  };

  const handleRoleChange = (role: string) => {
    if (selectedItemId) {
      onRoleChange(selectedItemId, role);
      handleClose();
    }
  };

  const getRoleTranslation = (role: string) => {
    if (role.includes("editor")) {
      return t("editor");
    } else if (role.includes("viewer")) {
      return t("viewer");
    }
  };

  return (
    <>
      <List disablePadding sx={{ width: "100%", bgcolor: "background.paper" }}>
        {items.map((item) => (
          <Stack key={item.id} justifyContent="center">
            <ListItem>
              <ListItemAvatar>
                <Avatar alt={item.name} src={item.avatar} />
              </ListItemAvatar>
              <ListItemText primary={item.name} />
              <ListItemSecondaryAction
                sx={{
                  display: "flex",
                  alignItems: "center",
                  gap: 1,
                }}>
                {item.inheritedRole && (
                  <Tooltip
                    title={
                      disableInherited
                        ? t("access_via_folder_disable_hint")
                        : t("access_via_folder")
                    }
                    placement="top">
                    <Typography variant="caption" color="text.secondary" sx={{ fontStyle: "italic" }}>
                      {t("via_folder")}
                    </Typography>
                  </Tooltip>
                )}
                <Button
                  variant="text"
                  sx={{ borderRadius: "4px" }}
                  size="small"
                  color="secondary"
                  disabled={disableInherited && Boolean(item.inheritedRole)}
                  onClick={(event) => handleClick(event, item.id)}
                  endIcon={<KeyboardArrowDownIcon color="inherit" />}>
                  <Typography variant="body2" fontWeight="bold" color="inherit">
                    {item.role ? getRoleTranslation(item.role) : item.inheritedRole ? getRoleTranslation(item.inheritedRole) : t("no_access")}
                  </Typography>
                </Button>
              </ListItemSecondaryAction>
            </ListItem>
            <Divider sx={{ mt: 0, pt: 0 }} />
          </Stack>
        ))}
      </List>
      <Menu
        id="role-select-menu"
        anchorEl={anchorEl}
        open={open}
        onClose={handleClose}
        MenuListProps={{
          "aria-labelledby": "role-select-button",
        }}>
        <MenuList dense>
          {roleOptions.map((role) => (
            <MenuItem
              sx={{ width: "110px" }}
              selected={
                selectedItemId ? items.find((item) => item.id === selectedItemId)?.role === role : false
              }
              key={role}
              onClick={() => handleRoleChange(role)}>
              {role ? getRoleTranslation(role) : t("no_access")}
            </MenuItem>
          ))}
        </MenuList>
      </Menu>
    </>
  );
};

const ShareWithPublicTab: React.FC<ShareWithPublicTabProps> = ({ project }) => {
  const { t } = useTranslation("common");
  const theme = useTheme();
  const { sharedProject, isLoading, mutate } = usePublicProject(project.id);
  const { organization } = useOrganization();
  const { domains: orgDomains, mutate: mutateOrgDomains } = useOrganizationDomains(
    organization?.id
  );
  const { analytics: orgAnalytics } = useOrganizationAnalytics(organization?.id);
  const [isPublishing, setIsPublishing] = useState(false);
  const [isUnpublishing, setIsUnpublishing] = useState(false);
  const [isCustomUrlBusy, setIsCustomUrlBusy] = useState(false);
  const [isTrackingBusy, setIsTrackingBusy] = useState(false);
  const [isConsentBusy, setIsConsentBusy] = useState(false);
  const dateLocale = useDateFnsLocale();
  const baseUrl = window.location.origin;
  const publicUrl = `${baseUrl}/map/public/${project.id}`;
  const embedCode = `<iframe src="${publicUrl}" width="100%" height="600" frameborder="0" style="max-width: 100%; border: 1px solid #EAEAEA; border-radius: 4px;"></iframe>`;

  const assignedDomainId = sharedProject?.custom_domain_id ?? null;
  // Domains the user can pick:
  //   - cert is active (so it can actually serve traffic), AND
  //   - not already taken by a different project (the backend rejects
  //     double-assignment, so let's not surface options that error out).
  // The currently-assigned domain is always included so we can render its
  // option even if its cert later regresses (otherwise MUI warns about an
  // out-of-range value).
  const selectableDomains = useMemo(() => {
    const all = orgDomains ?? [];
    return all.filter((d) => {
      if (d.id === assignedDomainId) return true;
      if (d.cert_status !== "active") return false;
      const takenByOther =
        d.assigned_project_id != null && d.assigned_project_id !== project.id;
      return !takenByOther;
    });
  }, [orgDomains, assignedDomainId, project.id]);
  const showCustomUrl =
    Boolean(sharedProject) &&
    (selectableDomains.some((d) => d.id !== assignedDomainId) ||
      assignedDomainId !== null);

  const handleCustomUrlChange = async (event: SelectChangeEvent<string>) => {
    const value = event.target.value;
    setIsCustomUrlBusy(true);
    try {
      if (value === "" || value === "__none__") {
        await unassignDomainFromProject(project.id);
        toast.success(
          t("share_custom_url_unassign_success", "Custom URL removed")
        );
      } else {
        await assignDomainToProject(project.id, value);
        const picked = selectableDomains.find((d) => d.id === value);
        toast.success(
          t("share_custom_url_assign_success", "Custom URL set to {{domain}}", {
            domain: picked?.base_domain ?? "",
          })
        );
      }
      await mutate();
      await mutateOrgDomains();
    } catch (error) {
      const message =
        error instanceof Error
          ? error.message
          : t("error_updating_share_access");
      toast.error(message);
    } finally {
      setIsCustomUrlBusy(false);
    }
  };

  const handleTrackingToggle = async (event: React.ChangeEvent<HTMLInputElement>) => {
    const next = event.target.checked;
    setIsTrackingBusy(true);
    try {
      await setProjectTrackingEnabled(project.id, next);
      toast.success(
        next
          ? t("share_tracking_enabled_success", "Analytics tracking enabled")
          : t("share_tracking_disabled_success", "Analytics tracking disabled")
      );
      await mutate();
    } catch {
      toast.error(t("share_tracking_update_error", "Failed to update tracking"));
    } finally {
      setIsTrackingBusy(false);
    }
  };

  const handleConsentToggle = async (event: React.ChangeEvent<HTMLInputElement>) => {
    const next = event.target.checked;
    setIsConsentBusy(true);
    try {
      await setProjectTrackingRequireConsent(project.id, next);
      await mutate();
    } catch {
      toast.error(t("share_tracking_update_error", "Failed to update tracking"));
    } finally {
      setIsConsentBusy(false);
    }
  };

  const handlePublish = async () => {
    try {
      setIsPublishing(true);
      await publishProject(project.id);
      mutate();
    } catch {
      toast.error(t("error_publishing_project"));
    } finally {
      setIsPublishing(false);
    }
  };

  const handleUnpublish = async () => {
    try {
      setIsUnpublishing(true);
      await unpublishProject(project.id);
      mutate();
    } catch {
      toast.error(t("error_unpublishing_project"));
    } finally {
      setIsUnpublishing(false);
    }
  };

  return (
    <>
      {/* {sharedProject && (
        <Alert variant="outlined" severity="warning" sx={{ my: 2 }}>
          {t("map_has_unsaved")} <br />
          <b>{t("click_republish_to_make_live")}</b>
        </Alert>
      )} */}
      {isLoading && (
        <Box
          sx={{
            minHeight: "80px",
            display: "flex",
            justifyContent: "center",
            alignItems: "center",
          }}>
          <Loading size={40} />
        </Box>
      )}

      {!isLoading && (
        <Stack spacing={4} sx={{ my: 4, mx: 0 }}>
          {/* <Divider /> */}
          <Stack direction="row" justifyContent="space-between" alignItems="center">
            {!sharedProject && (
              <>
                <Stack>
                  <Typography variant="body1">{t("publish_map")}</Typography>
                  <Typography variant="caption">{t("publish_map_description")}</Typography>
                </Stack>
                <LoadingButton
                  variant="contained"
                  color="primary"
                  disableElevation
                  onClick={handlePublish}
                  loading={isPublishing}>
                  {t("publish")}
                </LoadingButton>
              </>
            )}
            {sharedProject && (
              <>
                <Stack>
                  <Typography variant="body1">{t("publish_map")}</Typography>
                  <Typography variant="caption">
                    {t("last_published")}
                    {": "}
                    {formatDistance(new Date(sharedProject.updated_at), new Date(), {
                      addSuffix: true,
                      locale: dateLocale,
                    })}
                  </Typography>
                </Stack>
                <Stack direction="row" spacing={2}>
                  <LoadingButton
                    variant="text"
                    color="error"
                    disableElevation
                    onClick={handleUnpublish}
                    disabled={isLoading || isPublishing}
                    loading={isUnpublishing}>
                    {t("unpublish")}
                  </LoadingButton>
                  <LoadingButton
                    variant="contained"
                    color="primary"
                    disabled={isUnpublishing || isLoading}
                    disableElevation
                    onClick={handlePublish}
                    loading={isPublishing}>
                    {t("republish")}
                  </LoadingButton>
                </Stack>
              </>
            )}
          </Stack>
          <Divider />
          {/* Public URL  */}
          {sharedProject && (
            <>
              <Stack spacing={1}>
                <Typography variant="body1">{t("public_url")}</Typography>
                <CopyField value={publicUrl} copyText="Copy URL" copiedText="Copied URL" />
              </Stack>

              {/* Embed Code */}
              <Stack spacing={1}>
                <Typography variant="body1">{t("embed_code")}</Typography>
                <CopyField value={embedCode} copyText="Copy Code" copiedText="Copied Code" />
              </Stack>

              {/* Custom URL (white-label custom domains).
                  Only renders when the org has at least one active domain;
                  the section is intentionally hidden otherwise to avoid
                  surfacing an empty dropdown for orgs that don't use the
                  feature. */}
              {showCustomUrl && (
                <Stack spacing={1}>
                  <Typography variant="body1">
                    {t("share_custom_url_label", "Custom URL")}
                  </Typography>
                  <Stack direction="row" spacing={1} alignItems="center">
                    <FormControl fullWidth size="small">
                      <Select
                        value={assignedDomainId ?? ""}
                        displayEmpty
                        disabled={isCustomUrlBusy}
                        onChange={handleCustomUrlChange}
                        renderValue={(selected) => {
                          if (!selected) {
                            return (
                              <Typography variant="body2" color="text.secondary">
                                {t(
                                  "share_custom_url_placeholder",
                                  "Choose a domain…"
                                )}
                              </Typography>
                            );
                          }
                          const picked = selectableDomains.find(
                            (d) => d.id === selected
                          );
                          return picked?.base_domain ?? "";
                        }}>
                        {selectableDomains.map((domain) => (
                          <MenuItem key={domain.id} value={domain.id}>
                            {domain.base_domain}
                          </MenuItem>
                        ))}
                      </Select>
                    </FormControl>
                    {assignedDomainId && (
                      <Tooltip
                        title={t("share_custom_url_none", "Remove custom URL")}>
                        <span>
                          <IconButton
                            size="small"
                            disabled={isCustomUrlBusy}
                            onClick={() =>
                              handleCustomUrlChange({
                                target: { value: "__none__" },
                              } as SelectChangeEvent<string>)
                            }
                            aria-label={t(
                              "share_custom_url_none",
                              "Remove custom URL"
                            )}>
                            <Icon
                              iconName={ICON_NAME.XCLOSE}
                              htmlColor="inherit"
                              style={{ fontSize: 16 }}
                            />
                          </IconButton>
                        </span>
                      </Tooltip>
                    )}
                  </Stack>
                </Stack>
              )}

              {/* Analytics opt-in. Only renders when the org has an
                  analytics configuration; otherwise the whole section is
                  hidden so we don't surface a dead toggle. */}
              {orgAnalytics && (
                <Stack spacing={1}>
                  <Typography variant="body1">
                    {t("share_analytics_label", "Analytics tracking")}
                  </Typography>
                  <Stack direction="row" alignItems="center" spacing={1}>
                    <Switch
                      checked={Boolean(sharedProject?.tracking_enabled)}
                      disabled={isTrackingBusy}
                      onChange={handleTrackingToggle}
                      inputProps={{ "aria-label": "tracking-toggle" }}
                    />
                    <Typography variant="body2" color="text.secondary">
                      {t(
                        "share_analytics_helper",
                        "Use your organization's analytics configuration to track visits to this dashboard."
                      )}
                    </Typography>
                  </Stack>

                  {sharedProject?.tracking_enabled && (
                    <Stack spacing={0.5}>
                      <Stack direction="row" alignItems="center" spacing={1}>
                        <Switch
                          checked={sharedProject.tracking_require_consent !== false}
                          disabled={isConsentBusy}
                          onChange={handleConsentToggle}
                          inputProps={{ "aria-label": "consent-toggle" }}
                        />
                        <Typography variant="body2" color="text.secondary">
                          {t(
                            "share_consent_helper",
                            "Wait for visitor consent before sending analytics events (recommended in GDPR jurisdictions)."
                          )}
                        </Typography>
                      </Stack>
                      {sharedProject.tracking_require_consent === false && (
                        <Stack
                          direction="row"
                          spacing={1}
                          alignItems="flex-start"
                          sx={{ pl: 7 }}>
                          <Icon
                            iconName={ICON_NAME.CIRCLEINFO}
                            htmlColor={theme.palette.warning.main}
                            style={{ fontSize: 14, marginTop: 3, flexShrink: 0 }}
                          />
                          <Typography variant="caption" color="warning.main">
                            {t(
                              "share_consent_warning",
                              "Tracking will fire on every page view without waiting for consent. In Germany and most of the EU this is not compliant with GDPR/TDDDG — only disable if you operate in a jurisdiction that permits cookieless analytics without consent."
                            )}
                          </Typography>
                        </Stack>
                      )}
                    </Stack>
                  )}
                </Stack>
              )}
            </>
          )}
        </Stack>
      )}
    </>
  );
};

const ShareModal: React.FC<ShareProps> = ({ open, onClose, type, content }) => {
  const { t } = useTranslation("common");
  const [isBusy, setIsBusy] = useState(false);
  const [value, setValue] = useState(0);
  const { organization: organization } = useOrganization();
  const { teams: teamsList } = useTeams();

  const sharedWithContent = "shared_with" in content ? content.shared_with : undefined;
  const [sharedWith, setSharedWith] = useState(() => {
    const { teams = [], organizations = [] } = sharedWithContent || {};
    const mapToIdAndRole = (item) => ({ id: item.id, role: item.role });
    return {
      teams: teams.map(mapToIdAndRole),
      organizations: organizations.map(mapToIdAndRole),
    };
  });

  // For folders: fetch grants and sync into sharedWith once on open
  const { data: folderGrants } = useFolderGrants(
    type === "folder" && open ? content.id : null
  );

  // For layers and projects: fetch the folder's grants to show inherited access read-only
  const itemFolderId =
    type === "layer"
      ? (content as Layer).folder_id
      : type === "project"
        ? (content as Project).folder_id
        : null;
  const { data: layerFolderGrants, error: layerFolderGrantsError } = useFolderGrants(
    (type === "layer" || type === "project") && open && itemFolderId ? itemFolderId : null
  );
  const grantsLoaded = useRef(false);
  useEffect(() => {
    if (type === "folder" && folderGrants && !grantsLoaded.current) {
      setSharedWith({
        teams: folderGrants.grants
          .filter((g) => g.grantee_type === "team")
          .map((g) => ({ id: g.grantee_id, role: g.role })),
        organizations: folderGrants.grants
          .filter((g) => g.grantee_type === "organization")
          .map((g) => ({ id: g.grantee_id, role: g.role })),
      });
      grantsLoaded.current = true;
    }
  }, [type, folderGrants]);

  const handleChange = (_event: React.SyntheticEvent, newValue: number) => {
    setValue(newValue);
  };

  const tabItems = useMemo(() => {
    // If accounts features are disabled → ONLY show "Public"
    if (ACCOUNTS_DISABLED) {
      return [{ label: t("public"), value: "public" }];
    }

    // Otherwise → show the normal tabs
    const items = [
      { label: t("organization"), value: "organization" },
      { label: t("teams"), value: "teams" },
    ];

    // Public only for projects
    if (type === "project") {
      items.push({ label: t("public"), value: "public" });
    }

    return items;
  }, [t, type]);

  const handleOnClose = () => {
    setIsBusy(false);
    grantsLoaded.current = false;
    onClose && onClose();
  };

  const organizationsAccessLevel: Item[] = useMemo(() => {
    if (!organization) {
      return [];
    }
    const sharedWitthOrg = sharedWith?.organizations;
    const inheritedOrgRole = layerFolderGrants?.grants.find(
      (g) => g.grantee_type === "organization" && g.grantee_id === organization.id
    )?.role;
    const accessLevels = [
      {
        id: organization.id,
        name: organization.name,
        avatar: organization.avatar as string,
        role: sharedWitthOrg?.find((org) => org.id === organization.id)?.role || "",
        inheritedRole: inheritedOrgRole,
      },
    ];
    return accessLevels;
  }, [organization, sharedWith, layerFolderGrants]);

  const teamsAccessLevel: Item[] = useMemo(() => {
    if (!teamsList) {
      return [];
    }
    const sharedWithTeams = sharedWith?.teams;
    const accessLevels = teamsList.map((team) => ({
      id: team.id,
      name: team.name,
      avatar: team.avatar as string,
      role: sharedWithTeams?.find((t) => t.id === team.id)?.role || "",
      inheritedRole: layerFolderGrants?.grants.find(
        (g) => g.grantee_type === "team" && g.grantee_id === team.id
      )?.role,
    }));
    return accessLevels;
  }, [teamsList, sharedWith, layerFolderGrants]);

  const roleOptions = useMemo(() => {
    if (type === "layer") return [...layerShareRoleEnum.options, ""] as string[];
    if (type === "project") return [...projectShareRoleEnum.options, ""] as string[];
    if (type === "folder") return [...folderShareRoleEnum.options, ""] as string[];
    return [];
  }, [type]);

  const handleSubmit = async () => {
    try {
      setIsBusy(true);
      if (type === "project") {
        await shareProject(content.id, sharedWith);
        mutate((key) => Array.isArray(key) && key[0] === PROJECTS_API_BASE_URL);
      } else if (type === "layer") {
        await shareLayer(content.id, sharedWith);
        mutate((key) => Array.isArray(key) && key[0] === LAYERS_API_BASE_URL);
      } else if (type === "folder") {
        const oldGrants = folderGrants?.grants ?? [];
        const newTeams = (sharedWith.teams ?? []).filter((t) => t.role !== "");
        const newOrgs = (sharedWith.organizations ?? []).filter((o) => o.role !== "");
        const newTeamIds = new Set(newTeams.map((t) => t.id));
        const newOrgIds = new Set(newOrgs.map((o) => o.id));
        for (const team of newTeams) {
          await shareFolderGrant(content.id, {
            grantee_type: "team",
            grantee_id: team.id,
            role: team.role as "folder-viewer" | "folder-editor",
          });
        }
        for (const org of newOrgs) {
          await shareFolderGrant(content.id, {
            grantee_type: "organization",
            grantee_id: org.id,
            role: org.role as "folder-viewer" | "folder-editor",
          });
        }
        for (const grant of oldGrants) {
          if (grant.grantee_type === "team" && !newTeamIds.has(grant.grantee_id)) {
            await deleteFolderGrant(content.id, "team", grant.grantee_id);
          } else if (grant.grantee_type === "organization" && !newOrgIds.has(grant.grantee_id)) {
            await deleteFolderGrant(content.id, "organization", grant.grantee_id);
          }
        }
        mutate(
          (key) =>
            (typeof key === "string" && key.startsWith(FOLDERS_API_BASE_URL)) ||
            (Array.isArray(key) && typeof key[0] === "string" && key[0].startsWith(FOLDERS_API_BASE_URL))
        );
      }
      toast.success(t("share_access_updated_successfully"));
    } catch {
      toast.error(t("error_updating_share_access"));
    } finally {
      handleOnClose();
    }
  };

  const handleRoleChange = (type: "organizations" | "teams", id: string, role: string) => {
    setSharedWith((prevSharedWith) => {
      const items = prevSharedWith?.[type] || [];
      const itemExists = items.some((item) => item.id === id);

      let updatedItems;
      if (role === "") {
        // Remove the item if the role is an empty string
        updatedItems = items.filter((item) => item.id !== id);
      } else if (itemExists) {
        // Update the role if the item exists
        updatedItems = items.map((item) => {
          if (item.id === id) {
            return { ...item, role };
          }
          return item;
        });
      } else {
        // Add the item if it doesn't exist
        updatedItems = [...items, { id, role }];
      }

      return { ...prevSharedWith, [type]: updatedItems };
    });
  };
  const handleOrganizationRoleChange = (id: string, role: string) => {
    if (type === "folder" && role !== "") {
      // Clear all team grants when org is granted
      setSharedWith((prev) => ({ ...prev, teams: [] }));
    }
    handleRoleChange("organizations", id, role);
  };

  const handleTeamRoleChange = (id: string, role: string) => {
    if (type === "folder" && role !== "") {
      // Clear org grant when a team is granted
      setSharedWith((prev) => ({ ...prev, organizations: [] }));
    }
    handleRoleChange("teams", id, role);
  };

  // const isSharingUpdated = useMemo(() => {
  //   const { teams = [], organizations = [] } = content.shared_with || {};

  //   const sharedWithTeams = sharedWith.teams;
  //   const sharedWithOrgs = sharedWith.organizations;

  //   const isTeamsUpdated = sharedWithTeams
  //     ? sharedWithTeams.some((team) => {
  //         const existingTeam = teams.find((t) => t.id === team.id);
  //         return !existingTeam || existingTeam.role !== team.role;
  //       })
  //     : false;

  //   const isOrgsUpdated = sharedWithOrgs
  //     ? sharedWithOrgs.some((org) => {
  //         const existingOrg = organizations.find((o) => o.id === org.id);
  //         return !existingOrg || existingOrg.role !== org.role;
  //       })
  //     : false;

  //   return isTeamsUpdated || isOrgsUpdated;
  // }, [content.shared_with, sharedWith]);

  return (
    <>
      <Dialog open={open} onClose={handleOnClose} fullWidth maxWidth="sm">
        <DialogTitle>
          <Trans
            i18nKey="common:manage_share_access_for_content"
            values={{
              content_type: type === "layer" ? t("layer") : type === "project" ? t("project") : t("folder_label"),
              content_name: content.name,
            }}
          />
        </DialogTitle>
        <DialogContent>
          <Box sx={{ mt: 1, maxHeight: "500px" }}>
            <Box sx={{ width: "100%", mt: 8 }}>
              <Box sx={{ borderBottom: 1, borderColor: "divider" }}>
                <Tabs value={value} scrollButtons onChange={handleChange}>
                  {tabItems.map((item) => (
                    <Tab key={item.value} label={item.label} {...a11yProps(item.value)} />
                  ))}
                </Tabs>
              </Box>
              {/* <Divider sx={{ mt: 2, mb: 0, pb: 0 }} /> */}
              {tabItems.map((item) => (
                <CustomTabPanel
                  disablePadding
                  key={item.value}
                  value={value}
                  index={tabItems.findIndex((tab) => tab.value === item.value)}>
                  {item.value === "organization" && (
                    <>
                      {type === "folder" && sharedWith.teams.length > 0 && (
                        <Typography variant="caption" color="text.secondary" sx={{ px: 2, pt: 1, display: "block" }}>
                          {t("folder_share_conflict_warning")}
                        </Typography>
                      )}
                      {(type === "layer" || type === "project") && layerFolderGrantsError && itemFolderId && (
                        <Typography variant="caption" color="text.secondary" sx={{ px: 2, pt: 1, display: "block" }}>
                          {t("folder_access_may_apply")}
                        </Typography>
                      )}
                      <ShareWithItemsTab
                        items={organizationsAccessLevel}
                        roleOptions={roleOptions}
                        onRoleChange={handleOrganizationRoleChange}
                        disableInherited={type === "layer" || type === "project"}
                      />
                    </>
                  )}
                  {item.value === "teams" && (
                    <>
                      {type === "folder" && sharedWith.organizations.length > 0 && (
                        <Typography variant="caption" color="text.secondary" sx={{ px: 2, pt: 1, display: "block" }}>
                          {t("folder_share_conflict_warning")}
                        </Typography>
                      )}
                      {(type === "layer" || type === "project") && layerFolderGrantsError && itemFolderId && (
                        <Typography variant="caption" color="text.secondary" sx={{ px: 2, pt: 1, display: "block" }}>
                          {t("folder_access_may_apply")}
                        </Typography>
                      )}
                      <ShareWithItemsTab
                        items={teamsAccessLevel}
                        roleOptions={roleOptions}
                        onRoleChange={handleTeamRoleChange}
                        disableInherited={type === "layer" || type === "project"}
                      />
                    </>
                  )}
                  {item.value === "public" && type === "project" && (
                    <ShareWithPublicTab project={content as Project} />
                  )}
                </CustomTabPanel>
              ))}
            </Box>
          </Box>
        </DialogContent>
        {/* Disable for public sharing */}
        {value !== 2 && (
          <DialogActions
            disableSpacing
            sx={{
              pt: 6,
              pb: 2,
              justifyContent: "flex-end",
            }}>
            <Stack direction="row" spacing={2}>
              <Button onClick={handleOnClose} variant="text">
                <Typography variant="body2" fontWeight="bold">
                  {t("cancel")}
                </Typography>
              </Button>
              <LoadingButton
                // disabled={!isSharingUpdated}
                loading={isBusy}
                variant="contained"
                color="primary"
                onClick={handleSubmit}>
                {t("save")}
              </LoadingButton>
            </Stack>
          </DialogActions>
        )}
      </Dialog>
    </>
  );
};

export default ShareModal;
