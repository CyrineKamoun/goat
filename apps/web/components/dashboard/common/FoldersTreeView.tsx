import {
  Collapse,
  List,
  ListItemButton,
  ListItemIcon,
  ListItemText,
  Tooltip,
  IconButton,
  Typography,
  useTheme,
} from "@mui/material";
import { useEffect, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import { useFolders } from "@/lib/api/folders";
import { useTeams } from "@/lib/api/teams";
import { useOrganization } from "@/lib/api/users";
import type { GetDatasetSchema } from "@/lib/validations/layer";
import type { GetProjectsQueryParams } from "@/lib/validations/project";

import type { SelectedFolderForEdit } from "@/components/modals/Folder";
import FolderModal from "@/components/modals/Folder";

type EditModal = {
  type: "create" | "update" | "delete";
  selectedFolder?: SelectedFolderForEdit;
  open: boolean;
};

export type SelectedFolder = {
  type: "folder" | "team" | "organization";
  id: string;
  name: string;
};

interface FoldersTreeViewProps {
  setQueryParams: (
    params: GetDatasetSchema | GetProjectsQueryParams,
    teamId: string | undefined,
    organizationId: string | undefined
  ) => void;
  queryParams: GetDatasetSchema | GetProjectsQueryParams;
  enableActions?: boolean;
  hideMyContent?: boolean;
}

export default function FoldersTreeView(props: FoldersTreeViewProps) {
  const { setQueryParams, queryParams, hideMyContent, enableActions = true } = props;
  const [teamsOpen, setTeamsOpen] = useState(true);
  const [orgsOpen, setOrgsOpen] = useState(true);
  const { organization } = useOrganization();
  const { teams: teamsData } = useTeams();
  const { t } = useTranslation("common");
  const [editModal, setEditModal] = useState<EditModal>();

  // Still fetch folders so FolderModal can validate unique names
  const { folders } = useFolders({});

  const theme = useTheme();

  const teams = useMemo(
    () => teamsData?.map((team) => ({ id: team.id, avatar: team.avatar, name: team.name })) ?? [],
    [teamsData]
  );

  const organizations = useMemo(
    () =>
      organization
        ? [{ id: organization.id, avatar: organization.avatar, name: organization.name }]
        : [],
    [organization]
  );

  // Determine active context from queryParams
  const activeTeamId = (queryParams as GetProjectsQueryParams).team_id;
  const activeOrgId = (queryParams as GetProjectsQueryParams).organization_id;
  const isMyContent = !activeTeamId && !activeOrgId;

  // Auto-select organization context when hideMyContent is true
  useEffect(() => {
    if (hideMyContent && organization && !activeTeamId && !activeOrgId) {
      const { folder_id: _, ...rest } = queryParams;
      setQueryParams(rest, undefined, organization.id);
    }
  }, [hideMyContent, organization]); // eslint-disable-line react-hooks/exhaustive-deps

  const handleSelectMyContent = () => {
    const { folder_id: _, team_id: __, organization_id: ___, ...rest } = queryParams as Record<string, unknown>;
    setQueryParams(rest as GetProjectsQueryParams, undefined, undefined);
  };

  const handleSelectTeam = (teamId: string) => {
    const { folder_id: _, ...rest } = queryParams;
    setQueryParams(rest, teamId, undefined);
  };

  const handleSelectOrg = (orgId: string) => {
    const { folder_id: _, ...rest } = queryParams;
    setQueryParams(rest, undefined, orgId);
  };

  return (
    <>
      {enableActions && (
        <FolderModal
          type={editModal?.type || "create"}
          open={editModal?.open || false}
          onClose={() => setEditModal(undefined)}
          onEdit={() => setEditModal(undefined)}
          existingFolderNames={folders?.map((f) => f.name)}
          selectedFolder={editModal?.selectedFolder}
        />
      )}

      <List sx={{ width: "100%", maxWidth: 360 }} component="nav" aria-labelledby="content-tree-view">
        {/* My Content */}
        {!hideMyContent && (
          <ListItemButton
            disableRipple
            selected={isMyContent}
            onClick={handleSelectMyContent}
            sx={{ ...(isMyContent && { color: "primary.main" }) }}>
            <ListItemIcon sx={{ minWidth: "40px" }}>
              <Icon
                iconName={ICON_NAME.HOUSE}
                fontSize="small"
                htmlColor={isMyContent ? theme.palette.primary.main : "inherit"}
              />
            </ListItemIcon>
            <ListItemText
              primary={
                <Typography
                  variant="body1"
                  sx={{ ...(isMyContent && { color: "primary.main", fontWeight: 700 }) }}>
                  {t("my_content")}
                </Typography>
              }
            />
            {enableActions && (
              <Tooltip title={t("new_folder")} placement="top">
                <IconButton
                  size="small"
                  onClick={(e) => {
                    e.stopPropagation();
                    setEditModal({ type: "create", open: true });
                  }}>
                  <Icon iconName={ICON_NAME.FOLDER_NEW} fontSize="small" htmlColor="inherit" />
                </IconButton>
              </Tooltip>
            )}
          </ListItemButton>
        )}

        {/* Teams */}
        {teams.length > 0 && (
          <>
            <ListItemButton disableRipple onClick={() => setTeamsOpen((o) => !o)}>
              {teamsOpen ? (
                <Icon iconName={ICON_NAME.CHEVRON_DOWN} style={{ fontSize: "15px" }} />
              ) : (
                <Icon iconName={ICON_NAME.CHEVRON_RIGHT} style={{ fontSize: "15px" }} />
              )}
              <ListItemIcon sx={{ ml: 3, minWidth: "40px" }}>
                <Icon iconName={ICON_NAME.USERS} fontSize="small" />
              </ListItemIcon>
              <ListItemText primary={<Typography variant="body1">{t("teams")}</Typography>} />
            </ListItemButton>
            <Collapse in={teamsOpen} timeout="auto" unmountOnExit>
              <List component="div" disablePadding>
                {teams.map((team) => (
                  <ListItemButton
                    disableRipple
                    key={team.id}
                    selected={activeTeamId === team.id}
                    onClick={() => handleSelectTeam(team.id)}
                    sx={{
                      pl: 10,
                      ...(activeTeamId === team.id && { color: "primary.main" }),
                    }}>
                    <ListItemIcon sx={{ ml: 4, minWidth: "40px" }}>
                      <Icon
                        iconName={ICON_NAME.USERS}
                        fontSize="small"
                        htmlColor={activeTeamId === team.id ? theme.palette.primary.main : "inherit"}
                      />
                    </ListItemIcon>
                    <ListItemText
                      primary={team.name}
                      sx={{
                        "& .MuiTypography-root": {
                          ...(activeTeamId === team.id && { color: "primary.main", fontWeight: 700 }),
                        },
                      }}
                    />
                  </ListItemButton>
                ))}
              </List>
            </Collapse>
          </>
        )}

        {/* Organizations */}
        {organizations.length > 0 && (
          <>
            <ListItemButton disableRipple onClick={() => setOrgsOpen((o) => !o)}>
              {orgsOpen ? (
                <Icon iconName={ICON_NAME.CHEVRON_DOWN} style={{ fontSize: "15px" }} />
              ) : (
                <Icon iconName={ICON_NAME.CHEVRON_RIGHT} style={{ fontSize: "15px" }} />
              )}
              <ListItemIcon sx={{ ml: 3, minWidth: "40px" }}>
                <Icon iconName={ICON_NAME.ORGANIZATION} fontSize="small" />
              </ListItemIcon>
              <ListItemText primary={<Typography variant="body1">{t("organizations")}</Typography>} />
            </ListItemButton>
            <Collapse in={orgsOpen} timeout="auto" unmountOnExit>
              <List component="div" disablePadding>
                {organizations.map((org) => (
                  <ListItemButton
                    disableRipple
                    key={org.id}
                    selected={activeOrgId === org.id}
                    onClick={() => handleSelectOrg(org.id)}
                    sx={{
                      pl: 10,
                      ...(activeOrgId === org.id && { color: "primary.main" }),
                    }}>
                    <ListItemIcon sx={{ ml: 4, minWidth: "40px" }}>
                      <Icon
                        iconName={ICON_NAME.ORGANIZATION}
                        fontSize="small"
                        htmlColor={activeOrgId === org.id ? theme.palette.primary.main : "inherit"}
                      />
                    </ListItemIcon>
                    <ListItemText
                      primary={org.name}
                      sx={{
                        "& .MuiTypography-root": {
                          ...(activeOrgId === org.id && { color: "primary.main", fontWeight: 700 }),
                        },
                      }}
                    />
                  </ListItemButton>
                ))}
              </List>
            </Collapse>
          </>
        )}
      </List>
    </>
  );
}
