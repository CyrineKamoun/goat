"use client";

import {
  Box,
  Button,
  ButtonGroup,
  Container,
  Grid,
  ListItemIcon,
  ListItemText,
  Menu,
  MenuItem,
  Pagination,
  Paper,
  Stack,
  Typography,
} from "@mui/material";
import { useRouter } from "next/navigation";
import { useCallback, useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { toast } from "react-toastify";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import { executeProcessAsync } from "@/lib/api/processes";
import { copyProject, useProjects } from "@/lib/api/projects";
import { setRunningJobIds } from "@/lib/store/jobs/slice";
import type { Layer } from "@/lib/validations/layer";
import type { Project } from "@/lib/validations/project";
import type { GetProjectsQueryParams } from "@/lib/validations/project";

import { ContentActions } from "@/types/common";

import { useAuthZ } from "@/hooks/auth/AuthZ";
import { useJobStatus } from "@/hooks/jobs/JobStatus";
import { useAppDispatch, useAppSelector } from "@/hooks/store/ContextHooks";

import ContentSearchBar from "@/components/dashboard/common/ContentSearchbar";
import FoldersTreeView from "@/components/dashboard/common/FoldersTreeView";
import TileGrid from "@/components/dashboard/common/TileGrid";
import ProjectModal from "@/components/modals/Project";
import ProjectImportModal from "@/components/modals/ProjectImport";

const Projects = () => {
  const router = useRouter();
  const [queryParams, setQueryParams] = useState<GetProjectsQueryParams>({
    order: "descendent",
    order_by: "updated_at",
    size: 12,
    page: 1,
  });
  const [view, setView] = useState<"list" | "grid">("grid");
  const { t } = useTranslation("common");

  const { projects, isLoading: isProjectLoading, isError: _isProjectError, mutate } = useProjects(queryParams);

  const [openProjectModal, setOpenProjectModal] = useState(false);
  const [openImportModal, setOpenImportModal] = useState(false);
  const [anchorEl, setAnchorEl] = useState<null | HTMLElement>(null);
  const { isOrgEditor } = useAuthZ();
  const dispatch = useAppDispatch();
  const runningJobIds = useAppSelector((state) => state.jobs.runningJobIds);
  useJobStatus(mutate, mutate);

  const handleProjectAction = useCallback(
    async (action: ContentActions, item: Project | Layer) => {
      const project = item as Project;
      if (action === ContentActions.DUPLICATE) {
        try {
          const newProject = await copyProject(project.id);
          toast.success(t("project_duplicated"));
          mutate();
          if (newProject?.id) {
            router.push(`/map/${newProject.id}`);
          }
        } catch (_error) {
          toast.error(t("error_duplicating_project"));
        }
      } else if (action === ContentActions.EXPORT) {
        try {
          const job = await executeProcessAsync("project_export", {
            project_id: project.id,
          });
          if (job?.jobID) {
            dispatch(setRunningJobIds([...runningJobIds, job.jobID]));
          }
          toast.info(t("project_export_submitted"));
        } catch (_error) {
          toast.error(t("error_exporting_project"));
        }
      }
    },
    [mutate, router, t]
  );

  useEffect(() => {
    if (projects?.pages && queryParams?.page && projects?.pages < queryParams?.page) {
      setQueryParams({
        ...queryParams,
        page: projects.pages,
      });
    }
  }, [projects, queryParams]);

  return (
    <Container sx={{ py: 10, px: 10 }} maxWidth="xl">
      <ProjectModal type="create" open={openProjectModal} onClose={() => setOpenProjectModal(false)} />
      <ProjectImportModal
        open={openImportModal}
        onClose={() => setOpenImportModal(false)}
        onImportStarted={() => {
          // Projects list will refresh on next render cycle via SWR
        }}
      />
      <Box
        sx={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
          mb: 8,
        }}>
        <Typography variant="h6">{t("projects")}</Typography>
        {isOrgEditor && (
          <>
            <ButtonGroup variant="contained" disableElevation sx={{ "& .MuiButtonGroup-grouped:not(:last-of-type)": { borderColor: "white" } }}>
              <Button
                startIcon={<Icon iconName={ICON_NAME.PLUS} style={{ fontSize: 12 }} />}
                onClick={() => setOpenProjectModal(true)}>
                {t("new_project")}
              </Button>
              <Button
                size="small"
                onClick={(e) => setAnchorEl(e.currentTarget)}>
                <Icon iconName={ICON_NAME.CHEVRON_DOWN} style={{ fontSize: 10 }} />
              </Button>
            </ButtonGroup>
            <Menu
              anchorEl={anchorEl}
              open={Boolean(anchorEl)}
              onClose={() => setAnchorEl(null)}>
              <MenuItem
                onClick={() => {
                  setAnchorEl(null);
                  setOpenImportModal(true);
                }}>
                <ListItemIcon>
                  <Icon iconName={ICON_NAME.UPLOAD} style={{ fontSize: 14 }} />
                </ListItemIcon>
                <ListItemText>{t("import_project")}</ListItemText>
              </MenuItem>
            </Menu>
          </>
        )}
      </Box>
      <Grid container justifyContent="space-between" spacing={4}>
        <Grid item xs={12}>
          <ContentSearchBar
            contentType="project"
            view={view}
            setView={setView}
            queryParams={queryParams}
            setQueryParams={(queryParams) => {
              setQueryParams({
                ...queryParams,
                page: 1,
              });
            }}
          />
        </Grid>
        <Grid item xs={3}>
          <Paper elevation={3}>
            <FoldersTreeView
              queryParams={queryParams}
              enableActions={isOrgEditor}
              hideMyContent={!isOrgEditor}
              setQueryParams={(params, teamId, organizationId) => {
                const newQueryParams = { ...params, page: 1 };
                delete newQueryParams?.["team_id"];
                delete newQueryParams?.["organization_id"];
                if (teamId) {
                  newQueryParams["team_id"] = teamId;
                } else if (organizationId) {
                  newQueryParams["organization_id"] = organizationId;
                }
                setQueryParams(newQueryParams);
              }}
            />
          </Paper>
        </Grid>
        <Grid item xs={9}>
          <TileGrid
            view={view}
            items={projects?.items ?? []}
            enableActions={isOrgEditor}
            isLoading={isProjectLoading}
            type="project"
            onClick={(item) => {
              if (item && item.id) {
                router.push(`/map/${item.id}`);
              }
            }}
            onAction={handleProjectAction}
          />
          {!isProjectLoading && projects && projects?.items.length > 0 && (
            <Stack direction="row" justifyContent="center" alignItems="center" sx={{ p: 4 }}>
              <Pagination
                count={projects.pages || 1}
                size="large"
                page={queryParams.page || 1}
                onChange={(_e, page) => {
                  setQueryParams({
                    ...queryParams,
                    page,
                  });
                }}
              />
            </Stack>
          )}
        </Grid>
      </Grid>
    </Container>
  );
};

export default Projects;
