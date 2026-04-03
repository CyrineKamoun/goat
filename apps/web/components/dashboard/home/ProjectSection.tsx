import { Box, Grid, Skeleton } from "@mui/material";
import { useRouter } from "next/navigation";
import { useCallback, useState } from "react";
import { useTranslation } from "react-i18next";
import { toast } from "react-toastify";

import { executeProcessAsync } from "@/lib/api/processes";
import { copyProject } from "@/lib/api/projects";
import { setRunningJobIds } from "@/lib/store/jobs/slice";
import type { Layer } from "@/lib/validations/layer";
import type { Project } from "@/lib/validations/project";

import { ContentActions } from "@/types/common";

import { useContentMoreMenu } from "@/hooks/dashboard/ContentHooks";
import { useAppDispatch, useAppSelector } from "@/hooks/store/ContextHooks";

import EmptyCard from "@/components/dashboard/common/EmptyCard";
import TileCard from "@/components/dashboard/common/TileCard";
import ContentDialogWrapper from "@/components/modals/ContentDialogWrapper";
import ProjectModal from "@/components/modals/Project";

interface ProjectSectionProps {
  projects: Project[];
  isLoading: boolean;
  hideCreate?: boolean;
}

const NON_DIALOG_ACTIONS = new Set([ContentActions.EXPORT, ContentActions.DUPLICATE]);

const ProjectSection = (props: ProjectSectionProps) => {
  const router = useRouter();
  const { projects, isLoading } = props;
  const { getMoreMenuOptions, activeContent, moreMenuState, closeMoreMenu, openMoreMenu } =
    useContentMoreMenu();

  const { t } = useTranslation("common");
  const [openProjectModal, setOpenProjectModal] = useState(false);
  const dispatch = useAppDispatch();
  const runningJobIds = useAppSelector((state) => state.jobs.runningJobIds);

  const handleMoreMenuSelect = (menuItem: Parameters<typeof openMoreMenu>[0], contentItem: Project | Layer) => {
    if (NON_DIALOG_ACTIONS.has(menuItem.id as ContentActions)) {
      handleProjectAction(menuItem.id as ContentActions, contentItem as Project);
      return;
    }
    openMoreMenu(menuItem, contentItem);
  };

  const handleProjectAction = useCallback(
    async (action: ContentActions, project: Project) => {
      if (action === ContentActions.DUPLICATE) {
        try {
          const newProject = await copyProject(project.id);
          toast.success(t("project_duplicated"));
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
    [dispatch, runningJobIds, router, t]
  );
  return (
    <Box>
      <ProjectModal type="create" open={openProjectModal} onClose={() => setOpenProjectModal(false)} />
      {activeContent && moreMenuState && (
        <>
          <ContentDialogWrapper
            content={activeContent}
            action={moreMenuState.id as ContentActions}
            onClose={closeMoreMenu}
            onContentDelete={closeMoreMenu}
            type="project"
          />
        </>
      )}
      <Grid container spacing={5}>
        {(isLoading ? Array.from(new Array(3)) : (projects ?? [])).map((item: Project, index: number) => (
          <Grid
            item
            key={item?.id ?? index}
            xs={12}
            sm={6}
            md={4}
            lg={3}
            onClick={() => {
              if (item && item.id) {
                router.push(`/map/${item.id}`);
              }
            }}
            display={{
              sm: index > 2 ? "none" : "block",
              md: index > 1 ? "none" : "block",
              lg: index > 2 ? "none" : "block",
            }}>
            {!item ? (
              <Skeleton variant="rectangular" height={200} />
            ) : (
              <TileCard
                cardType="grid"
                item={item}
                moreMenuOptions={getMoreMenuOptions("project", item)}
                onMoreMenuSelect={handleMoreMenuSelect}
              />
            )}
          </Grid>
        ))}
        <Grid item xs={12} sm={6} md={4} lg={3}>
          {isLoading ? (
            <Skeleton variant="rectangular" height={200} />
          ) : (
            !props.hideCreate && (
              <EmptyCard
                onClick={() => {
                  setOpenProjectModal(true);
                }}
                tooltip={t("create_new_project")}
                backgroundImage="https://assets.plan4better.de/img/goat_new_project_artwork.png"
              />
            )
          )}
        </Grid>
      </Grid>
    </Box>
  );
};

export default ProjectSection;
