import { Box, Fab, Stack, Tooltip, Typography, useTheme } from "@mui/material";
import { useEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import type { Project } from "@/lib/validations/project";
import type { PopupPlacement, PopupSize, PopupType } from "@/lib/validations/widget";

import MarkdownContentEditor from "@/components/builder/widgets/common/MarkdownContentEditor";
import PopupContentRenderer from "@/components/builder/widgets/common/PopupContentRenderer";
import PopupSettingsControls from "@/components/builder/widgets/common/PopupSettingsControls";
import AppDialog, { AppDialogFooter } from "@/components/common/AppDialog";

interface ProjectInfoProps {
  project: Project;
  viewOnly?: boolean;
  onProjectUpdate?: (key: string, value: unknown, refresh?: boolean) => void;
}

export function ProjectInfo({ project, viewOnly, onProjectUpdate }: ProjectInfoProps) {
  const { t } = useTranslation("common");
  const builderConfig = project?.builder_config;
  const theme = useTheme();
  const [open, setOpen] = useState(false);
  const fabRef = useRef<HTMLButtonElement | null>(null);

  const settings = builderConfig?.settings;
  const infoContent = settings?.project_info_content || "";
  const popupType = (settings?.project_info_popup_type as PopupType) ?? "dialog";
  const popupPlacement = (settings?.project_info_popup_placement as PopupPlacement) ?? "auto";
  const popupSize = (settings?.project_info_popup_size as PopupSize) ?? "md";
  const hasInfo = infoContent.trim().length > 0;

  const onClose = () => setOpen(false);

  // Open the popup automatically on first visit (viewer-only)
  useEffect(() => {
    if (!project?.id) return;
    const key = `project_info_seen_${project.id}`;
    const seen = localStorage.getItem(key);
    if (!seen && viewOnly && hasInfo) {
      setOpen(true);
      localStorage.setItem(key, "true");
    }
  }, [project?.id, viewOnly, hasInfo]);

  const updateSetting = (key: string, val: unknown) => {
    if (!onProjectUpdate || !builderConfig) return;
    onProjectUpdate("builder_config", {
      ...builderConfig,
      settings: { ...builderConfig.settings, [key]: val },
    });
  };

  const fab = (
    <Tooltip title={t("info")} arrow placement="left">
      <Fab
        ref={fabRef}
        onClick={() => setOpen(true)}
        size="small"
        sx={{
          backgroundColor: theme.palette.background.paper,
          marginBottom: theme.spacing(1),
          pointerEvents: "all",
          color: theme.palette.action.active,
          "&:hover": { backgroundColor: theme.palette.background.default },
        }}>
        <Icon iconName={ICON_NAME.INFO} htmlColor="inherit" fontSize="small" />
      </Fab>
    </Tooltip>
  );

  // Public/viewer mode: render via PopupContentRenderer using configured type.
  if (viewOnly) {
    if (!hasInfo) return null;
    return (
      <>
        {fab}
        {open && (
          <PopupContentRenderer
            open
            onClose={onClose}
            popup_type={popupType}
            placement={popupPlacement}
            size={popupSize}
            anchorEl={fabRef.current}
            title={project.name}
            content={infoContent}
          />
        )}
      </>
    );
  }

  // Builder/edit mode: same dialog shell as InfoChipEditDialog and
  // LinkPopupEditDialog.
  return (
    <>
      <AppDialog
        open={open}
        onClose={onClose}
        icon={ICON_NAME.INFO}
        title={t("edit_popup_content")}
        maxWidth={600}
        bodySx={{ pt: 2 }}
        footer={<AppDialogFooter primaryLabel={t("done", { defaultValue: "Done" })} onPrimary={onClose} />}>
        <Stack spacing={2}>
          <PopupSettingsControls
            popupType={popupType}
            placement={popupPlacement}
            size={popupSize}
            onPopupTypeChange={(v) => updateSetting("project_info_popup_type", v)}
            onPlacementChange={(v) => updateSetting("project_info_popup_placement", v)}
            onSizeChange={(v) => updateSetting("project_info_popup_size", v)}
          />
          <Box>
            <Typography
              variant="caption"
              color="text.secondary"
              fontWeight={600}
              sx={{ mb: 0.5, display: "block" }}>
              {t("info_text", { defaultValue: "Info text" })}
            </Typography>
            <MarkdownContentEditor
              value={infoContent}
              onChange={(v) => updateSetting("project_info_content", v)}
              plainText={popupType === "tooltip"}
            />
          </Box>
        </Stack>
      </AppDialog>

      {fab}
    </>
  );
}
