"use client";

import { Box, Button, TextField, Typography, useTheme } from "@mui/material";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { toast } from "react-toastify";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import { homeFolderOf, spaceDisplayName } from "@/lib/utils/content";
import { stripMarkdown } from "@/lib/utils/templates";
import type { TemplatePayloadKind, TemplateRead, TemplateUseResult } from "@/lib/validations/template";

import { type UseTemplateContext, useUseTemplate } from "@/hooks/templates/useUseTemplate";

import AppDialog, { AppDialogFooter } from "@/components/common/AppDialog";
import FolderBrowser from "@/components/dashboard/common/FolderBrowser";
import Selector from "@/components/map/panels/common/Selector";
import TextFieldInput from "@/components/map/panels/common/TextFieldInput";
import { DialogGroupLabel } from "@/components/modals/content/ContentDialogChrome";

export interface UseTemplateFlowProps {
  template: TemplateRead;
  /** Panels already sitting inside a project pass `in_project`; Home,
   * Content, and Catalog pass `new_project`. */
  context: UseTemplateContext;
  onClose: () => void;
  /** The caller navigates — this component only reports what got created.
   * Not called on a failed apply (the dialog stays open with the error). */
  onDone: (result: TemplateUseResult) => void;
}

const PAYLOAD_KIND_ICON: Record<TemplatePayloadKind, ICON_NAME> = {
  workflow: ICON_NAME.WORKFLOW,
  layout: ICON_NAME.REPORT,
  project: ICON_NAME.MAP,
};

const GEOMETRY_ICON: Record<string, ICON_NAME> = {
  point: ICON_NAME.POINT_FEATURE,
  line: ICON_NAME.LINE_FEATURE,
  polygon: ICON_NAME.POLYGON_FEATURE,
};

const geometryIconFor = (geometryType: string | null): ICON_NAME =>
  (geometryType && GEOMETRY_ICON[geometryType]) || ICON_NAME.TABLE;

/**
 * T7's "Using a template" flow: (1) for a fresh project, where it should
 * live — name, space, folder; (2) a row per `ask` input to bind to one of
 * the target project's layers, or leave for later; (3) apply; (4) hand the
 * result to the caller. The caller owns navigation — it builds the URL from
 * `templateResultHref` (`useUseTemplate`) — so this component stays a plain
 * dialog with no router dependency of its own. Either step is skipped when
 * it doesn't apply — `in_project` never asks for a location, and a template
 * with no `ask` inputs never asks for bindings.
 */
const UseTemplateFlow = ({ template, context, onClose, onDone }: UseTemplateFlowProps) => {
  const { t } = useTranslation("common");
  const theme = useTheme();
  const flow = useUseTemplate(template, context);
  const [stepIndex, setStepIndex] = useState(0);

  const currentStep = flow.steps[stepIndex] ?? null;
  const isLastStep = stepIndex >= flow.steps.length - 1;
  const canAdvance = currentStep !== "location" || flow.locationValid;

  const handlePrimary = async () => {
    if (!isLastStep) {
      setStepIndex((index) => index + 1);
      return;
    }
    const result = await flow.apply();
    if (!result) return;
    toast.success(t("template_created_in", { name: template.name }));
    if (result.unresolved_inputs.length > 0) {
      toast.info(t("unresolved_input_count", { count: result.unresolved_inputs.length }));
    }
    onDone(result);
  };

  // A step the caller can walk back out of keeps its Back button at the left
  // end of the action row.
  const footer = (
    <AppDialogFooter
      onCancel={onClose}
      cancelDisabled={flow.busy}
      primaryLabel={isLastStep ? t("create") : t("next_step")}
      onPrimary={() => void handlePrimary()}
      primaryDisabled={!canAdvance}
      primaryLoading={flow.busy}
      extra={
        stepIndex > 0 ? (
          <Button variant="text" disabled={flow.busy} onClick={() => setStepIndex((index) => index - 1)}>
            <Typography variant="body2" fontWeight="bold">
              {t("back")}
            </Typography>
          </Button>
        ) : undefined
      }
    />
  );

  return (
    <AppDialog
      open
      onClose={flow.busy ? () => undefined : onClose}
      icon={PAYLOAD_KIND_ICON[template.payload_kind]}
      title={t("use_template")}
      subtitle={template.name}
      maxWidth={480}
      closeLabel={t("cancel")}
      bodySx={{ padding: "22px", maxHeight: "60vh" }}
      footer={footer}>
      <>
        {currentStep === "location" && (
          <>
            <DialogGroupLabel first>{t("location")}</DialogGroupLabel>
            {/* TextFieldInput leaves its unfocused label colour to `inherit`,
                so the box around it is what sets the house secondary. */}
            <Box sx={{ color: "text.secondary" }}>
              <TextFieldInput
                label={t("name")}
                value={flow.name}
                onChange={(value) => flow.setName(value)}
                inputProps={{ "aria-label": t("name") }}
              />
            </Box>
            <Box sx={{ mt: "14px" }}>
              <Selector
                label={t("destination")}
                items={flow.spaces.map((space) => ({
                  value: space.id,
                  label: spaceDisplayName(space, t),
                }))}
                selectedItems={
                  flow.selectedSpace
                    ? { value: flow.selectedSpace.id, label: spaceDisplayName(flow.selectedSpace, t) }
                    : undefined
                }
                setSelectedItems={(items) => {
                  const picked = Array.isArray(items) ? items[0] : items;
                  const space = flow.spaces.find((candidate) => candidate.id === picked?.value) ?? null;
                  flow.setSelectedSpace(space);
                }}
              />
            </Box>
            {flow.selectedSpace && (
              <Box sx={{ mt: "14px" }}>
                <FolderBrowser
                  space={flow.selectedSpace}
                  folders={flow.folders}
                  homeFolderId={homeFolderOf(flow.folders, flow.selectedSpace.id)?.id ?? null}
                  value={flow.selectedFolder?.id ?? null}
                  onChange={(folderId) =>
                    flow.setSelectedFolder(
                      folderId ? (flow.folders.find((folder) => folder.id === folderId) ?? null) : null
                    )
                  }
                  label={t("folder")}
                  maxHeight={180}
                />
              </Box>
            )}
          </>
        )}

        {currentStep === "bindings" && (
          <>
            <DialogGroupLabel first>{t("pick_a_layer")}</DialogGroupLabel>
            {flow.askInputs.map((input) => {
              // A locked project layer (no access of the caller's own to the
              // underlying dataset) can't be bound — it carries none of the
              // real style/metadata fields an actual binding needs.
              const candidates = flow.candidatesFor(input).filter((layer) => !layer.locked);
              return (
                <Box key={input.key} sx={{ mb: "16px" }}>
                  <Box sx={{ display: "flex", alignItems: "center", gap: "8px", mb: "6px" }}>
                    <Icon
                      iconName={geometryIconFor(input.geometry_type)}
                      style={{ fontSize: 15, color: theme.palette.text.secondary }}
                    />
                    <Typography sx={{ fontSize: 13.5, fontWeight: 600 }}>{input.label}</Typography>
                  </Box>
                  {candidates.length > 0 ? (
                    <TextField
                      select
                      SelectProps={{ native: true }}
                      fullWidth
                      size="small"
                      inputProps={{ "aria-label": input.label }}
                      sx={{
                        "& .MuiInputBase-root": { minHeight: "40px", fontSize: "0.875rem" },
                      }}
                      value={flow.bindings[input.key] ?? ""}
                      onChange={(event) => flow.setBinding(input.key, event.target.value || null)}>
                      <option value="">{t("decide_later")}</option>
                      {candidates.map((layer) => (
                        <option key={layer.id} value={layer.layer_id}>
                          {layer.name}
                        </option>
                      ))}
                    </TextField>
                  ) : (
                    <Typography sx={{ fontSize: 12.5, color: theme.palette.text.secondary }}>
                      {t("decide_later")}
                    </Typography>
                  )}
                </Box>
              );
            })}
          </>
        )}

        {currentStep === null && (
          <Typography sx={{ fontSize: 13.5, color: theme.palette.text.secondary }}>
            {template.description ? stripMarkdown(template.description) : template.name}
          </Typography>
        )}

        {flow.error && (
          <Typography role="alert" sx={{ mt: "14px", fontSize: 12.5, color: theme.palette.error.main }}>
            {flow.error}
          </Typography>
        )}
      </>
    </AppDialog>
  );
};

export default UseTemplateFlow;
