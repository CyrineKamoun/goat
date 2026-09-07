import { zodResolver } from "@hookform/resolvers/zod";
import { Box, DialogContentText, Stack, TextField, useTheme } from "@mui/material";
import { useState } from "react";
import { useForm } from "react-hook-form";
import { useTranslation } from "react-i18next";
import { toast } from "react-toastify";

import { ICON_NAME } from "@p4b/ui/components/Icon";

import { createTeam } from "@/lib/api/teams";
import type { TeamBase } from "@/lib/validations/team";
import { teamBaseSchema } from "@/lib/validations/team";

import AppDialog, { AppDialogFooter } from "@/components/common/AppDialog";

/** Ties the footer's primary to the form, so Enter in a field creates the
 * team the way the in-body submit button used to. */
const FORM_ID = "team-create-form";

interface TeamCreateDialogProps {
  onClose: () => void;
  open: boolean;
  onCreate?: () => void;
}

const TeamCreateModal: React.FC<TeamCreateDialogProps> = ({ open, onClose, onCreate }) => {
  const theme = useTheme();
  const { t } = useTranslation(["common"]);

  const [isBusy, setIsBusy] = useState(false);

  const { register, handleSubmit, reset, formState, getValues } = useForm<TeamBase>({
    mode: "onChange",
    resolver: zodResolver(teamBaseSchema),
  });

  const onTeamCreate = async () => {
    try {
      onCreate?.();
      setIsBusy(true);
      const payload = getValues();
      await createTeam(payload);
      toast.success(t("common:team_created_success"));
    } catch {
      toast.error(t("common:error_creating_team"));
    } finally {
      setIsBusy(false);
      reset();
      onClose();
    }
  };

  return (
    <AppDialog
      open={open}
      onClose={onClose}
      icon={ICON_NAME.USERS}
      title={t("common:create_team")}
      maxWidth={600}
      footer={
        <AppDialogFooter
          onCancel={onClose}
          primaryLabel={t("common:create")}
          onPrimary={() => void handleSubmit(onTeamCreate)()}
          primaryType="submit"
          primaryForm={FORM_ID}
          primaryDisabled={!formState.isValid}
          primaryLoading={isBusy}
        />
      }>
      <DialogContentText>{t("common:team_create_description")}</DialogContentText>
      <Box component="form" id={FORM_ID} onSubmit={handleSubmit(onTeamCreate)}>
        <Stack
          spacing={theme.spacing(6)}
          sx={{
            mt: 4,
          }}>
          <TextField fullWidth required label={t("common:team_name")} {...register("name")} id="name" />
          <TextField
            fullWidth
            label={t("common:team_description")}
            {...register("description")}
            id="description"
          />
        </Stack>
      </Box>
    </AppDialog>
  );
};

export default TeamCreateModal;
