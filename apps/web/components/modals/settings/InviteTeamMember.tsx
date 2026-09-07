import type { SelectChangeEvent } from "@mui/material";
import {
  Avatar,
  Box,
  DialogContentText,
  FormControl,
  InputLabel,
  ListItemAvatar,
  ListItemText,
  MenuItem,
  Select,
  Stack,
  useTheme,
} from "@mui/material";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { toast } from "react-toastify";

import { ICON_NAME } from "@p4b/ui/components/Icon";

import { createTeamMember } from "@/lib/api/teams";

import AppDialog, { AppDialogFooter } from "@/components/common/AppDialog";

interface TeamMemberInviteDialogProps {
  onClose: () => void;
  open: boolean;
  onInvite?: () => void;
  teamId: string;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  members: any[];
}

const TeamMemberInviteModal: React.FC<TeamMemberInviteDialogProps> = ({ open, onClose, members, teamId }) => {
  const theme = useTheme();
  const { t } = useTranslation(["common"]);

  const [isBusy, setIsBusy] = useState(false);
  const [selectedMember, setSelectedMember] = useState("");
  const onSelectedMemberChange = (event: SelectChangeEvent) => {
    setSelectedMember(event.target.value as string);
  };

  const onTeamMemberInvite = async () => {
    try {
      setIsBusy(true);
      await createTeamMember(teamId, selectedMember);
      toast.success(t("common:member_added"));
    } catch {
      toast.error(t("common:error_adding_member"));
    } finally {
      setIsBusy(false);
      setSelectedMember("");
      onClose();
    }
  };

  return (
    <AppDialog
      open={open}
      onClose={onClose}
      icon={ICON_NAME.ADD_USER}
      title={t("common:add_member")}
      maxWidth={600}
      footer={
        <AppDialogFooter
          onCancel={onClose}
          primaryLabel={t("common:add")}
          onPrimary={() => void onTeamMemberInvite()}
          primaryDisabled={isBusy || !selectedMember}
          primaryLoading={isBusy}
        />
      }>
      <DialogContentText>{t("common:select_an_organization_member")}</DialogContentText>
      <Box>
        <Stack
          spacing={theme.spacing(6)}
          sx={{
            mt: 4,
          }}
        />
        <FormControl fullWidth>
          <InputLabel id="demo-simple-select-disabled-label">{t("member")}</InputLabel>
          <Select
            value={selectedMember}
            label={t("common:member")}
            size="medium"
            fullWidth
            sx={{
              "& .MuiSelect-select": {
                ...(selectedMember ? { py: 1 } : {}),
              },
            }}
            onChange={onSelectedMemberChange}>
            {members.map((member) => (
              <MenuItem key={member.id} value={member.id}>
                <Stack direction="row" alignItems="center">
                  <ListItemAvatar>
                    <Avatar alt={`${member.firstname} ${member.lastname}`} src={member.avatar} />
                  </ListItemAvatar>
                  <ListItemText primary={`${member.firstname} ${member.lastname}`} secondary={member.email} />
                </Stack>
              </MenuItem>
            ))}
          </Select>
        </FormControl>
      </Box>
    </AppDialog>
  );
};

export default TeamMemberInviteModal;
