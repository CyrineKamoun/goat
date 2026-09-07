import { DialogContentText } from "@mui/material";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { toast } from "react-toastify";

import { ICON_NAME } from "@p4b/ui/components/Icon";

import {
  deleteInvitation as deleteOrganizationInvitation,
  deleteMember as deleteOrganizationMember,
} from "@/lib/api/organizations";
import { deleteMember as deleteTeamMember } from "@/lib/api/teams";
import { invitationStatusEnum } from "@/lib/validations/invitation";

import type { MemberDialogBaseProps } from "@/types/dashboard/settings";

import AppDialog, { AppDialogFooter } from "@/components/common/AppDialog";

interface DeleteMemberDialogProps extends MemberDialogBaseProps {
  disabled?: boolean;
  onDelete?: () => void;
  teamId?: string;
  organizationId?: string;
}

const DeleteMemberModal: React.FC<DeleteMemberDialogProps> = ({
  open,
  disabled,
  onClose,
  onDelete,
  member,
  organizationId,
  teamId,
}) => {
  const { t } = useTranslation("common");
  const [isBusy, setIsBusy] = useState(false);
  const handleDelete = async () => {
    try {
      setIsBusy(true);
      if (organizationId) {
        if (member.invitation_status === invitationStatusEnum.Enum.accepted) {
          await deleteOrganizationMember(organizationId, member.id);
        } else {
          await deleteOrganizationInvitation(organizationId, member.id);
        }
      } else if (teamId) {
        await deleteTeamMember(teamId, member.id);
      }
      toast.success(t("member_deleted_success"));
    } catch {
      toast.error(t("member_delete_error"));
    } finally {
      setIsBusy(false);
    }

    onDelete?.();
  };

  return (
    <AppDialog
      open={open}
      onClose={() => onClose?.()}
      icon={ICON_NAME.TRASH}
      tone="warning"
      title={t("delete_member")}
      footer={
        <AppDialogFooter
          onCancel={onClose}
          primaryLabel={t("remove")}
          onPrimary={() => void handleDelete()}
          primaryColor="error"
          primaryDisabled={disabled}
          primaryLoading={isBusy}
        />
      }>
      <DialogContentText>
        {t("delete_member_description")}
        <br />
        <b>{member?.email}</b>
      </DialogContentText>
    </AppDialog>
  );
};

export default DeleteMemberModal;
