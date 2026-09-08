import { ICON_NAME } from "@p4b/ui/components/Icon";

import AppDialog, { AppDialogFooter } from "@/components/common/AppDialog";

interface ViewModalProps {
  title: string;
  children?: React.ReactNode;
  open: boolean;
  closeText?: string;
  onClose?: () => void;
}

const ViewModal: React.FC<ViewModalProps> = ({ open, title, children, closeText, onClose }) => {
  return (
    <AppDialog
      open={open}
      onClose={() => onClose?.()}
      icon={ICON_NAME.EYE}
      title={title}
      maxWidth={900}
      footer={
        // A read-only dialog: the one action is the way out.
        <AppDialogFooter cancelLabel={closeText || "Close"} onCancel={() => onClose?.()} />
      }>
      {children}
    </AppDialog>
  );
};

export default ViewModal;
