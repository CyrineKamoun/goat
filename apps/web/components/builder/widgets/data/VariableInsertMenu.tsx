import ArrowDropDownIcon from "@mui/icons-material/ArrowDropDown";
import DataObjectIcon from "@mui/icons-material/DataObject";
import { ListItemText, Menu, MenuItem, ToggleButton, Typography } from "@mui/material";
import type { Editor } from "@tiptap/react";
import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";

import type { RichTextVariableSchema } from "@/lib/validations/widget";

interface VariableInsertMenuProps {
  editor: Editor;
  variables: RichTextVariableSchema[];
  onOpen?: () => void;
  onClose?: () => void;
  forceClose?: boolean;
}

const VariableInsertMenu: React.FC<VariableInsertMenuProps> = ({
  editor,
  variables,
  onOpen,
  onClose,
  forceClose,
}) => {
  const { t } = useTranslation("common");
  const [anchorEl, setAnchorEl] = useState<null | HTMLElement>(null);
  const open = Boolean(anchorEl);

  useEffect(() => {
    if (forceClose && anchorEl) {
      setAnchorEl(null);
    }
  }, [forceClose, anchorEl]);

  const handleOpen = (event: React.MouseEvent<HTMLElement>) => {
    event.stopPropagation();
    if (anchorEl) {
      setAnchorEl(null);
    } else {
      onOpen?.();
      setAnchorEl(event.currentTarget);
    }
  };

  const handleClose = () => {
    setAnchorEl(null);
    onClose?.();
  };

  const insertVariable = (name: string) => {
    editor.chain().focus().insertVariable(name).run();
    handleClose();
  };

  return (
    <>
      <ToggleButton
        value="variable"
        size="small"
        selected={open}
        onClick={handleOpen}
        sx={{ display: "flex", alignItems: "center" }}>
        <DataObjectIcon fontSize="small" sx={{ color: open ? "primary.main" : "inherit" }} />
        <ArrowDropDownIcon fontSize="small" />
      </ToggleButton>

      <Menu
        anchorEl={anchorEl}
        open={open}
        onClose={handleClose}
        anchorOrigin={{ vertical: "bottom", horizontal: "left" }}
        transformOrigin={{ vertical: "top", horizontal: "left" }}
        sx={{ zIndex: 1500 }}
        slotProps={{
          paper: {
            onMouseDown: (e: React.MouseEvent) => e.stopPropagation(),
            onPointerDown: (e: React.PointerEvent) => e.stopPropagation(),
            sx: { maxHeight: 350, minWidth: 200 },
          },
        }}>
        {variables.length === 0 ? (
          <MenuItem disabled dense>
            <ListItemText>
              <Typography variant="caption" color="text.secondary">
                {t("rich_text_no_variables")}
              </Typography>
            </ListItemText>
          </MenuItem>
        ) : (
          variables.map((variable) => (
            <MenuItem key={variable.id} dense onClick={() => insertVariable(variable.name)}>
              <ListItemText>
                <Typography
                  variant="body2"
                  sx={{ fontFamily: "monospace", color: "primary.main" }}>
                  {variable.name}
                </Typography>
              </ListItemText>
              <Typography variant="caption" color="text.secondary" sx={{ ml: 2 }}>
                {variable.operation_type || "—"}
              </Typography>
            </MenuItem>
          ))
        )}
      </Menu>
    </>
  );
};

export default VariableInsertMenu;
