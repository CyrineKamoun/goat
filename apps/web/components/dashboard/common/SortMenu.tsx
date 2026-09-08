"use client";

import CheckIcon from "@mui/icons-material/Check";
import { Menu, MenuItem, Stack, Typography, useTheme } from "@mui/material";
import { useRef, useState } from "react";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import ToolPill from "@/components/dashboard/common/ToolPill";

export type SortOption = { value: string; label: string; icon?: ICON_NAME };

interface SortMenuProps {
  value: string;
  options: SortOption[];
  onChange: (value: string) => void;
  /** Phone layout: the pill keeps only its glyph, dropping the current
   * option's label and the chevron. */
  compact?: boolean;
  /** The pill's accessible name — "Sort" in both pages' toolbars. */
  label: string;
}

/** The sort control both pages carry: a tool pill naming the option in force,
 * opening a menu with a check on it. */
const SortMenu = ({ value, options, onChange, compact, label }: SortMenuProps) => {
  const theme = useTheme();
  const anchor = useRef<HTMLButtonElement | null>(null);
  const [open, setOpen] = useState(false);
  const current = options.find((option) => option.value === value) ?? options[0];

  return (
    <>
      <ToolPill
        ref={anchor}
        icon={ICON_NAME.SORT}
        label={compact ? label : (current?.label ?? label)}
        chevron={!compact}
        active={open}
        iconOnly={compact}
        onClick={() => setOpen(true)}
      />
      <Menu
        anchorEl={anchor.current}
        open={open}
        onClose={() => setOpen(false)}
        anchorOrigin={{ vertical: "bottom", horizontal: "right" }}
        transformOrigin={{ vertical: "top", horizontal: "right" }}
        slotProps={{ paper: { sx: { minWidth: 180, mt: 1.5 } } }}>
        {options.map((option) => {
          const selected = option.value === value;
          return (
            <MenuItem
              key={option.value}
              selected={selected}
              onClick={() => {
                onChange(option.value);
                setOpen(false);
              }}>
              <Stack direction="row" alignItems="center" spacing={2.5} sx={{ width: "100%" }}>
                {option.icon && (
                  <Icon
                    iconName={option.icon}
                    style={{ fontSize: 13 }}
                    htmlColor={selected ? theme.palette.primary.main : theme.palette.text.secondary}
                  />
                )}
                <Typography
                  variant="body2"
                  sx={{ flex: 1, fontWeight: selected ? 600 : 500 }}
                  color={selected ? "primary" : "text.primary"}>
                  {option.label}
                </Typography>
                {selected && <CheckIcon sx={{ fontSize: 14 }} color="primary" />}
              </Stack>
            </MenuItem>
          );
        })}
      </Menu>
    </>
  );
};

export default SortMenu;
