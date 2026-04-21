import { Box, Stack, Switch, Typography } from "@mui/material";
import { useRef, useState } from "react";
import { useTranslation } from "react-i18next";

import type { ColorItem } from "@/types/map/color";

import { SingleColorPopper } from "@/components/map/panels/style/other/SingleColorPopper";

const DEFAULT_NO_DATA_COLOR = "#CCCCCC";

const NoDataRow = ({
  noDataColor,
  onNoDataColorChange,
  setIsClickAwayEnabled,
}: {
  noDataColor?: string;
  onNoDataColorChange?: (color: string | undefined) => void;
  setIsClickAwayEnabled?: (enabled: boolean) => void;
}) => {
  const { t } = useTranslation("common");
  const isEnabled = !!noDataColor && noDataColor !== "transparent";
  const [editingItem, setEditingItem] = useState<ColorItem | null>(null);
  const anchorRef = useRef<HTMLDivElement>(null);

  return (
    <Box>
      <Stack direction="row" alignItems="center" spacing={1}>
        <Box
          ref={anchorRef}
          onClick={() => {
            if (!isEnabled) return;
            if (editingItem) {
              setEditingItem(null);
              setIsClickAwayEnabled?.(true);
            } else {
              setEditingItem({ id: "no_data", color: noDataColor || DEFAULT_NO_DATA_COLOR });
              setIsClickAwayEnabled?.(false);
            }
          }}
          sx={{
            borderRadius: "4px",
            width: "32px",
            minWidth: "32px",
            height: "20px",
            backgroundColor: isEnabled ? noDataColor : "transparent",
            border: isEnabled ? "none" : "1px dashed",
            borderColor: "divider",
            cursor: isEnabled ? "pointer" : "default",
          }}
        />
        <Typography variant="caption" sx={{ flex: 1 }} color="text.secondary">
          {t("no_data")}
        </Typography>
        <Switch
          size="small"
          checked={isEnabled}
          onChange={(e) => {
            if (e.target.checked) {
              onNoDataColorChange?.(DEFAULT_NO_DATA_COLOR);
            } else {
              onNoDataColorChange?.("transparent");
              setEditingItem(null);
              setIsClickAwayEnabled?.(true);
            }
          }}
        />
      </Stack>
      <SingleColorPopper
        editingItem={editingItem}
        anchorEl={anchorRef.current}
        onInputHexChange={(item) => {
          setEditingItem(item);
          onNoDataColorChange?.(item.color);
        }}
      />
    </Box>
  );
};

export default NoDataRow;
