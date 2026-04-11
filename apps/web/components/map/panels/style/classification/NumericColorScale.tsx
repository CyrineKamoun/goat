import { Box, InputBase, MenuItem, Select, Stack, TextField, Typography, useTheme } from "@mui/material";
import React, { useEffect, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";

import { formatNumber } from "@/lib/utils/helpers";
import type { ClassBreaks, ColorMap, ColorRange } from "@/lib/validations/layer";
import { classBreaks } from "@/lib/validations/layer";

import type { ColorLegends, ColorScaleSelectorProps } from "@/types/map/color";

import DropdownFooter from "@/components/map/panels/style/other/DropdownFooter";

type NumericColorScaleProps = ColorScaleSelectorProps & {
  setIsClickAwayEnabled: (isClickAwayEnabled: boolean) => void;
  onCancel?: () => void;
};

const NumericColorScale = (props: NumericColorScaleProps) => {
  const theme = useTheme();
  const { classBreaksValues } = props;
  const { t } = useTranslation("common");

  const existingLegends = (props.colorSet.selectedColor as ColorRange)?.color_legends as
    | ColorLegends
    | undefined;

  const colorMapValues = useMemo(() => {
    if (!classBreaksValues || !Array.isArray(classBreaksValues.breaks)) {
      return [];
    }
    const intervalValues: ColorMap = [];
    const staticColor = "#000000";
    classBreaksValues.breaks.forEach((value, index) => {
      const colors = (props.colorSet.selectedColor as ColorRange).colors;
      const color = colors[index] !== undefined ? colors[index] : staticColor;
      const roundedValue = formatNumber(value, 2).toString();
      intervalValues.push([[roundedValue], color]);
      if (index === classBreaksValues.breaks.length - 1) {
        intervalValues.push([null, colors[index + 1] !== undefined ? colors[index + 1] : staticColor]);
      }
    });

    return intervalValues;
  }, [classBreaksValues, props.colorSet.selectedColor]);

  const [labels, setLabels] = useState<Record<string, string>>(() => ({ ...existingLegends }));

  // Sync local labels when external legends change (e.g., classification method switch clears them)
  useEffect(() => {
    setLabels({ ...existingLegends });
  }, [existingLegends]);

  const hasChanges = useMemo(() => {
    const existing = existingLegends || {};
    const currentKeys = Object.keys(labels).filter((k) => labels[k]);
    const existingKeys = Object.keys(existing).filter((k) => existing[k]);
    if (currentKeys.length !== existingKeys.length) return true;
    return currentKeys.some((k) => labels[k] !== existing[k]);
  }, [labels, existingLegends]);

  function onApply() {
    const cleanedLegends: ColorLegends = {};
    Object.entries(labels).forEach(([key, label]) => {
      if (label) cleanedLegends[key] = label;
    });
    props.onColorLegendsChange?.(Object.keys(cleanedLegends).length > 0 ? cleanedLegends : undefined);
    props.onCancel?.();
  }

  function onCancel() {
    props.onCancel?.();
  }

  return (
    <Box sx={{ pt: 3, pb: 3, pl: 3, pr: 0, flex: 1, minHeight: 0, display: "flex", flexDirection: "column", overflow: "hidden" }}>
      <Box sx={{ pr: 3 }}>
      <Select
        fullWidth
        size="small"
        IconComponent={() => null}
        value={props.selectedColorScaleMethod}
        onOpen={() => {
          props.setIsClickAwayEnabled && props.setIsClickAwayEnabled(false);
        }}
        MenuProps={{
          TransitionProps: {
            onExited: () => {
              props.setIsClickAwayEnabled && props.setIsClickAwayEnabled(true);
            },
          },
        }}
        onChange={(e) => {
          props.setSelectedColorScaleMethod(e.target.value as ClassBreaks);
        }}>
        {classBreaks.options.map((option, index) => (
          <MenuItem key={index} value={String(option)}>
            {t(`${option}`)}
          </MenuItem>
        ))}
      </Select>
      <Stack direction="row" justifyContent="space-between" alignItems="center" py={2}>
        <Typography variant="caption">
          Min: <b>{classBreaksValues?.min ? formatNumber(classBreaksValues?.min) : ""}</b>
        </Typography>
        <Typography variant="caption">
          Max: <b>{classBreaksValues?.max ? formatNumber(classBreaksValues?.max) : ""}</b>
        </Typography>
      </Stack>
      </Box>
      <Box sx={{ flex: 1, minHeight: 0, overflowY: "auto", pt: 2 }}>
        <Stack spacing={1} sx={{ pr: 3 }}>
          {colorMapValues &&
            colorMapValues.map((colorMapValue, index) => (
              <Box key={`row_${index}`}>
                <Stack direction="row" alignItems="center" spacing={1}>
                  <Box
                    sx={{
                      borderRadius: "4px",
                      width: "32px",
                      minWidth: "32px",
                      height: "20px",
                      backgroundColor: colorMapValue[1],
                    }}
                  />
                  <TextField
                    size="small"
                    value={index === 0 ? `<${classBreaksValues?.min}` : colorMapValues[index - 1][0]}
                    disabled
                    InputProps={{ sx: { height: "28px" } }}
                    sx={{
                      flex: 1,
                      "& .MuiOutlinedInput-input": {
                        padding: `0 ${theme.spacing(1)}`,
                        fontSize: 12,
                      },
                    }}
                  />
                  <Typography variant="body2">-</Typography>
                  <TextField
                    size="small"
                    value={
                      colorMapValues[index][0] === null
                        ? `>${classBreaksValues?.max}`
                        : colorMapValues[index][0]
                    }
                    disabled
                    InputProps={{ sx: { height: "28px" } }}
                    sx={{
                      flex: 1,
                      "& .MuiOutlinedInput-input": {
                        padding: `0 ${theme.spacing(1)}`,
                        fontSize: 12,
                      },
                    }}
                  />
                </Stack>
                <InputBase
                  value={labels[String(index)] || labels[colorMapValue[1]] || ""}
                  placeholder={t("legend_label")}
                  onChange={(e) => {
                    setLabels((prev) => ({ ...prev, [String(index)]: e.target.value }));
                  }}
                  sx={{
                    fontSize: 11,
                    color: "text.secondary",
                    pl: "40px",
                    "& .MuiInputBase-input": {
                      p: 0,
                      "&::placeholder": {
                        opacity: 0.5,
                        fontStyle: "italic",
                      },
                    },
                  }}
                />
              </Box>
            ))}
        </Stack>
      </Box>
      {hasChanges ? (
        <Box sx={{ mt: 2, pr: 3 }}>
          <DropdownFooter isValid={true} onCancel={onCancel} onApply={onApply} />
        </Box>
      ) : (
        <Stack sx={{ pt: 4, pr: 3 }}>
          <Typography variant="caption">{t("common:change_colors_and_steps")}</Typography>
        </Stack>
      )}
    </Box>
  );
};

export default NumericColorScale;
