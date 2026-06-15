"use client";

import {
  Autocomplete,
  Box,
  Chip,
  Stack,
  TextField,
  Typography,
} from "@mui/material";
import { useState } from "react";
import { useTranslation } from "react-i18next";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import { type NutsRegion, useNutsSearch } from "@/lib/api/layers";

const LEVEL_LABELS: Record<number, string> = {
  0: "Country",
  1: "State",
  2: "Region",
  3: "District",
};

interface SpatialFilterSearchProps {
  bbox: string | null;
  bboxLabel: string;
  onBboxChange: (bbox: string | null, label: string) => void;
}

export default function SpatialFilterSearch({
  bbox,
  bboxLabel,
  onBboxChange,
}: SpatialFilterSearchProps) {
  const { t } = useTranslation("common");
  const [inputValue, setInputValue] = useState("");
  const { regions, isLoading } = useNutsSearch(inputValue);

  const handleSelect = (_event: unknown, value: NutsRegion | null) => {
    if (value) {
      const bboxStr = value.bbox.join(",");
      const label = `${value.nuts_name} (NUTS ${value.level})`;
      onBboxChange(bboxStr, label);
      setInputValue("");
    }
  };

  const handleClear = () => {
    onBboxChange(null, "");
    setInputValue("");
  };

  return (
    <Stack spacing={1.5}>
      {/* Active filter chip */}
      {bbox && bboxLabel && (
        <Chip
          label={bboxLabel}
          onDelete={handleClear}
          color="primary"
          size="small"
          icon={<Icon iconName={ICON_NAME.LOCATION} style={{ fontSize: 14 }} />}
        />
      )}

      {/* NUTS search */}
      <Autocomplete
        size="small"
        options={regions}
        loading={isLoading}
        inputValue={inputValue}
        onInputChange={(_e, v) => setInputValue(v)}
        onChange={handleSelect}
        getOptionLabel={(option) => `${option.nuts_id} — ${option.nuts_name}`}
        isOptionEqualToValue={(a, b) => a.nuts_id === b.nuts_id}
        filterOptions={(x) => x}
        noOptionsText={t("no_results")}
        renderOption={(props, option) => (
          <Box component="li" {...props} key={option.nuts_id}>
            <Stack>
              <Typography variant="body2" fontWeight={500}>
                {option.nuts_id} — {option.nuts_name}
              </Typography>
              <Typography variant="caption" color="text.secondary">
                NUTS {option.level} · {LEVEL_LABELS[option.level] || ""}
              </Typography>
            </Stack>
          </Box>
        )}
        renderInput={(params) => (
          <TextField
            {...params}
            placeholder={t("search_region")}
            variant="outlined"
          />
        )}
      />

    </Stack>
  );
}
