"use client";

import { FormControl, MenuItem, Select, Typography } from "@mui/material";
import { useRouter } from "next/navigation";

import type { CatalogLayerSummary } from "@/lib/validations/layer";

interface LayerSwitcherDropdownProps {
  currentLayerId: string;
  layers: CatalogLayerSummary[];
  groupId: string;
}

export default function LayerSwitcherDropdown({
  currentLayerId,
  layers,
  groupId,
}: LayerSwitcherDropdownProps) {
  const router = useRouter();

  if (layers.length < 2) return null;

  return (
    <FormControl size="small" sx={{ minWidth: 200 }}>
      <Select
        value={currentLayerId}
        onChange={(e) => {
          const layerId = e.target.value as string;
          if (layerId !== currentLayerId) {
            router.push(`/datasets/${layerId}?group=${groupId}`);
          }
        }}
        renderValue={(selected) => {
          const layer = layers.find((l) => l.id === selected);
          return (
            <Typography variant="body2" noWrap>
              {layer?.name || selected}
            </Typography>
          );
        }}>
        {layers.map((layer) => (
          <MenuItem key={layer.id} value={layer.id}>
            <Typography variant="body2" sx={{ mr: 1 }}>
              {layer.name}
            </Typography>
            <Typography variant="caption" color="text.secondary">
              {layer.type}
              {layer.feature_layer_geometry_type ? ` · ${layer.feature_layer_geometry_type}` : ""}
            </Typography>
          </MenuItem>
        ))}
      </Select>
    </FormControl>
  );
}
